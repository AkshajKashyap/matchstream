"""FastAPI application serving durable MatchStream state and live updates."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Protocol

from fastapi import FastAPI, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from prometheus_client import REGISTRY, CollectorRegistry, generate_latest

from matchstream.observability.health import PostgresHealthCheck
from matchstream.observability.logging import get_logger
from matchstream.observability.metrics import MatchStreamMetrics, metrics
from matchstream.storage import (
    DatabaseConfig,
    DurableStorageError,
    PostgresProjectionRepository,
    ProjectionRepository,
)

from .connections import MatchConnectionManager
from .notifications import PostgresStateNotifier
from .schemas import (
    ApiErrorResponse,
    CanonicalEventResponse,
    EventHistoryResponse,
    MatchListResponse,
    MatchStateResponse,
    MatchSummaryResponse,
    WebSocketSnapshot,
    WebSocketStateUpdate,
)

logger = get_logger("api")


class StateNotifier(Protocol):
    """Lifecycle boundary for external durable-state change signals."""

    def start(self, handler: object) -> None:
        """Start forwarding match IDs to the asynchronous handler."""

    def close(self) -> None:
        """Stop forwarding notifications."""


class UpdateDispatcher:
    """Read authoritative state once per notification before match-scoped broadcast."""

    def __init__(self, repository: ProjectionRepository, connections: MatchConnectionManager) -> None:
        self._repository = repository
        self._connections = connections
        self._lock = asyncio.Lock()

    async def notify(self, match_id: str) -> None:
        """Turn a non-durable signal into an authoritative durable state update."""

        async with self._lock:
            try:
                stored = self._repository.get_match(match_id)
            except DurableStorageError:
                logger.error(
                    "api_notification_state_read_failure",
                    extra={"component": "api", "operation": "notification_read", "match_id": match_id},
                )
                return
            if stored is None:
                return
            state = MatchStateResponse.from_stored_match(stored)
            message = WebSocketStateUpdate(state=state).model_dump(mode="json")
            await self._connections.broadcast(match_id, state.latest_sequence, message)


def create_app(
    repository: ProjectionRepository | None = None,
    notifier: StateNotifier | None = None,
    observer: MatchStreamMetrics | None = None,
    registry: CollectorRegistry = REGISTRY,
) -> FastAPI:
    """Create an independently runnable API application backed only by PostgreSQL."""

    telemetry = observer or metrics()
    database = DatabaseConfig.from_environment()
    durable_repository = repository or PostgresProjectionRepository(database, observer=telemetry)
    state_notifier = notifier or PostgresStateNotifier(database)
    connections = MatchConnectionManager(telemetry)
    dispatcher = UpdateDispatcher(durable_repository, connections)
    health = PostgresHealthCheck(database)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        application.state.dispatcher = dispatcher
        application.state.connections = connections
        state_notifier.start(dispatcher.notify)
        logger.info("api_started", extra={"component": "api", "operation": "startup"})
        try:
            yield
        finally:
            state_notifier.close()
            logger.info("api_stopped", extra={"component": "api", "operation": "shutdown"})

    app = FastAPI(
        title="MatchStream API",
        version="1.0.0",
        description="Read-only durable match projections and best-effort live state updates.",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def observe_http(request: Request, call_next: object) -> Response:
        started = time.monotonic()
        response = await call_next(request)  # type: ignore[operator]
        route = getattr(request.scope.get("route"), "path", "/unmatched")
        telemetry.http_requests.labels(
            method=request.method, route=route, status_class=f"{response.status_code // 100}xx"
        ).inc()
        telemetry.http_request_duration.labels(method=request.method, route=route).observe(
            time.monotonic() - started
        )
        return response

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/health/ready")
    async def ready() -> Response:
        status = health.check()
        code = 200 if status.healthy else 503
        return JSONResponse(
            status_code=code,
            content={"status": "ready" if status.healthy else "not_ready", "dependencies": [asdict(status)]},
        )

    @app.get("/metrics")
    async def prometheus_metrics() -> Response:
        return Response(generate_latest(registry), media_type="text/plain; version=0.0.4; charset=utf-8")

    @app.get(
        "/api/v1/matches",
        response_model=MatchListResponse,
        responses={503: {"model": ApiErrorResponse}},
    )
    async def list_matches(
        limit: int = Query(default=50, ge=1, le=100),
        after_match_id: str | None = Query(default=None, min_length=1),
    ) -> MatchListResponse:
        try:
            stored = durable_repository.list_matches(limit, after_match_id)
        except (DurableStorageError, ValueError) as error:
            _storage_error("list_matches", error)
        summaries = [MatchSummaryResponse.from_stored_match(item) for item in stored]
        return MatchListResponse(
            matches=summaries,
            next_after_match_id=summaries[-1].match_id if len(summaries) == limit else None,
        )

    @app.get(
        "/api/v1/matches/{match_id}",
        response_model=MatchStateResponse,
        responses={404: {"model": ApiErrorResponse}, 503: {"model": ApiErrorResponse}},
    )
    async def get_match(match_id: str) -> MatchStateResponse:
        stored = await _get_match_or_404(durable_repository, match_id)
        return MatchStateResponse.from_stored_match(stored)

    @app.get(
        "/api/v1/matches/{match_id}/events",
        response_model=EventHistoryResponse,
        responses={404: {"model": ApiErrorResponse}, 503: {"model": ApiErrorResponse}},
    )
    async def get_match_events(
        match_id: str,
        after_sequence: int | None = Query(default=None, ge=0),
        limit: int = Query(default=50, ge=1, le=100),
    ) -> EventHistoryResponse:
        await _get_match_or_404(durable_repository, match_id)
        try:
            events = durable_repository.list_match_events(match_id, after_sequence, limit)
        except (DurableStorageError, ValueError) as error:
            _storage_error("list_match_events", error)
        response_events = [CanonicalEventResponse.from_domain(event) for event in events]
        return EventHistoryResponse(
            events=response_events,
            next_after_sequence=response_events[-1].sequence if len(response_events) == limit else None,
        )

    @app.websocket("/api/v1/matches/{match_id}/stream")
    async def stream_match(match_id: str, websocket: WebSocket) -> None:
        connection = await connections.connect(match_id, websocket)
        logger.info("websocket_connected", extra={"component": "api", "operation": "websocket", "match_id": match_id})
        try:
            stored = durable_repository.get_match(match_id)
            if stored is None:
                await websocket.close(code=4404, reason="match not found")
                return
            state = MatchStateResponse.from_stored_match(stored)
            await websocket.send_json(WebSocketSnapshot(state=state).model_dump(mode="json"))
            telemetry.websocket_messages.labels(message_type="snapshot").inc()
            if await connections.mark_ready(match_id, connection, state.latest_sequence):
                await dispatcher.notify(match_id)
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        except DurableStorageError:
            telemetry.websocket_errors.labels(operation="state_read").inc()
            logger.error(
                "websocket_state_read_failure",
                extra={"component": "api", "operation": "websocket", "match_id": match_id},
            )
            await websocket.close(code=1011, reason="durable state unavailable")
        finally:
            await connections.disconnect(match_id, connection)
            logger.info("websocket_disconnected", extra={"component": "api", "operation": "websocket", "match_id": match_id})

    return app


async def _get_match_or_404(repository: ProjectionRepository, match_id: str):
    try:
        stored = repository.get_match(match_id)
    except DurableStorageError as error:
        _storage_error("get_match", error)
    if stored is None:
        raise HTTPException(status_code=404, detail="match not found")
    return stored


def _storage_error(operation: str, error: Exception) -> None:
    logger.error("api_storage_read_failure", extra={"component": "api", "operation": operation})
    raise HTTPException(status_code=503, detail="durable state unavailable") from error
