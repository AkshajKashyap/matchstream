from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import httpx
from prometheus_client import CollectorRegistry, generate_latest

from matchstream.api.app import create_app
from matchstream.api.connections import MatchConnectionManager
from matchstream.models import CanonicalEvent, EntityReference
from matchstream.observability.metrics import MatchStreamMetrics
from matchstream.state import MatchState
from matchstream.storage import DurableStorageError, StoredMatch


def _state(match_id: str, sequence: int = 1) -> MatchState:
    team = EntityReference(id=f"{match_id}-home", name="Home FC")
    return MatchState(
        match_id=match_id,
        current_period=1,
        current_match_clock_seconds=float(sequence),
        latest_sequence=sequence,
        total_processed_events=sequence,
        home_team=team,
        score_by_team={team.id: 0},
        possession_team=team,
        event_counts={"Pass": sequence},
        last_event_id=f"test:{match_id}:event-{sequence}",
    )


def _stored(match_id: str, sequence: int = 1) -> StoredMatch:
    return StoredMatch(_state(match_id, sequence), datetime(2026, 1, sequence, tzinfo=UTC))


def _event(match_id: str, sequence: int) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=f"test:{match_id}:event-{sequence}",
        match_id=match_id,
        sequence=sequence,
        period=1,
        match_clock_seconds=float(sequence),
        event_type="Pass",
        team=EntityReference(id=f"{match_id}-home", name="Home FC"),
        source="test",
        source_event_id=f"event-{sequence}",
    )


class _Repository:
    def __init__(self) -> None:
        self.matches = {"match-1": _stored("match-1"), "match-2": _stored("match-2")}
        self.events = {
            "match-1": [_event("match-1", 1), _event("match-1", 2)],
            "match-2": [_event("match-2", 1)],
        }
        self.fail = False

    def get_match(self, match_id: str) -> StoredMatch | None:
        self._fail_if_requested()
        return self.matches.get(match_id)

    def list_matches(self, limit: int, after_match_id: str | None = None) -> list[StoredMatch]:
        self._fail_if_requested()
        return [
            self.matches[match_id]
            for match_id in sorted(self.matches)
            if after_match_id is None or match_id > after_match_id
        ][:limit]

    def list_match_events(
        self, match_id: str, after_sequence: int | None, limit: int
    ) -> list[CanonicalEvent]:
        self._fail_if_requested()
        return [
            event
            for event in self.events.get(match_id, [])
            if after_sequence is None or event.sequence > after_sequence
        ][:limit]

    def _fail_if_requested(self) -> None:
        if self.fail:
            raise DurableStorageError("database unavailable")


class _Notifier:
    def __init__(self) -> None:
        self._handler: Callable[[str], Awaitable[None]] | None = None
        self.closed = False

    def start(self, handler: Callable[[str], Awaitable[None]]) -> None:
        self._handler = handler

    def close(self) -> None:
        self.closed = True

    async def emit(self, match_id: str) -> None:
        assert self._handler is not None
        await self._handler(match_id)


class _WebSocketSession:
    """Tiny ASGI WebSocket harness; avoids a network server in unit tests."""

    def __init__(self, app: object, match_id: str) -> None:
        self._app = app
        self._match_id = match_id
        self._incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        path = f"/api/v1/matches/{self._match_id}/stream"
        scope = {
            "type": "websocket",
            "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.1",
            "scheme": "ws",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
            "subprotocols": [],
        }
        self._task = asyncio.create_task(self._app(scope, self._receive, self._send))  # type: ignore[operator]
        await self._incoming.put({"type": "websocket.connect"})
        assert (await self._outgoing.get())["type"] == "websocket.accept"

    async def receive_json(self) -> dict[str, Any]:
        message = await asyncio.wait_for(self._outgoing.get(), timeout=1)
        assert message["type"] == "websocket.send"
        return json.loads(message["text"])

    async def close(self) -> None:
        await self._incoming.put({"type": "websocket.disconnect", "code": 1000})
        assert self._task is not None
        await asyncio.wait_for(self._task, timeout=1)

    async def receive_close(self) -> dict[str, Any]:
        message = await asyncio.wait_for(self._outgoing.get(), timeout=1)
        assert message["type"] == "websocket.close"
        assert self._task is not None
        await asyncio.wait_for(self._task, timeout=1)
        return message

    async def _receive(self) -> dict[str, Any]:
        return await self._incoming.get()

    async def _send(self, message: dict[str, Any]) -> None:
        await self._outgoing.put(message)


def _application() -> tuple[object, _Repository, _Notifier, CollectorRegistry]:
    repository = _Repository()
    notifier = _Notifier()
    registry = CollectorRegistry()
    return create_app(repository, notifier, MatchStreamMetrics(registry), registry), repository, notifier, registry


def test_http_state_listing_history_pagination_and_normalized_metrics() -> None:
    async def scenario() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], int, int, str]:
        app, _repository, _notifier, registry = _application()
        async with app.router.lifespan_context(app):  # type: ignore[attr-defined]
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                listing = await client.get("/api/v1/matches?limit=1")
                state = await client.get("/api/v1/matches/match-1")
                history = await client.get("/api/v1/matches/match-1/events?after_sequence=1&limit=1")
                missing = await client.get("/api/v1/matches/missing")
                invalid_page = await client.get("/api/v1/matches/match-1/events?limit=101")
        return (
            listing.json(),
            state.json(),
            history.json(),
            missing.status_code,
            invalid_page.status_code,
            generate_latest(registry).decode(),
        )

    listing, state, history, missing_status, invalid_page_status, exposition = asyncio.run(scenario())

    assert listing["matches"][0]["match_id"] == "match-1"
    assert listing["next_after_match_id"] == "match-1"
    assert state["latest_sequence"] == 1
    assert history["events"][0]["sequence"] == 2
    assert missing_status == 404
    assert invalid_page_status == 422
    assert 'route="/api/v1/matches/{match_id}"' in exposition
    assert "match-1" not in exposition


def test_storage_failure_becomes_safe_service_error() -> None:
    async def scenario() -> tuple[int, dict[str, str]]:
        app, repository, _notifier, _registry = _application()
        repository.fail = True
        async with app.router.lifespan_context(app), httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:  # type: ignore[attr-defined]
            response = await client.get("/api/v1/matches/match-1")
        return response.status_code, response.json()

    status, body = asyncio.run(scenario())

    assert status == 503
    assert body == {"detail": "durable state unavailable"}


def test_websocket_snapshot_update_and_reconnect_uses_fresh_durable_state() -> None:
    async def scenario() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], str]:
        app, repository, notifier, registry = _application()
        async with app.router.lifespan_context(app):  # type: ignore[attr-defined]
            socket = _WebSocketSession(app, "match-1")
            await socket.connect()
            snapshot = await socket.receive_json()
            repository.matches["match-1"] = _stored("match-1", 2)
            repository.events["match-1"].append(_event("match-1", 2))
            await notifier.emit("match-1")
            update = await socket.receive_json()
            await socket.close()
            reconnected = _WebSocketSession(app, "match-1")
            await reconnected.connect()
            fresh_snapshot = await reconnected.receive_json()
            await reconnected.close()
        return snapshot, update, fresh_snapshot, generate_latest(registry).decode()

    snapshot, update, fresh_snapshot, exposition = asyncio.run(scenario())

    assert snapshot["type"] == "snapshot"
    assert snapshot["state"]["latest_sequence"] == 1
    assert update["type"] == "state_update"
    assert update["state"]["latest_sequence"] == 2
    assert fresh_snapshot["type"] == "snapshot"
    assert fresh_snapshot["state"]["latest_sequence"] == 2
    assert "matchstream_websocket_messages_total{message_type=\"snapshot\"} 2.0" in exposition


def test_unknown_websocket_subscription_closes_with_not_found_code() -> None:
    async def scenario() -> dict[str, Any]:
        app, _repository, _notifier, _registry = _application()
        async with app.router.lifespan_context(app):  # type: ignore[attr-defined]
            socket = _WebSocketSession(app, "missing")
            await socket.connect()
            return await socket.receive_close()

    close = asyncio.run(scenario())

    assert close["code"] == 4404


def test_match_connection_manager_routes_updates_and_removes_failed_clients() -> None:
    class Socket:
        def __init__(self, fail: bool = False) -> None:
            self.fail = fail
            self.messages: list[dict[str, object]] = []

        async def accept(self) -> None:
            return None

        async def send_json(self, message: dict[str, object]) -> None:
            if self.fail:
                raise RuntimeError("closed")
            self.messages.append(message)

    async def scenario() -> tuple[Socket, Socket, Socket, Socket]:
        manager = MatchConnectionManager(send_timeout_seconds=0.1)
        first, second, other, failed = Socket(), Socket(), Socket(), Socket(fail=True)
        connections = [
            await manager.connect("match-1", first),
            await manager.connect("match-1", second),
            await manager.connect("match-2", other),
            await manager.connect("match-1", failed),
        ]
        for connection in connections:
            await manager.mark_ready("match-2" if connection is connections[2] else "match-1", connection, 1)
        await manager.broadcast("match-1", 2, {"type": "state_update"})
        await manager.broadcast("match-1", 3, {"type": "state_update"})
        return first, second, other, failed

    first, second, other, failed = asyncio.run(scenario())

    assert len(first.messages) == len(second.messages) == 2
    assert other.messages == []
    assert failed.messages == []


def test_connection_initialization_records_a_pending_notification() -> None:
    class Socket:
        async def accept(self) -> None:
            return None

        async def send_json(self, message: dict[str, object]) -> None:
            raise AssertionError(f"must not send before snapshot initialization: {message}")

    async def scenario() -> bool:
        manager = MatchConnectionManager()
        connection = await manager.connect("match-1", Socket())
        await manager.broadcast("match-1", 2, {"type": "state_update"})
        return await manager.mark_ready("match-1", connection, 1)

    assert asyncio.run(scenario()) is True
