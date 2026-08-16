from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any
from uuid import uuid4

import httpx
import pytest

from matchstream.api.app import create_app
from matchstream.models import CanonicalEvent, EntityReference
from matchstream.state import DurableMatchProjector
from matchstream.storage import DatabaseConfig, PostgresProjectionRepository
from matchstream.streaming.processing import project_next
from matchstream.streaming.redpanda import (
    BrokerConfig,
    RedpandaEventConsumer,
    RedpandaEventProducer,
)

pytestmark = pytest.mark.integration


class _WebSocketSession:
    def __init__(self, app: object, match_id: str) -> None:
        self._app = app
        self._match_id = match_id
        self._incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        path = f"/api/v1/matches/{self._match_id}/stream"
        self._task = asyncio.create_task(
            self._app(  # type: ignore[operator]
                {
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
                },
                self._receive,
                self._send,
            )
        )
        await self._incoming.put({"type": "websocket.connect"})
        assert (await self._outgoing.get())["type"] == "websocket.accept"

    async def receive_json(self) -> dict[str, Any]:
        message = await asyncio.wait_for(self._outgoing.get(), timeout=8)
        assert message["type"] == "websocket.send"
        return json.loads(message["text"])

    async def close(self) -> None:
        await self._incoming.put({"type": "websocket.disconnect", "code": 1000})
        assert self._task is not None
        await asyncio.wait_for(self._task, timeout=2)

    async def _receive(self) -> dict[str, Any]:
        return await self._incoming.get()

    async def _send(self, message: dict[str, Any]) -> None:
        await self._outgoing.put(message)


def _event(match_id: str, sequence: int) -> CanonicalEvent:
    team = EntityReference(id="home", name="Home FC")
    return CanonicalEvent(
        event_id=f"api:{match_id}:event-{sequence}",
        match_id=match_id,
        sequence=sequence,
        period=1,
        match_clock_seconds=float(sequence),
        event_type="Starting XI" if sequence == 1 else "Pass",
        team=team,
        source="integration",
        source_event_id=f"event-{sequence}",
    )


def _project_one(config: BrokerConfig, repository: PostgresProjectionRepository) -> None:
    deadline = time.monotonic() + 12
    with RedpandaEventConsumer(config) as consumer:
        while time.monotonic() < deadline:
            result = project_next(consumer, DurableMatchProjector(repository), 1.0)
            if result is not None:
                return
    pytest.fail("did not receive event for durable projection")


def test_redpanda_to_postgres_to_api_websocket_update() -> None:
    suffix = uuid4().hex
    match_id = f"api-match-{suffix}"
    config = BrokerConfig(
        bootstrap_servers=os.environ.get("MATCHSTREAM_BROKER_ADDRESS", "localhost:19092"),
        topic=f"matchstream.api.{suffix}",
        group_id=f"matchstream-api-{suffix}",
    )
    repository = PostgresProjectionRepository(DatabaseConfig.from_environment())
    repository.initialize_schema()
    with RedpandaEventProducer(config) as producer:
        producer.publish(_event(match_id, 1))
    _project_one(config, repository)

    async def scenario() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        app = create_app(repository)
        async with app.router.lifespan_context(app):  # type: ignore[attr-defined]
            socket = _WebSocketSession(app, match_id)
            await socket.connect()
            snapshot = await socket.receive_json()
            with RedpandaEventProducer(config) as producer:
                producer.publish(_event(match_id, 2))
            _project_one(config, repository)
            update = await socket.receive_json()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                current = (await client.get(f"/api/v1/matches/{match_id}")).json()
            await socket.close()
        return snapshot, update, current

    snapshot, update, current = asyncio.run(scenario())

    assert snapshot["type"] == "snapshot"
    assert snapshot["state"]["latest_sequence"] == 1
    assert update["type"] == "state_update"
    assert update["state"]["latest_sequence"] == 2
    assert current["latest_sequence"] == update["state"]["latest_sequence"]
