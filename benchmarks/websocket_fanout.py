"""Measure durable PostgreSQL commit to FastAPI WebSocket delivery for local clients."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path
from uuid import uuid4

import websockets

from benchmarks.common import environment, latency_summary, write_json
from matchstream.models import CanonicalEvent, EntityReference
from matchstream.state import DurableMatchProjector
from matchstream.storage import DatabaseConfig, PostgresProjectionRepository


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--websocket-base-url", default="ws://127.0.0.1:8000")
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 4, 8])
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if any(value < 1 for value in arguments.clients):
        parser.error("each client count must be positive")
    results = asyncio.run(_run(arguments))
    write_json(arguments.output, results)
    print(f"wrote {arguments.output}")


async def _run(arguments: argparse.Namespace) -> dict[str, object]:
    rows = [await _exercise(arguments.websocket_base_url.rstrip("/"), count) for count in arguments.clients]
    return {
        "kind": "websocket_fanout",
        "label": "local development benchmark",
        "method": "direct durable commit timestamp to receipt of its WebSocket state_update",
        "scope": "measures PostgreSQL notify -> API listener -> WebSocket delivery, not broker-to-database latency",
        "environment": environment(),
        "rows": rows,
    }


async def _exercise(websocket_base_url: str, client_count: int) -> dict[str, object]:
    suffix = uuid4().hex
    match_id = f"benchmark-websocket-{suffix}"
    repository = PostgresProjectionRepository(DatabaseConfig.from_environment())
    repository.initialize_schema()
    projector = DurableMatchProjector(repository)
    projector.process(_event(match_id, 1))
    uri = f"{websocket_base_url}/api/v1/matches/{match_id}/stream"
    sockets = await asyncio.gather(*(websockets.connect(uri) for _ in range(client_count)))
    try:
        snapshots = await asyncio.gather(*(socket.recv() for socket in sockets))
        if not all(json.loads(message).get("type") == "snapshot" for message in snapshots):
            raise RuntimeError("expected an initial snapshot for every WebSocket client")
        receives = [asyncio.create_task(_receive_with_timestamp(socket)) for socket in sockets]
        await asyncio.sleep(0.1)
        committed_at = time.perf_counter()
        projector.process(_event(match_id, 2))
        delivered = await asyncio.gather(*receives, return_exceptions=True)
        latencies_ms: list[float] = []
        errors = 0
        for received in delivered:
            if isinstance(received, Exception):
                errors += 1
                continue
            message, received_at = received
            payload = json.loads(message)
            if payload.get("type") != "state_update" or payload["state"].get("latest_sequence") != 2:
                errors += 1
                continue
            latencies_ms.append((received_at - committed_at) * 1000)
        return {
            "clients_requested": client_count,
            "successful_connections": client_count,
            "state_updates_delivered": len(latencies_ms),
            "errors": errors,
            "durable_commit_to_receipt_latency": latency_summary(latencies_ms),
        }
    finally:
        await asyncio.gather(*(socket.close() for socket in sockets), return_exceptions=True)


def _event(match_id: str, sequence: int) -> CanonicalEvent:
    team = EntityReference(id="benchmark-home", name="Benchmark Home")
    return CanonicalEvent(
        event_id=f"benchmark:{match_id}:{sequence}",
        match_id=match_id,
        sequence=sequence,
        period=1,
        match_clock_seconds=float(sequence),
        event_type="Starting XI" if sequence == 1 else "Pass",
        team=team,
        source="benchmark-synthetic",
        source_event_id=str(sequence),
    )


async def _receive_with_timestamp(socket: websockets.ClientConnection) -> tuple[str, float]:
    return await socket.recv(), time.perf_counter()


if __name__ == "__main__":
    main()
