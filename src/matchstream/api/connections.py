"""Small match-scoped WebSocket connection manager."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass

from fastapi import WebSocket

from matchstream.observability.metrics import MatchStreamMetrics, metrics


@dataclass(eq=False, slots=True)
class ManagedConnection:
    """One socket's initialization and last-delivered sequence state."""

    websocket: WebSocket
    ready: bool = False
    pending_notification: bool = False
    last_sequence: int | None = None


class MatchConnectionManager:
    """Route durable state updates only to sockets subscribed to the same match."""

    def __init__(self, observer: MatchStreamMetrics | None = None, send_timeout_seconds: float = 1.0) -> None:
        self._metrics = observer or metrics()
        self._send_timeout_seconds = send_timeout_seconds
        self._connections: dict[str, set[ManagedConnection]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def connect(self, match_id: str, websocket: WebSocket) -> ManagedConnection:
        """Accept and register a socket before its initial durable state is read."""

        await websocket.accept()
        connection = ManagedConnection(websocket)
        async with self._lock:
            self._connections[match_id].add(connection)
        self._metrics.websocket_connections.inc()
        return connection

    async def disconnect(self, match_id: str, connection: ManagedConnection) -> None:
        """Remove a socket exactly once after disconnect or failed delivery."""

        async with self._lock:
            connections = self._connections.get(match_id)
            if connections is None or connection not in connections:
                return
            connections.remove(connection)
            if not connections:
                self._connections.pop(match_id, None)
        self._metrics.websocket_connections.dec()

    async def mark_ready(self, match_id: str, connection: ManagedConnection, sequence: int | None) -> bool:
        """Finish snapshot initialization and report a signal received during it."""

        async with self._lock:
            if connection not in self._connections.get(match_id, set()):
                return False
            connection.ready = True
            connection.last_sequence = sequence
            pending = connection.pending_notification
            connection.pending_notification = False
            return pending

    async def broadcast(self, match_id: str, sequence: int | None, message: dict[str, object]) -> None:
        """Send one durable state message concurrently, bounded per connection."""

        async with self._lock:
            targets: list[ManagedConnection] = []
            for connection in self._connections.get(match_id, set()):
                if not connection.ready:
                    connection.pending_notification = True
                elif _is_newer(sequence, connection.last_sequence):
                    connection.last_sequence = sequence
                    targets.append(connection)
        await asyncio.gather(*(self._send(match_id, connection, message) for connection in targets))

    async def _send(
        self, match_id: str, connection: ManagedConnection, message: dict[str, object]
    ) -> None:
        try:
            await asyncio.wait_for(connection.websocket.send_json(message), self._send_timeout_seconds)
        except Exception:  # noqa: BLE001 - a failed peer must not stop other match clients
            self._metrics.websocket_errors.labels(operation="send").inc()
            await self.disconnect(match_id, connection)
        else:
            self._metrics.websocket_messages.labels(message_type=str(message["type"])).inc()


def _is_newer(sequence: int | None, previous: int | None) -> bool:
    if sequence is None:
        return previous is not None
    return previous is None or sequence > previous
