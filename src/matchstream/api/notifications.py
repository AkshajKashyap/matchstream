"""PostgreSQL LISTEN/NOTIFY adapter used only to wake durable-state readers."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable

import psycopg
from psycopg import sql

from matchstream.observability.logging import get_logger
from matchstream.storage import DatabaseConfig
from matchstream.storage.postgres import STATE_UPDATE_CHANNEL

logger = get_logger("api.notifications")
NotificationHandler = Callable[[str], Awaitable[None]]


class PostgresStateNotifier:
    """Forward post-commit PostgreSQL notification payloads into an async API loop."""

    def __init__(self, config: DatabaseConfig, channel: str = STATE_UPDATE_CHANNEL) -> None:
        self._config = config
        self._channel = channel
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._handler: NotificationHandler | None = None

    def start(self, handler: NotificationHandler) -> None:
        """Start the dedicated LISTEN connection after FastAPI's event loop is live."""

        if self._thread is not None:
            return
        self._handler = handler
        self._loop = asyncio.get_running_loop()
        self._ready.clear()
        self._thread = threading.Thread(target=self._run, name="matchstream-pg-listener", daemon=True)
        self._thread.start()
        self._ready.wait(timeout=2)

    def close(self) -> None:
        """Stop accepting notifications; an in-flight notification remains harmless."""

        self._stop.set()
        self._ready.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                with psycopg.connect(self._config.dsn, autocommit=True) as connection:
                    connection.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self._channel)))
                    self._ready.set()
                    logger.info("postgres_listener_started", extra={"component": "api", "operation": "listen"})
                    while not self._stop.is_set():
                        for notification in connection.notifies(timeout=0.5, stop_after=1):
                            self._forward(notification.payload)
            except psycopg.Error:
                logger.error("postgres_listener_failure", extra={"component": "api", "operation": "listen"})
                self._stop.wait(0.5)

    def _forward(self, match_id: str) -> None:
        if self._loop is None or self._handler is None or not match_id:
            return
        future = asyncio.run_coroutine_threadsafe(self._handler(match_id), self._loop)
        future.add_done_callback(_log_notification_failure)


def _log_notification_failure(future: object) -> None:
    try:
        future.result()  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - background task failures need a safe log record
        logger.error("state_notification_failure", extra={"component": "api", "operation": "broadcast"})
