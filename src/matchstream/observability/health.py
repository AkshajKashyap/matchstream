"""Cheap dependency checks used by readiness endpoints and the operator CLI."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import psycopg

from matchstream.storage import DatabaseConfig
from matchstream.streaming.redpanda import BrokerConfig, BrokerTransportError, _kafka_module

from .logging import get_logger

logger = get_logger("health")


@dataclass(frozen=True, slots=True)
class HealthStatus:
    """A safe-to-display dependency health result."""

    name: str
    healthy: bool
    detail: str


class PostgresHealthCheck:
    """Verify PostgreSQL connectivity with a constant-time query."""

    def __init__(self, config: DatabaseConfig, connect: Callable[..., object] = psycopg.connect) -> None:
        self._config = config
        self._connect = connect

    def check(self) -> HealthStatus:
        try:
            with self._connect(self._config.dsn, connect_timeout=2) as connection, connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        except Exception as error:  # noqa: BLE001 - readiness must turn client failures into status
            logger.error("postgres_health_failed", extra={"component": "postgres", "operation": "health"})
            return HealthStatus("postgres", False, f"unavailable: {type(error).__name__}")
        return HealthStatus("postgres", True, "connected")


class RedpandaHealthCheck:
    """Verify Kafka-client metadata retrieval, not merely TCP reachability."""

    def __init__(self, config: BrokerConfig, client_factory: Callable[[BrokerConfig], object] | None = None) -> None:
        self._config = config
        self._client_factory = client_factory or _metadata_client

    def check(self) -> HealthStatus:
        try:
            metadata = self._client_factory(self._config).list_topics(timeout=2.0)  # type: ignore[attr-defined]
            topics = getattr(metadata, "topics", {})
            if self._config.topic not in topics:
                return HealthStatus("redpanda", False, "required topic unavailable")
        except Exception as error:  # noqa: BLE001 - Kafka client errors become safe readiness status
            logger.error("redpanda_health_failed", extra={"component": "redpanda", "operation": "health"})
            return HealthStatus("redpanda", False, f"unavailable: {type(error).__name__}")
        return HealthStatus("redpanda", True, "metadata reachable")


def _metadata_client(config: BrokerConfig) -> object:
    kafka = _kafka_module()
    try:
        return kafka.Producer({"bootstrap.servers": config.bootstrap_servers})
    except Exception as error:
        raise BrokerTransportError(f"could not create broker health client: {error}") from error


class ReadinessService:
    """Aggregate independently testable dependency checks."""

    def __init__(self, postgres: PostgresHealthCheck, redpanda: RedpandaHealthCheck) -> None:
        self._postgres = postgres
        self._redpanda = redpanda

    def ready(self) -> tuple[bool, list[HealthStatus]]:
        statuses = [self._postgres.check(), self._redpanda.check()]
        return all(status.healthy for status in statuses), statuses
