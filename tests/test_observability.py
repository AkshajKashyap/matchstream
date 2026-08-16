from __future__ import annotations

import json
import logging
from typing import Self

from prometheus_client import CollectorRegistry, generate_latest
from pytest import MonkeyPatch

from matchstream.observability import server as server_module
from matchstream.observability.health import (
    HealthStatus,
    PostgresHealthCheck,
    ReadinessService,
    RedpandaHealthCheck,
)
from matchstream.observability.logging import JsonFormatter
from matchstream.observability.metrics import MatchStreamMetrics
from matchstream.observability.server import ObservabilityServer
from matchstream.storage import DatabaseConfig
from matchstream.streaming import lag as lag_module
from matchstream.streaming.lag import inspect_consumer_lag, lag_from_offsets
from matchstream.streaming.redpanda import BrokerConfig


class _Cursor:
    def execute(self, statement: str) -> None:
        assert statement == "SELECT 1"

    def fetchone(self) -> tuple[int]:
        return (1,)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Connection:
    def cursor(self) -> _Cursor:
        return _Cursor()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Metadata:
    def __init__(self) -> None:
        self.topics = {"events": object()}


class _MetadataClient:
    def list_topics(self, timeout: float) -> _Metadata:
        assert timeout == 2.0
        return _Metadata()


class _StatusCheck:
    def __init__(self, status: HealthStatus) -> None:
        self.status = status
        self.calls = 0

    def check(self) -> HealthStatus:
        self.calls += 1
        return self.status


class _TopicPartition:
    def __init__(self, topic: str, partition: int, offset: int = -1001) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset


class _Topic:
    def __init__(self) -> None:
        self.partitions = {0: object(), 1: object()}
        self.error = None


class _LagClient:
    def list_topics(self, topic: str, timeout: float) -> object:
        return type("Metadata", (), {"topics": {topic: _Topic()}})()

    def committed(self, partitions: list[_TopicPartition], timeout: float) -> list[_TopicPartition]:
        return [_TopicPartition("events", 0, 7), _TopicPartition("events", 1, -1001)]

    def position(self, partitions: list[_TopicPartition]) -> list[_TopicPartition]:
        return [_TopicPartition("events", 0, 8)]

    def get_watermark_offsets(self, partition: _TopicPartition, timeout: float) -> tuple[int, int]:
        return (2, 10) if partition.partition == 0 else (3, 12)


def test_metrics_use_bounded_labels_and_record_real_observations() -> None:
    registry = CollectorRegistry()
    telemetry = MatchStreamMetrics(registry)

    telemetry.events_received.labels(component="consumer").inc()
    telemetry.retries.labels(component="projection", failure_class="poison").inc()
    with telemetry.timer(telemetry.processing_duration, result="success"):
        pass

    exposition = generate_latest(registry).decode()
    assert "matchstream_events_received_total{component=\"consumer\"} 1.0" in exposition
    assert "matchstream_retries_total{component=\"projection\",failure_class=\"poison\"} 1.0" in exposition
    assert "matchstream_event_processing_duration_seconds_count{result=\"success\"} 1.0" in exposition
    assert "event_id" not in exposition
    assert "match_id" not in exposition


def test_json_logging_preserves_context_without_full_event_payload() -> None:
    record = logging.makeLogRecord(
        {
            "name": "matchstream.processing",
            "levelno": logging.INFO,
            "levelname": "INFO",
            "msg": "event_received",
            "args": (),
            "event_id": "source:match-1:event-1",
            "match_id": "match-1",
            "event_sequence": 1,
            "component": "processing",
        }
    )

    rendered = json.loads(JsonFormatter().format(record))

    assert rendered["message"] == "event_received"
    assert rendered["event_id"] == "source:match-1:event-1"
    assert rendered["match_id"] == "match-1"
    assert "canonical_event" not in rendered


def test_postgres_and_redpanda_health_checks_verify_real_client_operations() -> None:
    postgres = PostgresHealthCheck(
        DatabaseConfig("postgresql://example"), connect=lambda *_args, **_kwargs: _Connection()
    )
    redpanda = RedpandaHealthCheck(BrokerConfig(topic="events"), client_factory=lambda _config: _MetadataClient())

    assert postgres.check() == HealthStatus("postgres", True, "connected")
    assert redpanda.check() == HealthStatus("redpanda", True, "metadata reachable")


def test_health_checks_return_safe_unavailable_statuses() -> None:
    postgres = PostgresHealthCheck(
        DatabaseConfig("postgresql://example"),
        connect=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("connection refused")),
    )
    redpanda = RedpandaHealthCheck(
        BrokerConfig(topic="events"),
        client_factory=lambda _config: (_ for _ in ()).throw(RuntimeError("broker down")),
    )

    assert postgres.check() == HealthStatus("postgres", False, "unavailable: RuntimeError")
    assert redpanda.check() == HealthStatus("redpanda", False, "unavailable: RuntimeError")


def test_readiness_requires_every_dependency_while_liveness_is_independent() -> None:
    postgres = _StatusCheck(HealthStatus("postgres", True, "connected"))
    redpanda = _StatusCheck(HealthStatus("redpanda", False, "unavailable: timeout"))
    readiness = ReadinessService(postgres, redpanda)  # type: ignore[arg-type]

    ready, statuses = readiness.ready()

    assert ready is False
    assert statuses[1].healthy is False


class _FakeHttpServer:
    def __init__(self, _address: object, handler: object) -> None:
        self.server_address = ("127.0.0.1", 9999)
        self.handler = handler
        self.calls: list[str] = []

    def serve_forever(self) -> None:
        self.calls.append("serve")

    def shutdown(self) -> None:
        self.calls.append("shutdown")

    def server_close(self) -> None:
        self.calls.append("close")


def test_observability_server_constructs_live_ready_and_metrics_handler(monkeypatch: MonkeyPatch) -> None:
    registry = CollectorRegistry()
    MatchStreamMetrics(registry).events_received.labels(component="consumer").inc()
    readiness = ReadinessService(
        _StatusCheck(HealthStatus("postgres", True, "connected")),  # type: ignore[arg-type]
        _StatusCheck(HealthStatus("redpanda", True, "metadata reachable")),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(server_module, "ThreadingHTTPServer", _FakeHttpServer)
    server = ObservabilityServer(readiness, port=0, registry=registry)
    server.start()
    try:
        assert server.address == ("127.0.0.1", 9999)
        assert isinstance(server._server.handler, type)  # type: ignore[attr-defined]
    finally:
        server.close()


def test_lag_calculation_and_inspection_use_actual_offsets(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(
        lag_module,
        "_kafka_module",
        lambda: type("Kafka", (), {"TopicPartition": _TopicPartition}),
    )
    registry = CollectorRegistry()
    telemetry = MatchStreamMetrics(registry)

    rows = inspect_consumer_lag(BrokerConfig(topic="events"), _LagClient(), telemetry)

    assert lag_from_offsets(7, 2, 10) == 3
    assert lag_from_offsets(None, 3, 12) == 9
    assert rows == [
        lag_module.PartitionLag(0, 7, 8, 10, 3),
        lag_module.PartitionLag(1, None, None, 12, 9),
    ]
    assert b"matchstream_consumer_lag 12.0" in generate_latest(registry)
