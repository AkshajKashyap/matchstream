from __future__ import annotations

import json
import os
from urllib.request import urlopen
from uuid import uuid4

import pytest
from prometheus_client import CollectorRegistry

from matchstream.models import CanonicalEvent
from matchstream.observability.health import (
    PostgresHealthCheck,
    ReadinessService,
    RedpandaHealthCheck,
)
from matchstream.observability.metrics import MatchStreamMetrics
from matchstream.observability.server import ObservabilityServer
from matchstream.storage import DatabaseConfig
from matchstream.streaming.redpanda import BrokerConfig, RedpandaEventProducer

pytestmark = pytest.mark.integration


def test_metrics_endpoint_and_readiness_use_live_dependencies() -> None:
    topic = f"matchstream.observability.{uuid4().hex}"
    config = BrokerConfig(
        bootstrap_servers=os.environ.get("MATCHSTREAM_BROKER_ADDRESS", "localhost:19092"),
        topic=topic,
    )
    # Metadata readiness requires the topic; Kafka-compatible brokers create it on first publish.
    with RedpandaEventProducer(config) as producer:
        producer.publish(
            CanonicalEvent(
                event_id=f"observability:{topic}:event-1",
                match_id="observability-match",
                sequence=1,
                period=1,
                match_clock_seconds=1.0,
                event_type="Pass",
                team=None,
                source="test",
                source_event_id="event-1",
            )
        )
    registry = CollectorRegistry()
    MatchStreamMetrics(registry).events_received.labels(component="consumer").inc()
    server = ObservabilityServer(
        ReadinessService(
            PostgresHealthCheck(DatabaseConfig.from_environment()), RedpandaHealthCheck(config)
        ),
        port=0,
        registry=registry,
    )
    server.start()
    host, port = server.address
    try:
        assert json.loads(urlopen(f"http://{host}:{port}/health/live", timeout=3).read()) == {
            "status": "live"
        }
        readiness = json.loads(urlopen(f"http://{host}:{port}/health/ready", timeout=3).read())
        assert readiness["status"] == "ready"
        assert b"matchstream_events_received_total" in urlopen(
            f"http://{host}:{port}/metrics", timeout=3
        ).read()
    finally:
        server.close()
