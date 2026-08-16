from __future__ import annotations

import os
import time
from uuid import uuid4

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from matchstream.models import CanonicalEvent, EntityReference
from matchstream.observability.metrics import MatchStreamMetrics
from matchstream.state import DurableMatchProjector
from matchstream.storage import DatabaseConfig, PostgresProjectionRepository
from matchstream.streaming.dlq_transport import (
    RedpandaDeadLetterConsumer,
    RedpandaDeadLetterProducer,
)
from matchstream.streaming.processing import QuarantinedEvent, project_next_with_dlq
from matchstream.streaming.redpanda import (
    BrokerConfig,
    RedpandaEventConsumer,
    RedpandaEventProducer,
)
from matchstream.streaming.retry import RetryPolicy

pytestmark = pytest.mark.integration


def _event(match_id: str, sequence: int) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=f"poison:{match_id}:event-{sequence}",
        match_id=match_id,
        sequence=sequence,
        period=1,
        match_clock_seconds=float(sequence),
        event_type="Starting XI" if sequence == 1 else "Pass",
        team=EntityReference(id="home", name="Home FC"),
        source="test",
        source_event_id=f"event-{sequence}",
    )


def test_poison_event_quarantine_allows_later_partition_event_and_rebuild() -> None:
    suffix = uuid4().hex
    match_id = f"poison-match-{suffix}"
    main = BrokerConfig(
        bootstrap_servers=os.environ.get("MATCHSTREAM_BROKER_ADDRESS", "localhost:19092"),
        topic=f"matchstream.poison.{suffix}",
        group_id=f"matchstream-poison-{suffix}",
    )
    dlq = BrokerConfig(
        bootstrap_servers=main.bootstrap_servers,
        topic=f"{main.topic}.dlq",
        group_id=f"{main.group_id}-dlq",
    )
    repository = PostgresProjectionRepository(DatabaseConfig.from_environment())
    repository.initialize_schema()
    registry = CollectorRegistry()
    telemetry = MatchStreamMetrics(registry)
    events = [_event(match_id, 1), _event(match_id, 3), _event(match_id, 2)]

    with RedpandaEventProducer(main) as producer:
        for event in events:
            producer.publish(event)

    projector = DurableMatchProjector(repository)
    received: list[object] = []
    deadline = time.monotonic() + 15
    with RedpandaEventConsumer(main) as consumer, RedpandaDeadLetterProducer(dlq) as publisher:
        while time.monotonic() < deadline and len(received) < len(events):
            result = project_next_with_dlq(
                consumer, projector, publisher, RetryPolicy(max_attempts=2), 1.0, observer=telemetry
            )
            if result is not None:
                received.append(result)

    state = repository.load_state(match_id)
    assert len(received) == 3
    assert any(isinstance(result, QuarantinedEvent) for result in received)
    assert state is not None and state.latest_sequence == 2
    assert state.total_processed_events == 2
    exposition = generate_latest(registry).decode()
    assert "matchstream_retries_total{component=\"projection\",failure_class=\"poison\"} 1.0" in exposition
    assert "matchstream_dlq_published_total 1.0" in exposition

    deadline = time.monotonic() + 15
    with RedpandaDeadLetterConsumer(dlq) as consumer:
        while time.monotonic() < deadline:
            record = consumer.poll(1.0)
            if record is not None:
                consumer.commit()
                assert record.original_event.sequence == 3
                assert record.source.topic == main.topic
                break
        else:
            pytest.fail("did not receive poison event from DLQ")

    assert repository.rebuild_match(match_id) == state
