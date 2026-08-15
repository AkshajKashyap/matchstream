from __future__ import annotations

import os
import time
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration

confluent_kafka = pytest.importorskip("confluent_kafka")

from matchstream.ingestion import load_statsbomb_events
from matchstream.models import CanonicalEvent
from matchstream.replay import MatchReplayer, ReplayConfig
from matchstream.state import MatchProjector
from matchstream.streaming import (
    BrokerConfig,
    RedpandaEventConsumer,
    RedpandaEventProducer,
    project_next,
    publish_replay,
)

FIXTURE = Path(__file__).parents[1] / "fixtures" / "statsbomb_events.json"


def test_redpanda_producer_consumer_round_trip() -> None:
    suffix = uuid4().hex
    config = BrokerConfig(
        bootstrap_servers=os.environ.get("MATCHSTREAM_BROKER_ADDRESS", "localhost:19092"),
        topic=f"matchstream.integration.{suffix}",
        group_id=f"matchstream-integration-{suffix}",
    )
    event = CanonicalEvent(
        event_id=f"test:match-{suffix}:event-1",
        match_id=f"match-{suffix}",
        sequence=1,
        period=1,
        match_clock_seconds=1.0,
        event_type="Pass",
        team=None,
        source="test",
        source_event_id="event-1",
    )

    with RedpandaEventProducer(config) as producer:
        producer.publish(event)

    deadline = time.monotonic() + 15
    with RedpandaEventConsumer(config) as consumer:
        while time.monotonic() < deadline:
            received = consumer.poll(1.0)
            if received is not None:
                consumer.commit()
                assert received == event
                return
    pytest.fail("did not receive the event from Redpanda within 15 seconds")


def test_redpanda_replay_to_state_projection() -> None:
    suffix = uuid4().hex
    config = BrokerConfig(
        bootstrap_servers=os.environ.get("MATCHSTREAM_BROKER_ADDRESS", "localhost:19092"),
        topic=f"matchstream.projection.{suffix}",
        group_id=f"matchstream-projection-{suffix}",
    )
    events = load_statsbomb_events(FIXTURE, match_id=f"match-{suffix}")

    with RedpandaEventProducer(config) as producer:
        publish_replay(MatchReplayer(events), producer, ReplayConfig(no_wait=True))

    projector = MatchProjector()
    deadline = time.monotonic() + 15
    processed_count = 0
    with RedpandaEventConsumer(config) as consumer:
        while time.monotonic() < deadline and processed_count < len(events):
            processed = project_next(consumer, projector, timeout_seconds=1.0)
            if processed is not None:
                processed_count += 1

    state = projector.state_for(f"match-{suffix}")
    assert processed_count == len(events)
    assert state is not None
    assert state.total_processed_events == 4
    assert state.event_counts == {"Starting XI": 1, "Pass": 2, "Half Start": 1}
