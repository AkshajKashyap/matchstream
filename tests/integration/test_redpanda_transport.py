from __future__ import annotations

import os
import time
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration

confluent_kafka = pytest.importorskip("confluent_kafka")

from matchstream.models import CanonicalEvent
from matchstream.streaming import BrokerConfig, RedpandaEventConsumer, RedpandaEventProducer


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
