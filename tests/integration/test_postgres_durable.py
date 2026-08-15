from __future__ import annotations

import os
import time
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from matchstream.ingestion import load_statsbomb_events
from matchstream.models import CanonicalEvent, EntityReference
from matchstream.replay import MatchReplayer, ReplayConfig
from matchstream.state import DurableMatchProjector, SequenceGapError, apply_event
from matchstream.storage import DatabaseConfig, PostgresProjectionRepository
from matchstream.streaming import (
    BrokerConfig,
    RedpandaEventConsumer,
    RedpandaEventProducer,
    project_next,
    publish_replay,
)

pytestmark = pytest.mark.integration

FIXTURE = Path(__file__).parents[1] / "fixtures" / "statsbomb_events.json"


def _database_config() -> DatabaseConfig:
    return DatabaseConfig.from_environment()


def _event(match_id: str, sequence: int) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=f"test:{match_id}:event-{sequence}",
        match_id=match_id,
        sequence=sequence,
        period=1,
        match_clock_seconds=float(sequence),
        event_type="Starting XI" if sequence == 1 else "Pass",
        team=EntityReference(id="home", name="Home FC"),
        source="test",
        source_event_id=f"event-{sequence}",
    )


def test_postgres_schema_duplicate_rollback_and_restart_recovery() -> None:
    repository = PostgresProjectionRepository(_database_config())
    repository.initialize_schema()
    match_id = f"durable-{uuid4().hex}"
    first, second, gap = _event(match_id, 1), _event(match_id, 2), _event(match_id, 4)

    first_result = repository.process_event(first, apply_event)
    duplicate = repository.process_event(first, apply_event)
    restarted_repository = PostgresProjectionRepository(_database_config())
    second_result = restarted_repository.process_event(second, apply_event)

    with pytest.raises(SequenceGapError):
        restarted_repository.process_event(gap, apply_event)

    recovered = restarted_repository.load_state(match_id)
    assert first_result.applied is True
    assert duplicate.applied is False
    assert second_result.applied is True
    assert recovered == second_result.state
    assert recovered is not None and recovered.latest_sequence == 2

    with psycopg.connect(_database_config().dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM processed_events WHERE match_id = %s", (match_id,))
        assert cursor.fetchone() == (2,)


def test_redpanda_to_durable_postgres_projection_and_duplicate_redelivery() -> None:
    suffix = uuid4().hex
    match_id = f"match-{suffix}"
    broker = BrokerConfig(
        bootstrap_servers=os.environ.get("MATCHSTREAM_BROKER_ADDRESS", "localhost:19092"),
        topic=f"matchstream.durable.{suffix}",
        group_id=f"matchstream-durable-{suffix}",
    )
    repository = PostgresProjectionRepository(_database_config())
    repository.initialize_schema()
    events = load_statsbomb_events(FIXTURE, match_id)

    with RedpandaEventProducer(broker) as producer:
        publish_replay(MatchReplayer(events), producer, ReplayConfig(no_wait=True))

    projector = DurableMatchProjector(repository)
    received = 0
    deadline = time.monotonic() + 15
    with RedpandaEventConsumer(broker) as consumer:
        while time.monotonic() < deadline and received < len(events):
            processed = project_next(consumer, projector, timeout_seconds=1.0)
            if processed is not None:
                received += 1

    state = PostgresProjectionRepository(_database_config()).load_state(match_id)
    redelivery = DurableMatchProjector(repository).process(events[0])
    assert received == len(events)
    assert state is not None and state.total_processed_events == len(events)
    assert redelivery.applied is False
    assert redelivery.state == state
