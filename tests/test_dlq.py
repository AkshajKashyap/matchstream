from __future__ import annotations

from datetime import UTC, datetime

import pytest

from matchstream.models import CanonicalEvent
from matchstream.state import (
    MatchState,
    ProjectionUpdate,
    SequenceGapError,
    apply_event,
    rebuild_state,
)
from matchstream.storage import DurableStorageError
from matchstream.streaming.dlq import (
    DeadLetterEvent,
    FailureClassification,
    SourcePosition,
    dead_letter_id,
    deserialize_dead_letter,
    serialize_dead_letter,
)
from matchstream.streaming.processing import QuarantinedEvent, project_next_with_dlq
from matchstream.streaming.retry import RetryExhaustedError, RetryPolicy


def _event(sequence: int = 1) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=f"source:match-1:event-{sequence}",
        match_id="match-1",
        sequence=sequence,
        period=1,
        match_clock_seconds=float(sequence),
        event_type="Pass",
        team=None,
        source="source",
        source_event_id=f"event-{sequence}",
    )


class _Consumer:
    def __init__(self, event: CanonicalEvent) -> None:
        self.event = event
        self.last_source_position = SourcePosition("events", 0, 12)
        self.commits = 0

    def poll(self, timeout_seconds: float) -> CanonicalEvent | None:
        event, self.event = self.event, None  # type: ignore[assignment]
        return event

    def commit(self) -> None:
        self.commits += 1


class _Publisher:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.records: list[DeadLetterEvent] = []

    def publish(self, record: DeadLetterEvent) -> None:
        if self.fail:
            raise RuntimeError("DLQ unavailable")
        self.records.append(record)


class _Projector:
    def __init__(self, failures: list[Exception]) -> None:
        self.failures = failures
        self.calls = 0

    def process(self, event: CanonicalEvent) -> ProjectionUpdate:
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return ProjectionUpdate(MatchState.initial(event.match_id), applied=True)


def test_dead_letter_contract_round_trip_and_identity_are_deterministic() -> None:
    event = _event()
    source = SourcePosition("events", 2, 9)
    record = DeadLetterEvent.from_failure(
        event,
        source,
        FailureClassification.POISON,
        SequenceGapError("gap"),
        3,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert dead_letter_id(event, source) == record.record_id
    assert deserialize_dead_letter(serialize_dead_letter(record)) == record


def test_successful_retry_commits_without_dead_letter() -> None:
    consumer = _Consumer(_event())
    projector = _Projector([DurableStorageError("temporary database error")])
    publisher = _Publisher()
    sleeps: list[float] = []

    result = project_next_with_dlq(
        consumer, projector, publisher, RetryPolicy(max_attempts=2, base_delay_seconds=0.1), 0.1, sleeps.append
    )

    assert result is not None and not isinstance(result, QuarantinedEvent)
    assert projector.calls == 2
    assert sleeps == [0.1]
    assert consumer.commits == 1
    assert publisher.records == []


def test_persistent_projection_failure_is_quarantined_before_source_commit() -> None:
    consumer = _Consumer(_event())
    projector = _Projector([SequenceGapError("gap"), SequenceGapError("gap")])
    publisher = _Publisher()

    result = project_next_with_dlq(consumer, projector, publisher, RetryPolicy(max_attempts=2), 0.1)

    assert isinstance(result, QuarantinedEvent)
    assert result.dead_letter.attempt_count == 2
    assert publisher.records == [result.dead_letter]
    assert consumer.commits == 1


def test_dlq_publish_failure_does_not_commit_source_offset() -> None:
    consumer = _Consumer(_event())
    projector = _Projector([SequenceGapError("gap")])
    publisher = _Publisher(fail=True)

    with pytest.raises(RuntimeError, match="DLQ unavailable"):
        project_next_with_dlq(consumer, projector, publisher, RetryPolicy(max_attempts=1), 0.1)

    assert consumer.commits == 0


def test_retriable_exhaustion_leaves_source_uncommitted() -> None:
    consumer = _Consumer(_event())
    projector = _Projector([DurableStorageError("database down")])
    publisher = _Publisher()

    with pytest.raises(RetryExhaustedError):
        project_next_with_dlq(consumer, projector, publisher, RetryPolicy(max_attempts=1), 0.1)

    assert consumer.commits == 0
    assert publisher.records == []


def test_rebuild_is_deterministic_and_matches_incremental_state() -> None:
    events = [_event(1), _event(2), _event(3)]
    incremental = MatchState.initial("match-1")
    for event in events:
        incremental = apply_event(incremental, event)

    assert rebuild_state("match-1", reversed(events)) == incremental
    assert rebuild_state("match-1", events) == rebuild_state("match-1", events)
