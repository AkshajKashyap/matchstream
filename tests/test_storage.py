from __future__ import annotations

import json

import pytest

from matchstream.models import CanonicalEvent, EntityReference
from matchstream.state import DurableMatchProjector, MatchState, SequenceGapError, apply_event
from matchstream.storage import (
    DurableProjectionResult,
    ProjectionRepository,
    StateSerializationError,
    state_from_dict,
    state_to_dict,
)
from matchstream.storage.repository import Transition
from matchstream.streaming import project_next

HOME = EntityReference(id="home", name="Home FC")


def _event(sequence: int) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=f"source:match-1:event-{sequence}",
        match_id="match-1",
        sequence=sequence,
        period=1,
        match_clock_seconds=float(sequence),
        event_type="Starting XI" if sequence == 1 else "Pass",
        team=HOME,
        source="source",
        source_event_id=f"event-{sequence}",
    )


class _TransactionalFakeRepository(ProjectionRepository):
    def __init__(self) -> None:
        self.states: dict[str, MatchState] = {}
        self.processed_event_ids: set[str] = set()
        self.fail_after_transition = False

    def process_event(self, event: CanonicalEvent, transition: Transition) -> DurableProjectionResult:
        if event.event_id in self.processed_event_ids:
            return DurableProjectionResult(self.states[event.match_id], applied=False)
        previous = self.states.get(event.match_id, MatchState.initial(event.match_id))
        next_state = transition(previous, event)
        if self.fail_after_transition:
            raise RuntimeError("simulated database transaction failure")
        self.states[event.match_id] = next_state
        self.processed_event_ids.add(event.event_id)
        return DurableProjectionResult(next_state, applied=True)

    def load_state(self, match_id: str) -> MatchState | None:
        return self.states.get(match_id)


class _Consumer:
    def __init__(self, events: list[CanonicalEvent], fail_commit: bool = False) -> None:
        self.events = events
        self.fail_commit = fail_commit
        self.commits = 0

    def poll(self, timeout_seconds: float) -> CanonicalEvent | None:
        return self.events.pop(0) if self.events else None

    def commit(self) -> None:
        if self.fail_commit:
            raise RuntimeError("simulated Kafka commit failure")
        self.commits += 1

    def close(self) -> None:
        pass


def test_match_state_snapshot_round_trips_through_json() -> None:
    state = apply_event(MatchState.initial("match-1"), _event(1))

    serialized = state_to_dict(state)

    assert state_from_dict(json.loads(json.dumps(serialized))) == state


def test_invalid_snapshot_fails_validation() -> None:
    with pytest.raises(StateSerializationError, match="missing required field 'match_id'"):
        state_from_dict({"schema_version": 1})


def test_durable_projector_recovery_matches_continuous_processing() -> None:
    events = [_event(1), _event(2), _event(3)]
    continuous_repository = _TransactionalFakeRepository()
    continuous = DurableMatchProjector(continuous_repository)
    for event in events:
        continuous.process(event)

    restart_repository = _TransactionalFakeRepository()
    before_restart = DurableMatchProjector(restart_repository)
    before_restart.process(events[0])
    before_restart.process(events[1])
    after_restart = DurableMatchProjector(restart_repository)
    after_restart.process(events[2])

    assert after_restart.state_for("match-1") == continuous.state_for("match-1")


def test_transition_failure_does_not_persist_identity_or_state() -> None:
    repository = _TransactionalFakeRepository()
    projector = DurableMatchProjector(repository)
    projector.process(_event(1))

    with pytest.raises(SequenceGapError):
        projector.process(_event(3))

    assert repository.processed_event_ids == {_event(1).event_id}
    assert repository.load_state("match-1").latest_sequence == 1  # type: ignore[union-attr]


def test_simulated_database_failure_has_no_partial_durable_update() -> None:
    repository = _TransactionalFakeRepository()
    projector = DurableMatchProjector(repository)
    repository.fail_after_transition = True

    with pytest.raises(RuntimeError, match="database transaction failure"):
        projector.process(_event(1))

    assert repository.states == {}
    assert repository.processed_event_ids == set()


def test_db_success_then_kafka_commit_failure_is_safe_on_redelivery() -> None:
    repository = _TransactionalFakeRepository()
    projector = DurableMatchProjector(repository)
    event = _event(1)

    with pytest.raises(RuntimeError, match="Kafka commit failure"):
        project_next(_Consumer([event], fail_commit=True), projector, timeout_seconds=0.1)

    persisted = projector.state_for("match-1")
    redelivery_consumer = _Consumer([event])
    redelivery = project_next(redelivery_consumer, DurableMatchProjector(repository), timeout_seconds=0.1)

    assert persisted is not None and persisted.total_processed_events == 1
    assert redelivery is not None and redelivery.update.applied is False
    assert redelivery_consumer.commits == 1
    assert repository.load_state("match-1") == persisted
