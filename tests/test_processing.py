from __future__ import annotations

import pytest

from matchstream.models import CanonicalEvent
from matchstream.state import MatchProjector, SequenceGapError
from matchstream.streaming import project_next


def _event(sequence: int) -> CanonicalEvent:
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


class _FakeAcknowledgingConsumer:
    def __init__(self, events: list[CanonicalEvent]) -> None:
        self.events = events
        self.commit_count = 0

    def poll(self, timeout_seconds: float) -> CanonicalEvent | None:
        return self.events.pop(0) if self.events else None

    def commit(self) -> None:
        self.commit_count += 1

    def close(self) -> None:
        pass


def test_successful_projection_is_committed_after_processing() -> None:
    consumer = _FakeAcknowledgingConsumer([_event(1)])
    projector = MatchProjector()

    processed = project_next(consumer, projector, timeout_seconds=0.1)

    assert processed is not None and processed.update.applied is True
    assert consumer.commit_count == 1
    assert projector.state_for("match-1") == processed.update.state


def test_failed_projection_is_not_committed() -> None:
    projector = MatchProjector()
    projector.process(_event(1))
    consumer = _FakeAcknowledgingConsumer([_event(3)])

    with pytest.raises(SequenceGapError):
        project_next(consumer, projector, timeout_seconds=0.1)

    assert consumer.commit_count == 0


def test_duplicate_projection_is_safe_and_can_be_committed() -> None:
    event = _event(1)
    consumer = _FakeAcknowledgingConsumer([event, event])
    projector = MatchProjector()

    first = project_next(consumer, projector, timeout_seconds=0.1)
    duplicate = project_next(consumer, projector, timeout_seconds=0.1)

    assert first is not None and first.update.applied is True
    assert duplicate is not None and duplicate.update.applied is False
    assert consumer.commit_count == 2
    state = projector.state_for("match-1")
    assert state is not None and state.total_processed_events == 1
