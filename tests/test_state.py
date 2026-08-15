from __future__ import annotations

import pytest

from matchstream.models import CanonicalEvent, EntityReference
from matchstream.state import (
    ClockRegressionError,
    MatchMismatchError,
    MatchProjector,
    MatchState,
    SequenceGapError,
    StaleEventError,
    apply_event,
)

HOME = EntityReference(id="home", name="Home FC")
AWAY = EntityReference(id="away", name="Away FC")


def _event(
    sequence: int,
    *,
    match_id: str = "match-1",
    event_type: str = "Pass",
    team: EntityReference | None = HOME,
    period: int = 1,
    clock: float | None = None,
    possession_team: EntityReference | None = None,
    outcome: str | None = None,
) -> CanonicalEvent:
    metadata = {} if outcome is None else {"outcome": {"id": outcome.lower(), "name": outcome}}
    return CanonicalEvent(
        event_id=f"source:{match_id}:event-{sequence}",
        match_id=match_id,
        sequence=sequence,
        period=period,
        match_clock_seconds=float(sequence) if clock is None else clock,
        event_type=event_type,
        team=team,
        possession_team=possession_team,
        metadata=metadata,
        source="source",
        source_event_id=f"event-{sequence}",
    )


def test_initial_state_is_empty_and_typed() -> None:
    state = MatchState.initial("match-1")

    assert state.match_id == "match-1"
    assert state.current_period == 0
    assert state.latest_sequence is None
    assert state.total_processed_events == 0
    assert state.event_counts == {}
    assert state.last_event_id is None


def test_transition_updates_match_context_counts_and_identity() -> None:
    state = MatchState.initial("match-1")
    state = apply_event(state, _event(1, event_type="Starting XI", clock=0.0))
    state = apply_event(
        state,
        _event(2, possession_team=HOME, clock=4.5),
    )

    assert state.home_team == HOME
    assert state.away_team is None
    assert state.current_period == 1
    assert state.current_match_clock_seconds == 4.5
    assert state.latest_sequence == 2
    assert state.total_processed_events == 2
    assert state.event_counts == {"Starting XI": 1, "Pass": 1}
    assert state.possession_team == HOME
    assert state.last_event_id == "source:match-1:event-2"


def test_goal_score_reconstruction_and_non_goal_shots() -> None:
    state = MatchState.initial("match-1")
    events = [
        _event(1, event_type="Starting XI", team=HOME),
        _event(2, event_type="Starting XI", team=AWAY),
        _event(3, event_type="Shot", team=HOME, outcome="Goal"),
        _event(4, event_type="Shot", team=AWAY, outcome="Saved"),
        _event(5, event_type="Shot", team=AWAY, outcome="Goal"),
    ]
    for event in events:
        state = apply_event(state, event)

    assert state.home_team == HOME
    assert state.away_team == AWAY
    assert state.score_by_team == {"home": 1, "away": 1}
    assert state.event_counts["Shot"] == 3


def test_projector_isolates_interleaved_matches() -> None:
    projector = MatchProjector()
    projector.process(_event(1, match_id="match-a", event_type="Starting XI"))
    projector.process(_event(1, match_id="match-b", event_type="Starting XI", team=AWAY))
    projector.process(_event(2, match_id="match-a", event_type="Shot", outcome="Goal"))

    first = projector.state_for("match-a")
    second = projector.state_for("match-b")
    assert first is not None and first.total_processed_events == 2
    assert first.score_by_team == {"home": 1}
    assert second is not None and second.total_processed_events == 1
    assert second.score_by_team == {"away": 0}
    assert projector.match_count == 2


def test_duplicate_event_does_not_mutate_projected_state_twice() -> None:
    projector = MatchProjector()
    event = _event(1)

    first = projector.process(event)
    duplicate = projector.process(event)

    assert first.applied is True
    assert duplicate.applied is False
    assert duplicate.state.total_processed_events == 1


def test_projector_does_not_record_failed_event_as_duplicate() -> None:
    projector = MatchProjector()
    projector.process(_event(1))
    with pytest.raises(SequenceGapError):
        projector.process(_event(3))

    projector.process(_event(2))
    recovered = projector.process(_event(3))

    assert recovered.applied is True
    assert recovered.state.latest_sequence == 3


@pytest.mark.parametrize(
    ("sequence", "clock", "match_id", "error"),
    [
        (0, 0.0, "match-1", StaleEventError),
        (3, 3.0, "match-1", SequenceGapError),
        (2, 0.5, "match-1", ClockRegressionError),
        (2, 2.0, "other", MatchMismatchError),
    ],
)
def test_transition_rejects_invalid_ordering_or_match(
    sequence: int, clock: float, match_id: str, error: type[Exception]
) -> None:
    previous = apply_event(MatchState.initial("match-1"), _event(1, clock=1.0))

    with pytest.raises(error):
        apply_event(previous, _event(sequence, clock=clock, match_id=match_id))


def test_transition_rejects_an_event_from_an_earlier_period() -> None:
    previous = apply_event(MatchState.initial("match-1"), _event(1, period=2, clock=1.0))

    with pytest.raises(ClockRegressionError, match="older than projected period"):
        apply_event(previous, _event(2, period=1, clock=50.0))


def test_repeated_valid_transition_sequence_is_deterministic() -> None:
    events = [_event(1), _event(2), _event(3, event_type="Shot", outcome="Goal")]

    def project() -> MatchState:
        state = MatchState.initial("match-1")
        for event in events:
            state = apply_event(state, event)
        return state

    assert project() == project()
