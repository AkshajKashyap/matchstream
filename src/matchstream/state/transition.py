"""Pure transitions from one match-state projection to the next."""

from __future__ import annotations

from collections.abc import Mapping

from matchstream.models import CanonicalEvent, EntityReference

from .models import MatchState


class MatchTransitionError(ValueError):
    """Raised when an event cannot safely update a match projection."""


class MatchMismatchError(MatchTransitionError):
    """Raised when an event is applied to another match's state."""


class DuplicateEventError(MatchTransitionError):
    """Raised when the state's most recently applied event is applied again."""


class StaleEventError(MatchTransitionError):
    """Raised when an event's sequence is older than the projected sequence."""


class SequenceGapError(MatchTransitionError):
    """Raised when an event skips a sequence after the projection has started."""


class ClockRegressionError(MatchTransitionError):
    """Raised when a same-period event moves the match clock backwards."""


class ScoreReconstructionError(MatchTransitionError):
    """Raised when a recognized scoring event has no scoring team."""


def apply_event(previous: MatchState, event: CanonicalEvent) -> MatchState:
    """Purely apply one ordered canonical event to one match state.

    The first event establishes the starting sequence so a projector can start
    mid-match. Every subsequent event must have exactly the next sequence.
    """

    _validate_transition(previous, event)
    home_team, away_team = _infer_teams(previous, event)
    scores = dict(previous.score_by_team)
    _ensure_team_score(scores, home_team)
    _ensure_team_score(scores, away_team)
    if is_goal(event):
        if event.team is None:
            raise ScoreReconstructionError("a goal event must include its scoring team")
        _ensure_team_score(scores, event.team)
        scores[event.team.id] += 1

    event_counts = dict(previous.event_counts)
    event_counts[event.event_type] = event_counts.get(event.event_type, 0) + 1
    return MatchState(
        match_id=previous.match_id,
        current_period=event.period,
        current_match_clock_seconds=event.match_clock_seconds,
        latest_sequence=event.sequence,
        total_processed_events=previous.total_processed_events + 1,
        home_team=home_team,
        away_team=away_team,
        score_by_team=scores,
        possession_team=event.possession_team or previous.possession_team,
        event_counts=event_counts,
        last_event_id=event.event_id,
    )


def is_goal(event: CanonicalEvent) -> bool:
    """Return whether canonical shot metadata identifies the shot as a goal."""

    if event.event_type.casefold() != "shot":
        return False
    outcome = event.metadata.get("outcome")
    if isinstance(outcome, str):
        return outcome.casefold() == "goal"
    if isinstance(outcome, Mapping):
        name = outcome.get("name")
        return isinstance(name, str) and name.casefold() == "goal"
    return False


def _validate_transition(previous: MatchState, event: CanonicalEvent) -> None:
    if event.match_id != previous.match_id:
        raise MatchMismatchError(
            f"event match_id {event.match_id!r} does not match state {previous.match_id!r}"
        )
    if event.event_id == previous.last_event_id:
        raise DuplicateEventError(f"event {event.event_id!r} is already the latest event")
    if previous.latest_sequence is None:
        return
    if event.sequence <= previous.latest_sequence:
        raise StaleEventError(
            f"event sequence {event.sequence} is not newer than {previous.latest_sequence}"
        )
    if event.sequence != previous.latest_sequence + 1:
        raise SequenceGapError(
            f"expected sequence {previous.latest_sequence + 1}, received {event.sequence}"
        )
    if event.period < previous.current_period:
        raise ClockRegressionError(
            f"event period {event.period} is older than projected period {previous.current_period}"
        )
    if event.period == previous.current_period and event.match_clock_seconds < previous.current_match_clock_seconds:
        raise ClockRegressionError("event match clock moved backwards within the current period")


def _infer_teams(previous: MatchState, event: CanonicalEvent) -> tuple[EntityReference | None, EntityReference | None]:
    home_team, away_team = previous.home_team, previous.away_team
    if event.event_type.casefold() != "starting xi" or event.team is None:
        return home_team, away_team
    if home_team is None:
        return event.team, away_team
    if away_team is None and event.team.id != home_team.id:
        return home_team, event.team
    return home_team, away_team


def _ensure_team_score(scores: dict[str, int], team: EntityReference | None) -> None:
    if team is not None:
        scores.setdefault(team.id, 0)
