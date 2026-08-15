"""Typed, source-independent representation of the current match projection."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from math import isfinite

from matchstream.models import EntityReference


class StateValidationError(ValueError):
    """Raised when a match-state invariant is violated."""


@dataclass(frozen=True, slots=True)
class MatchState:
    """Current derived state for one match, reconstructed from canonical events."""

    match_id: str
    current_period: int = 0
    current_match_clock_seconds: float = 0.0
    latest_sequence: int | None = None
    total_processed_events: int = 0
    home_team: EntityReference | None = None
    away_team: EntityReference | None = None
    score_by_team: Mapping[str, int] = field(default_factory=dict)
    possession_team: EntityReference | None = None
    event_counts: Mapping[str, int] = field(default_factory=dict)
    last_event_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.match_id, str) or not self.match_id.strip():
            raise StateValidationError("match_id must be a non-empty string")
        if isinstance(self.current_period, bool) or not isinstance(self.current_period, int) or self.current_period < 0:
            raise StateValidationError("current_period must be a non-negative integer")
        if (
            isinstance(self.current_match_clock_seconds, bool)
            or not isinstance(self.current_match_clock_seconds, (int, float))
            or not isfinite(self.current_match_clock_seconds)
            or self.current_match_clock_seconds < 0
        ):
            raise StateValidationError("current_match_clock_seconds must be finite and non-negative")
        if self.latest_sequence is not None and (
            isinstance(self.latest_sequence, bool)
            or not isinstance(self.latest_sequence, int)
            or self.latest_sequence < 0
        ):
            raise StateValidationError("latest_sequence must be a non-negative integer when provided")
        if (
            isinstance(self.total_processed_events, bool)
            or not isinstance(self.total_processed_events, int)
            or self.total_processed_events < 0
        ):
            raise StateValidationError("total_processed_events must be a non-negative integer")
        if not isinstance(self.score_by_team, Mapping):
            raise StateValidationError("score_by_team must be a mapping")
        if not isinstance(self.event_counts, Mapping):
            raise StateValidationError("event_counts must be a mapping")
        if self.total_processed_events == 0 and self.last_event_id is not None:
            raise StateValidationError("an empty state cannot have a last_event_id")
        if self.total_processed_events > 0 and not self.last_event_id:
            raise StateValidationError("a non-empty state must have a last_event_id")
        if self.home_team is not None and self.away_team is not None and self.home_team.id == self.away_team.id:
            raise StateValidationError("home_team and away_team must be different")
        for team_id, score in self.score_by_team.items():
            if not isinstance(team_id, str) or not team_id.strip():
                raise StateValidationError("score_by_team keys must be non-empty strings")
            if isinstance(score, bool) or not isinstance(score, int) or score < 0:
                raise StateValidationError("scores must be non-negative integers")
        for event_type, count in self.event_counts.items():
            if not isinstance(event_type, str) or not event_type.strip():
                raise StateValidationError("event count keys must be non-empty strings")
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise StateValidationError("event counts must be non-negative integers")
        if self.total_processed_events != sum(self.event_counts.values()):
            raise StateValidationError("total_processed_events must equal the sum of event counts")

    @classmethod
    def initial(cls, match_id: str) -> MatchState:
        """Return the empty projection for a match."""

        return cls(match_id=match_id)

    def score_for(self, team: EntityReference | None) -> int:
        """Return a team's score, or zero when that team is not known yet."""

        return 0 if team is None else self.score_by_team.get(team.id, 0)
