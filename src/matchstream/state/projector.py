"""Process-local, multi-match state projection management."""

from __future__ import annotations

from dataclasses import dataclass

from matchstream.models import CanonicalEvent
from matchstream.streaming.deduplication import EventDeduplicator

from .models import MatchState
from .transition import apply_event


@dataclass(frozen=True, slots=True)
class ProjectionUpdate:
    """The resulting state and whether a newly received event changed it."""

    state: MatchState
    applied: bool


class MatchProjector:
    """Maintain independent in-memory projections keyed by canonical match ID."""

    def __init__(self, deduplicator: EventDeduplicator | None = None) -> None:
        self._states: dict[str, MatchState] = {}
        self._deduplicator = deduplicator or EventDeduplicator()

    def process(self, event: CanonicalEvent) -> ProjectionUpdate:
        """Project an event once, recording identity only after a successful transition."""

        if self._deduplicator.seen(event):
            state = self._states.get(event.match_id)
            if state is None:
                raise RuntimeError(f"duplicate event {event.event_id!r} has no projected match state")
            return ProjectionUpdate(state=state, applied=False)

        previous = self._states.get(event.match_id, MatchState.initial(event.match_id))
        next_state = apply_event(previous, event)
        self._states[event.match_id] = next_state
        self._deduplicator.record(event)
        return ProjectionUpdate(state=next_state, applied=True)

    def state_for(self, match_id: str) -> MatchState | None:
        """Return a match's current projection, if it has received an event."""

        return self._states.get(match_id)

    @property
    def match_count(self) -> int:
        """Return the number of matches with an in-memory projection."""

        return len(self._states)
