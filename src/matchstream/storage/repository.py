"""Storage boundary for atomic durable event projection."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from matchstream.models import CanonicalEvent
from matchstream.state.models import MatchState


@dataclass(frozen=True, slots=True)
class DurableProjectionResult:
    """A durable snapshot and whether this delivery changed that snapshot."""

    state: MatchState
    applied: bool


Transition = Callable[[MatchState, CanonicalEvent], MatchState]


class ProjectionRepository(Protocol):
    """Persists a state snapshot and event identity in one transaction."""

    def process_event(self, event: CanonicalEvent, transition: Transition) -> DurableProjectionResult:
        """Durably process an event once or return its already-persisted state."""

    def load_state(self, match_id: str) -> MatchState | None:
        """Load one persisted state snapshot."""
