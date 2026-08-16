"""Storage boundary for atomic durable event projection."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from matchstream.models import CanonicalEvent
from matchstream.state.models import MatchState


@dataclass(frozen=True, slots=True)
class DurableProjectionResult:
    """A durable snapshot and whether this delivery changed that snapshot."""

    state: MatchState
    applied: bool


@dataclass(frozen=True, slots=True)
class StoredMatch:
    """A durable state snapshot and the time PostgreSQL last committed it."""

    state: MatchState
    updated_at: datetime


Transition = Callable[[MatchState, CanonicalEvent], MatchState]


class ProjectionRepository(Protocol):
    """Persists a state snapshot and event identity in one transaction."""

    def process_event(self, event: CanonicalEvent, transition: Transition) -> DurableProjectionResult:
        """Durably process an event once or return its already-persisted state."""

    def load_state(self, match_id: str) -> MatchState | None:
        """Load one persisted state snapshot."""

    def get_match(self, match_id: str) -> StoredMatch | None:
        """Load a durable state snapshot with its commit timestamp."""

    def list_matches(self, limit: int, after_match_id: str | None = None) -> list[StoredMatch]:
        """List durable snapshots in deterministic match-ID order."""

    def list_match_events(
        self, match_id: str, after_sequence: int | None, limit: int
    ) -> list[CanonicalEvent]:
        """List a bounded canonical history page in sequence order."""
