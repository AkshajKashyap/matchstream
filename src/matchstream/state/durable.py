"""State projector that delegates atomic durability to a storage boundary."""

from __future__ import annotations

from matchstream.models import CanonicalEvent
from matchstream.storage.repository import ProjectionRepository

from .models import MatchState
from .projector import ProjectionUpdate
from .transition import apply_event


class DurableMatchProjector:
    """Project events through a repository that atomically stores state and identity."""

    def __init__(self, repository: ProjectionRepository) -> None:
        self._repository = repository

    def process(self, event: CanonicalEvent) -> ProjectionUpdate:
        """Return a durable update; transition errors leave storage unchanged."""

        result = self._repository.process_event(event, apply_event)
        return ProjectionUpdate(state=result.state, applied=result.applied)

    def state_for(self, match_id: str) -> MatchState | None:
        """Load a durable projection without requiring an in-memory cache."""

        return self._repository.load_state(match_id)
