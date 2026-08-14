"""Process-local delivery identity helpers."""

from __future__ import annotations

from matchstream.models import CanonicalEvent


class EventDeduplicator:
    """Accept each canonical ``event_id`` once for the life of this process."""

    def __init__(self) -> None:
        self._seen_event_ids: set[str] = set()

    def accept(self, event: CanonicalEvent) -> bool:
        """Record an event identity and return whether it was not seen before."""

        if event.event_id in self._seen_event_ids:
            return False
        self._seen_event_ids.add(event.event_id)
        return True

    @property
    def size(self) -> int:
        """Return the number of identities retained in this process."""

        return len(self._seen_event_ids)
