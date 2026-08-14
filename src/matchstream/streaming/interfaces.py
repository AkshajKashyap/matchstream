"""Small broker-independent boundaries for event delivery."""

from __future__ import annotations

from typing import Protocol

from matchstream.models import CanonicalEvent


class EventPublisher(Protocol):
    """Publishes validated canonical events to an external transport."""

    def publish(self, event: CanonicalEvent) -> None:
        """Publish one event or raise a transport-specific error."""

    def close(self) -> None:
        """Release transport resources."""


class EventConsumer(Protocol):
    """Reads validated canonical events from an external transport."""

    def poll(self, timeout_seconds: float) -> CanonicalEvent | None:
        """Return the next event or ``None`` when no event arrives in time."""

    def close(self) -> None:
        """Release transport resources."""
