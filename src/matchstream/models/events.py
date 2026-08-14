"""Canonical, source-independent football event types.

Dataclasses keep MatchStream's foundation dependency-free while preserving
runtime validation at the boundary where external data becomes internal data.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from math import isfinite
from typing import TypeAlias

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]


class EventValidationError(ValueError):
    """Raised when an object cannot be represented as a canonical event."""


@dataclass(frozen=True, slots=True)
class EntityReference:
    """A stable identifier and display name for a football entity."""

    id: str
    name: str

    def __post_init__(self) -> None:
        if not self.id.strip():
            raise EventValidationError("entity id must be a non-empty string")
        if not self.name.strip():
            raise EventValidationError("entity name must be a non-empty string")


@dataclass(frozen=True, slots=True)
class CanonicalEvent:
    """The source-independent event emitted by MatchStream.

    ``match_clock_seconds`` is elapsed time within ``period``.  Periods are
    separate because the clock normally resets at half-time.  ``metadata`` is
    intentionally a small extension point for selected event-specific values,
    not a copy of a provider's raw event payload.
    """

    event_id: str
    match_id: str
    sequence: int
    period: int
    match_clock_seconds: float
    event_type: str
    team: EntityReference | None
    player: EntityReference | None = None
    location: tuple[float, ...] | None = None
    possession_id: str | None = None
    possession_team: EntityReference | None = None
    metadata: Mapping[str, JSONValue] = field(default_factory=dict)
    source: str = "statsbomb"
    source_event_id: str = ""

    def __post_init__(self) -> None:
        for field_name, value in (
            ("event_id", self.event_id),
            ("match_id", self.match_id),
            ("event_type", self.event_type),
            ("source", self.source),
            ("source_event_id", self.source_event_id),
        ):
            if not isinstance(value, str) or not value.strip():
                raise EventValidationError(f"{field_name} must be a non-empty string")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 0:
            raise EventValidationError("sequence must be a non-negative integer")
        if isinstance(self.period, bool) or not isinstance(self.period, int) or self.period < 1:
            raise EventValidationError("period must be a positive integer")
        if (
            isinstance(self.match_clock_seconds, bool)
            or not isinstance(self.match_clock_seconds, (int, float))
            or not isfinite(self.match_clock_seconds)
            or self.match_clock_seconds < 0
        ):
            raise EventValidationError("match_clock_seconds must be a finite non-negative number")
        if self.location is not None and (
            len(self.location) < 2
            or not all(
                not isinstance(value, bool) and isinstance(value, (int, float)) and isfinite(value)
                for value in self.location
            )
        ):
            raise EventValidationError("location must contain at least two finite coordinates")
        if self.possession_id is not None and not self.possession_id.strip():
            raise EventValidationError("possession_id must be non-empty when provided")


def event_order_key(event: CanonicalEvent) -> tuple[int, float, int, str]:
    """Return MatchStream's stable chronological ordering key."""

    return (event.period, event.match_clock_seconds, event.sequence, event.event_id)
