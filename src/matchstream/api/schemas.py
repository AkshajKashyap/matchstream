"""Stable Pydantic schemas for MatchStream's HTTP and WebSocket boundaries."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from matchstream.models import CanonicalEvent, EntityReference
from matchstream.storage import StoredMatch


class ApiEntity(BaseModel):
    """A football entity intentionally separated from its domain dataclass."""

    id: str
    name: str

    @classmethod
    def from_domain(cls, entity: EntityReference | None) -> ApiEntity | None:
        return None if entity is None else cls(id=entity.id, name=entity.name)


class MatchStateResponse(BaseModel):
    """Current durable materialized projection returned by the API."""

    model_config = ConfigDict(frozen=True)

    match_id: str
    period: int
    match_clock_seconds: float
    latest_sequence: int | None
    total_processed_events: int
    home_team: ApiEntity | None
    away_team: ApiEntity | None
    score: dict[str, int]
    possession_team: ApiEntity | None
    event_counts: dict[str, int]
    last_event_id: str | None
    updated_at: datetime

    @classmethod
    def from_stored_match(cls, stored: StoredMatch) -> MatchStateResponse:
        state = stored.state
        return cls(
            match_id=state.match_id,
            period=state.current_period,
            match_clock_seconds=state.current_match_clock_seconds,
            latest_sequence=state.latest_sequence,
            total_processed_events=state.total_processed_events,
            home_team=ApiEntity.from_domain(state.home_team),
            away_team=ApiEntity.from_domain(state.away_team),
            score=dict(state.score_by_team),
            possession_team=ApiEntity.from_domain(state.possession_team),
            event_counts=dict(state.event_counts),
            last_event_id=state.last_event_id,
            updated_at=stored.updated_at,
        )


class MatchSummaryResponse(BaseModel):
    """Bounded list representation for match discovery."""

    match_id: str
    period: int
    match_clock_seconds: float
    latest_sequence: int | None
    home_team: ApiEntity | None
    away_team: ApiEntity | None
    score: dict[str, int]
    updated_at: datetime

    @classmethod
    def from_stored_match(cls, stored: StoredMatch) -> MatchSummaryResponse:
        state = stored.state
        return cls(
            match_id=state.match_id,
            period=state.current_period,
            match_clock_seconds=state.current_match_clock_seconds,
            latest_sequence=state.latest_sequence,
            home_team=ApiEntity.from_domain(state.home_team),
            away_team=ApiEntity.from_domain(state.away_team),
            score=dict(state.score_by_team),
            updated_at=stored.updated_at,
        )


class MatchListResponse(BaseModel):
    """Deterministic match page using a match-ID cursor."""

    matches: list[MatchSummaryResponse]
    next_after_match_id: str | None


class CanonicalEventResponse(BaseModel):
    """Read-only canonical history representation, not a raw provider event."""

    event_id: str
    match_id: str
    sequence: int
    period: int
    match_clock_seconds: float
    event_type: str
    team: ApiEntity | None
    player: ApiEntity | None
    location: list[float] | None
    possession_id: str | None
    possession_team: ApiEntity | None
    metadata: dict[str, Any]
    source: str
    source_event_id: str

    @classmethod
    def from_domain(cls, event: CanonicalEvent) -> CanonicalEventResponse:
        return cls(
            event_id=event.event_id,
            match_id=event.match_id,
            sequence=event.sequence,
            period=event.period,
            match_clock_seconds=event.match_clock_seconds,
            event_type=event.event_type,
            team=ApiEntity.from_domain(event.team),
            player=ApiEntity.from_domain(event.player),
            location=list(event.location) if event.location is not None else None,
            possession_id=event.possession_id,
            possession_team=ApiEntity.from_domain(event.possession_team),
            metadata=dict(event.metadata),
            source=event.source,
            source_event_id=event.source_event_id,
        )


class EventHistoryResponse(BaseModel):
    """Bounded canonical history page in deterministic sequence order."""

    events: list[CanonicalEventResponse]
    next_after_sequence: int | None


class ApiErrorResponse(BaseModel):
    """Safe public error detail used for documented route failures."""

    detail: str


class WebSocketSnapshot(BaseModel):
    """Initial WebSocket message containing the current durable state."""

    type: Literal["snapshot"] = "snapshot"
    protocol_version: Literal[1] = 1
    state: MatchStateResponse


class WebSocketStateUpdate(BaseModel):
    """Best-effort post-commit WebSocket state update."""

    type: Literal["state_update"] = "state_update"
    protocol_version: Literal[1] = 1
    state: MatchStateResponse
