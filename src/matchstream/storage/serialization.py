"""Explicit JSON-compatible serialization for durable MatchState snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from matchstream.models import EntityReference
from matchstream.state.models import MatchState, StateValidationError

STATE_SCHEMA_VERSION = 1


class StateSerializationError(ValueError):
    """Raised when a durable snapshot cannot be validated as MatchState."""


def state_to_dict(state: MatchState) -> dict[str, Any]:
    """Return the explicit JSON-compatible representation stored in PostgreSQL."""

    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "match_id": state.match_id,
        "current_period": state.current_period,
        "current_match_clock_seconds": state.current_match_clock_seconds,
        "latest_sequence": state.latest_sequence,
        "total_processed_events": state.total_processed_events,
        "home_team": _entity_to_dict(state.home_team),
        "away_team": _entity_to_dict(state.away_team),
        "score_by_team": dict(state.score_by_team),
        "possession_team": _entity_to_dict(state.possession_team),
        "event_counts": dict(state.event_counts),
        "last_event_id": state.last_event_id,
    }


def state_from_dict(value: object) -> MatchState:
    """Validate a stored JSON object and reconstruct a MatchState value."""

    raw = _mapping(value, "state")
    if _integer(raw.get("schema_version"), "state.schema_version") != STATE_SCHEMA_VERSION:
        raise StateSerializationError("unsupported MatchState schema version")
    required = (
        "match_id",
        "current_period",
        "current_match_clock_seconds",
        "latest_sequence",
        "total_processed_events",
        "home_team",
        "away_team",
        "score_by_team",
        "possession_team",
        "event_counts",
        "last_event_id",
    )
    for field_name in required:
        if field_name not in raw:
            raise StateSerializationError(f"state is missing required field '{field_name}'")
    try:
        return MatchState(
            match_id=_string(raw["match_id"], "state.match_id"),
            current_period=_integer(raw["current_period"], "state.current_period"),
            current_match_clock_seconds=_number(
                raw["current_match_clock_seconds"], "state.current_match_clock_seconds"
            ),
            latest_sequence=_optional_integer(raw["latest_sequence"], "state.latest_sequence"),
            total_processed_events=_integer(
                raw["total_processed_events"], "state.total_processed_events"
            ),
            home_team=_entity_from_dict(raw["home_team"], "state.home_team"),
            away_team=_entity_from_dict(raw["away_team"], "state.away_team"),
            score_by_team=_score_map(raw["score_by_team"]),
            possession_team=_entity_from_dict(raw["possession_team"], "state.possession_team"),
            event_counts=_count_map(raw["event_counts"]),
            last_event_id=_optional_string(raw["last_event_id"], "state.last_event_id"),
        )
    except StateValidationError as error:
        raise StateSerializationError(f"invalid MatchState snapshot: {error}") from error


def _entity_to_dict(entity: EntityReference | None) -> dict[str, str] | None:
    return None if entity is None else {"id": entity.id, "name": entity.name}


def _entity_from_dict(value: object, field_name: str) -> EntityReference | None:
    if value is None:
        return None
    raw = _mapping(value, field_name)
    return EntityReference(
        id=_string(raw.get("id"), f"{field_name}.id"),
        name=_string(raw.get("name"), f"{field_name}.name"),
    )


def _score_map(value: object) -> dict[str, int]:
    raw = _mapping(value, "state.score_by_team")
    return {
        _string(key, "state.score_by_team key"): _integer(score, "state score")
        for key, score in raw.items()
    }


def _count_map(value: object) -> dict[str, int]:
    raw = _mapping(value, "state.event_counts")
    return {
        _string(key, "state.event_counts key"): _integer(count, "state event count")
        for key, count in raw.items()
    }


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise StateSerializationError(f"{field_name} must be an object")
    return value


def _string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise StateSerializationError(f"{field_name} must be a non-empty string")
    return value


def _optional_string(value: object, field_name: str) -> str | None:
    return None if value is None else _string(value, field_name)


def _integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StateSerializationError(f"{field_name} must be an integer")
    return value


def _optional_integer(value: object, field_name: str) -> int | None:
    return None if value is None else _integer(value, field_name)


def _number(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise StateSerializationError(f"{field_name} must be a number")
    return float(value)
