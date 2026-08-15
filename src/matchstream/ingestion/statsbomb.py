"""StatsBomb Open Data JSON ingestion.

This module is the only Milestone 1 component that understands StatsBomb's raw
event shape.  It converts a deliberately selected subset into canonical events.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from itertools import pairwise
from math import isfinite
from pathlib import Path
from typing import Any

from matchstream.models import (
    CanonicalEvent,
    EntityReference,
    EventValidationError,
    event_order_key,
)


class StatsBombIngestionError(ValueError):
    """Raised when a StatsBomb event file cannot be read or converted."""


def load_statsbomb_events(path: str | Path, match_id: str | int) -> list[CanonicalEvent]:
    """Load a StatsBomb Open Data event-array JSON file for one match."""

    source_path = Path(path)
    try:
        raw_events = json.loads(source_path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise StatsBombIngestionError(f"StatsBomb file does not exist: {source_path}") from error
    except OSError as error:
        raise StatsBombIngestionError(f"could not read StatsBomb file {source_path}: {error}") from error
    except json.JSONDecodeError as error:
        raise StatsBombIngestionError(f"invalid JSON in {source_path}: {error.msg}") from error
    return convert_statsbomb_events(raw_events, match_id)


def convert_statsbomb_events(raw_events: object, match_id: str | int) -> list[CanonicalEvent]:
    """Convert a raw StatsBomb event array into stable chronological order."""

    normalized_match_id = _identifier(match_id, "match_id")
    if not isinstance(raw_events, list):
        raise StatsBombIngestionError("StatsBomb event JSON must be an array")

    converted: list[tuple[int, CanonicalEvent]] = []
    for source_position, raw_event in enumerate(raw_events):
        try:
            converted.append(
                (source_position, convert_statsbomb_event(raw_event, normalized_match_id))
            )
        except (EventValidationError, TypeError, ValueError) as error:
            raise StatsBombIngestionError(
                f"invalid StatsBomb event at array position {source_position}: {error}"
            ) from error

    ordered_events = [
        event
        for _, event in sorted(converted, key=lambda item: (*event_order_key(item[1]), item[0]))
    ]
    for previous, event in pairwise(ordered_events):
        if event.sequence != previous.sequence + 1:
            raise StatsBombIngestionError(
                "StatsBomb event indexes must be contiguous after ordering; "
                f"expected {previous.sequence + 1}, received {event.sequence}"
            )
    return ordered_events


def convert_statsbomb_event(raw_event: object, match_id: str | int) -> CanonicalEvent:
    """Convert one StatsBomb event object; parsing remains separate from replay."""

    normalized_match_id = _identifier(match_id, "match_id")
    raw = _mapping(raw_event, "event")
    source_event_id = _identifier(_required(raw, "id"), "id")
    sequence = _non_negative_integer(_required(raw, "index"), "index")
    period = _positive_integer(_required(raw, "period"), "period")
    event_type = _named_value(_required(raw, "type"), "type")

    return CanonicalEvent(
        event_id=f"statsbomb:{normalized_match_id}:{source_event_id}",
        match_id=normalized_match_id,
        sequence=sequence,
        period=period,
        match_clock_seconds=_parse_timestamp(_required(raw, "timestamp")),
        event_type=event_type,
        team=_entity(raw.get("team"), "team"),
        player=_entity(raw.get("player"), "player"),
        location=_location(raw.get("location")),
        possession_id=_optional_identifier(raw.get("possession"), "possession"),
        possession_team=_entity(raw.get("possession_team"), "possession_team"),
        metadata=_metadata(raw, event_type),
        source="statsbomb",
        source_event_id=source_event_id,
    )


def _required(raw: Mapping[str, Any], field_name: str) -> Any:
    if field_name not in raw or raw[field_name] is None:
        raise EventValidationError(f"missing required field '{field_name}'")
    return raw[field_name]


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EventValidationError(f"{field_name} must be an object")
    return value


def _identifier(value: object, field_name: str) -> str:
    if isinstance(value, bool) or value is None:
        raise EventValidationError(f"{field_name} must be a non-empty identifier")
    normalized = str(value).strip()
    if not normalized:
        raise EventValidationError(f"{field_name} must be a non-empty identifier")
    return normalized


def _optional_identifier(value: object, field_name: str) -> str | None:
    return None if value is None else _identifier(value, field_name)


def _non_negative_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EventValidationError(f"{field_name} must be a non-negative integer")
    return value


def _positive_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise EventValidationError(f"{field_name} must be a positive integer")
    return value


def _named_value(value: object, field_name: str) -> str:
    object_value = _mapping(value, field_name)
    return _identifier(_required(object_value, "name"), f"{field_name}.name")


def _entity(value: object, field_name: str) -> EntityReference | None:
    if value is None:
        return None
    object_value = _mapping(value, field_name)
    return EntityReference(
        id=_identifier(_required(object_value, "id"), f"{field_name}.id"),
        name=_identifier(_required(object_value, "name"), f"{field_name}.name"),
    )


def _location(value: object) -> tuple[float, ...] | None:
    if value is None:
        return None
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or len(value) < 2:
        raise EventValidationError("location must be an array with at least two coordinates")
    coordinates: list[float] = []
    for coordinate in value:
        if isinstance(coordinate, bool) or not isinstance(coordinate, (int, float)):
            raise EventValidationError("location coordinates must be numbers")
        coordinates.append(float(coordinate))
    return tuple(coordinates)


def _parse_timestamp(value: object) -> float:
    if not isinstance(value, str):
        raise EventValidationError("timestamp must be a string in HH:MM:SS(.sss) format")
    pieces = value.split(":")
    if len(pieces) != 3:
        raise EventValidationError("timestamp must be in HH:MM:SS(.sss) format")
    try:
        hours, minutes = (int(pieces[0]), int(pieces[1]))
        seconds = float(pieces[2])
    except ValueError as error:
        raise EventValidationError("timestamp must be in HH:MM:SS(.sss) format") from error
    if hours < 0 or not 0 <= minutes < 60 or not 0 <= seconds < 60:
        raise EventValidationError("timestamp contains an invalid clock value")
    return hours * 3600 + minutes * 60 + seconds


def _metadata(raw: Mapping[str, Any], event_type: str) -> dict[str, Any]:
    """Extract provider-neutral details useful to later consumers.

    The selected keys are common match context and outcome/end-point data.  The
    original event is deliberately not retained, so downstream code does not
    become coupled to StatsBomb's complete schema.
    """

    metadata: dict[str, Any] = {}
    if "duration" in raw:
        metadata["duration_seconds"] = _number(raw["duration"], "duration")
    for flag_name in ("under_pressure", "counterpress", "off_camera", "out"):
        if flag_name in raw:
            metadata[flag_name] = _boolean(raw[flag_name], flag_name)

    play_pattern = raw.get("play_pattern")
    if play_pattern is not None:
        metadata["play_pattern"] = _entity_metadata(play_pattern, "play_pattern")

    details = raw.get(_detail_key(event_type))
    if isinstance(details, Mapping):
        for raw_name, canonical_name in (
            ("outcome", "outcome"),
            ("technique", "technique"),
            ("body_part", "body_part"),
            ("recipient", "recipient"),
            ("replacement", "replacement"),
        ):
            if raw_name in details and details[raw_name] is not None:
                metadata[canonical_name] = _entity_metadata(details[raw_name], raw_name)
        if "end_location" in details and details["end_location"] is not None:
            metadata["end_location"] = list(_location(details["end_location"]) or ())
        if "statsbomb_xg" in details:
            xg = details["statsbomb_xg"]
            metadata["expected_goals"] = _number(xg, "shot.statsbomb_xg")
    return metadata


def _entity_metadata(value: object, field_name: str) -> dict[str, str]:
    entity = _entity(value, field_name)
    if entity is None:  # Defensive: callers filter None, preserving a clear type for mypy.
        raise EventValidationError(f"{field_name} must be an object")
    return {"id": entity.id, "name": entity.name}


def _detail_key(event_type: str) -> str:
    return event_type.lower().replace(" ", "_").replace("*", "")


def _number(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value):
        raise EventValidationError(f"{field_name} must be a finite number")
    return float(value)


def _boolean(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise EventValidationError(f"{field_name} must be a boolean")
    return value
