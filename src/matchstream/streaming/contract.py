"""Versioned JSON wire contract for canonical MatchStream events."""

from __future__ import annotations

import json
from collections.abc import Mapping
from math import isfinite
from typing import Any

from matchstream.models import CanonicalEvent, EntityReference, EventValidationError, JSONValue

WIRE_SCHEMA = "matchstream.canonical-event"
WIRE_SCHEMA_VERSION = 1


class EventContractError(ValueError):
    """Raised when a wire message is not a valid MatchStream event envelope."""


class UnsupportedSchemaVersion(EventContractError):
    """Raised when an otherwise recognizable contract version is unsupported."""


def serialize_event(event: CanonicalEvent) -> bytes:
    """Serialize an event as deterministic UTF-8 JSON bytes.

    Explicit construction prevents Python implementation details, provider raw
    payloads, and unsupported metadata values from entering the wire contract.
    """

    envelope = {
        "schema": WIRE_SCHEMA,
        "schema_version": WIRE_SCHEMA_VERSION,
        "event": _event_to_wire(event),
    }
    try:
        return json.dumps(
            envelope,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise EventContractError(f"event cannot be serialized as JSON: {error}") from error


def deserialize_event(payload: bytes | str) -> CanonicalEvent:
    """Deserialize and validate a versioned wire message into a canonical event."""

    if isinstance(payload, bytes):
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise EventContractError("wire payload is not valid UTF-8") from error
    elif isinstance(payload, str):
        text = payload
    else:
        raise EventContractError("wire payload must be UTF-8 bytes or a string")

    try:
        envelope = json.loads(text)
    except json.JSONDecodeError as error:
        raise EventContractError(f"wire payload is not valid JSON: {error.msg}") from error

    root = _object(envelope, "envelope")
    if _string(root.get("schema"), "schema") != WIRE_SCHEMA:
        raise EventContractError(f"unsupported wire schema {root.get('schema')!r}")
    version = _integer(root.get("schema_version"), "schema_version")
    if version != WIRE_SCHEMA_VERSION:
        raise UnsupportedSchemaVersion(
            f"unsupported schema version {version}; supported version is {WIRE_SCHEMA_VERSION}"
        )
    return _event_from_wire(_object(root.get("event"), "event"))


def _event_to_wire(event: CanonicalEvent) -> dict[str, JSONValue]:
    return {
        "event_id": event.event_id,
        "match_id": event.match_id,
        "sequence": event.sequence,
        "period": event.period,
        "match_clock_seconds": event.match_clock_seconds,
        "event_type": event.event_type,
        "team": _entity_to_wire(event.team),
        "player": _entity_to_wire(event.player),
        "location": list(event.location) if event.location is not None else None,
        "possession_id": event.possession_id,
        "possession_team": _entity_to_wire(event.possession_team),
        "metadata": _json_value(event.metadata, "metadata"),
        "source": event.source,
        "source_event_id": event.source_event_id,
    }


def _entity_to_wire(entity: EntityReference | None) -> dict[str, str] | None:
    if entity is None:
        return None
    return {"id": entity.id, "name": entity.name}


def _event_from_wire(raw: Mapping[str, Any]) -> CanonicalEvent:
    required_fields = (
        "event_id",
        "match_id",
        "sequence",
        "period",
        "match_clock_seconds",
        "event_type",
        "team",
        "player",
        "location",
        "possession_id",
        "possession_team",
        "metadata",
        "source",
        "source_event_id",
    )
    for field_name in required_fields:
        if field_name not in raw:
            raise EventContractError(f"event is missing required field '{field_name}'")
    try:
        return CanonicalEvent(
            event_id=_string(raw["event_id"], "event.event_id"),
            match_id=_string(raw["match_id"], "event.match_id"),
            sequence=_integer(raw["sequence"], "event.sequence"),
            period=_integer(raw["period"], "event.period"),
            match_clock_seconds=_number(raw["match_clock_seconds"], "event.match_clock_seconds"),
            event_type=_string(raw["event_type"], "event.event_type"),
            team=_entity_from_wire(raw["team"], "event.team"),
            player=_entity_from_wire(raw["player"], "event.player"),
            location=_location_from_wire(raw["location"]),
            possession_id=_optional_string(raw["possession_id"], "event.possession_id"),
            possession_team=_entity_from_wire(raw["possession_team"], "event.possession_team"),
            metadata=_json_value(raw["metadata"], "event.metadata"),
            source=_string(raw["source"], "event.source"),
            source_event_id=_string(raw["source_event_id"], "event.source_event_id"),
        )
    except EventValidationError as error:
        raise EventContractError(f"invalid canonical event in wire payload: {error}") from error


def _entity_from_wire(value: object, field_name: str) -> EntityReference | None:
    if value is None:
        return None
    raw = _object(value, field_name)
    return EntityReference(
        id=_string(raw.get("id"), f"{field_name}.id"),
        name=_string(raw.get("name"), f"{field_name}.name"),
    )


def _location_from_wire(value: object) -> tuple[float, ...] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise EventContractError("event.location must be an array or null")
    return tuple(_number(coordinate, "event.location coordinate") for coordinate in value)


def _object(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EventContractError(f"{field_name} must be an object")
    return value


def _string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EventContractError(f"{field_name} must be a non-empty string")
    return value


def _optional_string(value: object, field_name: str) -> str | None:
    return None if value is None else _string(value, field_name)


def _integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EventContractError(f"{field_name} must be an integer")
    return value


def _number(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value):
        raise EventContractError(f"{field_name} must be a finite number")
    return float(value)


def _json_value(value: object, field_name: str) -> JSONValue:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not isfinite(value):
            raise EventContractError(f"{field_name} must not contain non-finite numbers")
        return value
    if isinstance(value, list):
        return [_json_value(item, field_name) for item in value]
    if isinstance(value, Mapping):
        serialized: dict[str, JSONValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise EventContractError(f"{field_name} object keys must be strings")
            serialized[key] = _json_value(item, f"{field_name}.{key}")
        return serialized
    raise EventContractError(f"{field_name} contains unsupported value {type(value).__name__}")
