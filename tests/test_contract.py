from __future__ import annotations

import json

import pytest

from matchstream.models import CanonicalEvent, EntityReference
from matchstream.streaming import (
    EventContractError,
    UnsupportedSchemaVersion,
    deserialize_event,
    serialize_event,
)


def _event(metadata: object | None = None) -> CanonicalEvent:
    return CanonicalEvent(
        event_id="statsbomb:match-1:event-1",
        match_id="match-1",
        sequence=8,
        period=1,
        match_clock_seconds=12.75,
        event_type="Pass",
        team=EntityReference(id="home", name="Home FC"),
        player=EntityReference(id="player-1", name="Player One"),
        location=(10.0, 20.0, 1.0),
        possession_id="3",
        possession_team=EntityReference(id="home", name="Home FC"),
        metadata={"outcome": {"name": "Complete"}, "end_location": [30.0, 40.0]}
        if metadata is None
        else metadata,  # type: ignore[arg-type]
        source="statsbomb",
        source_event_id="event-1",
    )


def test_wire_contract_round_trips_all_canonical_fields_and_identity() -> None:
    event = _event()
    payload = serialize_event(event)

    decoded = deserialize_event(payload)

    assert decoded == event
    assert deserialize_event(payload.decode("utf-8")) == event
    assert decoded.event_id == "statsbomb:match-1:event-1"


def test_wire_serialization_is_deterministic() -> None:
    first = _event(metadata={"z": 1, "a": {"second": 2, "first": 1}})
    second = _event(metadata={"a": {"first": 1, "second": 2}, "z": 1})

    assert serialize_event(first) == serialize_event(second)


def test_unsupported_schema_version_fails_clearly() -> None:
    payload = json.loads(serialize_event(_event()))
    payload["schema_version"] = 2

    with pytest.raises(UnsupportedSchemaVersion, match="unsupported schema version 2"):
        deserialize_event(json.dumps(payload))


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"not-json", "not valid JSON"),
        (b'{"schema":"matchstream.canonical-event","schema_version":1}', "event must be an object"),
        (
            b'{"schema":"matchstream.canonical-event","schema_version":1,"event":{}}',
            "missing required field 'event_id'",
        ),
        (b'{"schema":"other","schema_version":1,"event":{}}', "unsupported wire schema"),
    ],
)
def test_malformed_wire_messages_fail_clearly(payload: bytes, message: str) -> None:
    with pytest.raises(EventContractError, match=message):
        deserialize_event(payload)


def test_contract_rejects_arbitrary_python_metadata() -> None:
    with pytest.raises(EventContractError, match="unsupported value object"):
        serialize_event(_event(metadata={"bad": object()}))
