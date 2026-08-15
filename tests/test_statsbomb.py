from __future__ import annotations

import json
from pathlib import Path

import pytest

from matchstream.ingestion import (
    StatsBombIngestionError,
    convert_statsbomb_event,
    convert_statsbomb_events,
    load_statsbomb_events,
)

FIXTURE = Path(__file__).parent / "fixtures" / "statsbomb_events.json"


def test_load_converts_selected_fields_and_stably_orders_events() -> None:
    events = load_statsbomb_events(FIXTURE, match_id=123)

    assert [event.source_event_id for event in events] == ["event-1", "event-2", "event-3", "event-4"]
    pass_event = events[2]
    assert pass_event.event_id == "statsbomb:123:event-3"
    assert pass_event.match_clock_seconds == 5.0
    assert pass_event.team is not None and pass_event.team.name == "Home FC"
    assert pass_event.player is not None and pass_event.player.id == "101"
    assert pass_event.location == (20.0, 30.0)
    assert pass_event.possession_id == "8"
    assert pass_event.metadata == {
        "duration_seconds": 1.2,
        "under_pressure": True,
        "recipient": {"id": "102", "name": "Teammate"},
        "end_location": [42.0, 38.0],
        "body_part": {"id": "40", "name": "Right Foot"},
    }


def test_optional_statsbomb_fields_are_safe_when_absent() -> None:
    raw_event = json.loads(FIXTURE.read_text(encoding="utf-8"))[1]

    event = convert_statsbomb_event(raw_event, "demo")

    assert event.player is None
    assert event.location is None
    assert event.possession_id is None
    assert event.metadata == {}


def test_shot_goal_outcome_is_preserved_in_canonical_metadata() -> None:
    raw_event = {
        "id": "goal-event",
        "index": 5,
        "period": 1,
        "timestamp": "00:01:30.000",
        "type": {"id": 16, "name": "Shot"},
        "team": {"id": 10, "name": "Home FC"},
        "shot": {"outcome": {"id": 97, "name": "Goal"}},
    }

    event = convert_statsbomb_event(raw_event, "demo")

    assert event.metadata["outcome"] == {"id": "97", "name": "Goal"}


@pytest.mark.parametrize(
    ("raw_events", "message"),
    [
        ({}, "must be an array"),
        ([{"id": "missing-index"}], "missing required field 'index'"),
        (
            [
                {
                    "id": "bad-clock",
                    "index": 1,
                    "period": 1,
                    "timestamp": "nonsense",
                    "type": {"name": "Pass"},
                }
            ],
            "timestamp",
        ),
    ],
)
def test_malformed_statsbomb_input_has_useful_errors(raw_events: object, message: str) -> None:
    with pytest.raises(StatsBombIngestionError, match=message):
        convert_statsbomb_events(raw_events, "demo")


def test_missing_input_file_has_useful_error(tmp_path: Path) -> None:
    with pytest.raises(StatsBombIngestionError, match="does not exist"):
        load_statsbomb_events(tmp_path / "not-there.json", "demo")


def test_non_contiguous_source_indexes_are_rejected_for_strict_projection_ordering() -> None:
    raw_events = json.loads(FIXTURE.read_text(encoding="utf-8"))

    with pytest.raises(StatsBombIngestionError, match="indexes must be contiguous"):
        convert_statsbomb_events([raw_events[1], raw_events[0]], "demo")
