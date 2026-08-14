from __future__ import annotations

import pytest

from matchstream.models import CanonicalEvent, EntityReference, EventValidationError


def test_canonical_event_accepts_stable_domain_fields() -> None:
    event = CanonicalEvent(
        event_id="provider:42:event-1",
        match_id="42",
        sequence=7,
        period=1,
        match_clock_seconds=12.5,
        event_type="Pass",
        team=EntityReference(id="10", name="Home FC"),
        location=(10.0, 20.0),
        possession_id="3",
        source="provider",
        source_event_id="event-1",
    )

    assert event.location == (10.0, 20.0)
    assert event.match_clock_seconds == 12.5


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("sequence", -1, "sequence"),
        ("period", 0, "period"),
        ("match_clock_seconds", float("nan"), "match_clock_seconds"),
        ("location", (1.0,), "location"),
    ],
)
def test_canonical_event_rejects_invalid_core_values(
    field: str, value: object, message: str
) -> None:
    values: dict[str, object] = {
        "event_id": "source:1",
        "match_id": "match-1",
        "sequence": 1,
        "period": 1,
        "match_clock_seconds": 1.0,
        "event_type": "Pass",
        "team": None,
        "source_event_id": "1",
    }
    values[field] = value

    with pytest.raises(EventValidationError, match=message):
        CanonicalEvent(**values)  # type: ignore[arg-type]
