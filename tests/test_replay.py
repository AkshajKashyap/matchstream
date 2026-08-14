from __future__ import annotations

from matchstream.models import CanonicalEvent
from matchstream.replay import MatchReplayer, ReplayConfig


def _event(event_id: str, sequence: int, period: int, clock: float) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=event_id,
        match_id="demo",
        sequence=sequence,
        period=period,
        match_clock_seconds=clock,
        event_type="Pass",
        team=None,
        source_event_id=event_id,
    )


def test_replayer_sorts_input_and_emits_deterministically_without_waiting() -> None:
    replayer = MatchReplayer(
        [_event("third", 3, 1, 5.0), _event("first", 1, 1, 1.0), _event("second", 2, 1, 1.0)]
    )
    sleeps: list[float] = []

    first_run = [event.event_id for event in replayer.iter_events(ReplayConfig(no_wait=True), sleeps.append)]
    second_run = [event.event_id for event in replayer.iter_events(ReplayConfig(no_wait=True), sleeps.append)]

    assert first_run == ["first", "second", "third"]
    assert second_run == first_run
    assert sleeps == []


def test_replayer_scales_delays_and_does_not_wait_across_periods() -> None:
    replayer = MatchReplayer(
        [_event("late", 2, 1, 5.0), _event("new-half", 3, 2, 0.0), _event("early", 1, 1, 1.0)]
    )
    sleeps: list[float] = []
    received: list[str] = []

    replayer.replay(
        lambda event: received.append(event.event_id),
        config=ReplayConfig(speed=2.0),
        sleep=sleeps.append,
    )

    assert received == ["early", "late", "new-half"]
    assert sleeps == [2.0]


def test_replay_config_rejects_non_positive_speed() -> None:
    try:
        ReplayConfig(speed=0)
    except ValueError as error:
        assert "greater than zero" in str(error)
    else:
        raise AssertionError("expected replay speed validation to fail")
