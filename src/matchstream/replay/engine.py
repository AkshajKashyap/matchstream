"""Deterministic replay of canonical football events."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass
from math import isfinite

from matchstream.models import CanonicalEvent, event_order_key


@dataclass(frozen=True, slots=True)
class ReplayConfig:
    """Timing controls for a match replay."""

    speed: float = 1.0
    no_wait: bool = False

    def __post_init__(self) -> None:
        if (
            isinstance(self.speed, bool)
            or not isinstance(self.speed, (int, float))
            or not isfinite(self.speed)
            or self.speed <= 0
        ):
            raise ValueError("replay speed must be a finite number greater than zero")


class MatchReplayer:
    """Emit canonical events in a stable order without domain analytics logic."""

    def __init__(self, events: Iterable[CanonicalEvent]) -> None:
        self._events = tuple(sorted(events, key=event_order_key))

    def iter_events(
        self,
        config: ReplayConfig | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> Iterator[CanonicalEvent]:
        """Yield events, waiting by elapsed period-clock time when enabled."""

        active_config = config or ReplayConfig()
        previous: CanonicalEvent | None = None
        for event in self._events:
            delay = _delay_seconds(previous, event, active_config)
            if delay:
                sleep(delay)
            yield event
            previous = event

    def replay(
        self,
        callback: Callable[[CanonicalEvent], None],
        config: ReplayConfig | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Push events to a callback suitable for a future broker adapter."""

        for event in self.iter_events(config=config, sleep=sleep):
            callback(event)

    async def aiter_events(
        self,
        config: ReplayConfig | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> AsyncIterator[CanonicalEvent]:
        """Asynchronously yield events for future asynchronous publishers."""

        active_config = config or ReplayConfig()
        previous: CanonicalEvent | None = None
        for event in self._events:
            delay = _delay_seconds(previous, event, active_config)
            if delay:
                await sleep(delay)
            yield event
            previous = event


def _delay_seconds(
    previous: CanonicalEvent | None, event: CanonicalEvent, config: ReplayConfig
) -> float:
    if config.no_wait or previous is None or previous.period != event.period:
        return 0.0
    return max(0.0, event.match_clock_seconds - previous.match_clock_seconds) / config.speed
