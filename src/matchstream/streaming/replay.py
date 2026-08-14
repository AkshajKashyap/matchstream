"""Broker-independent connection between replay and a publisher."""

from __future__ import annotations

import time
from collections.abc import Callable

from matchstream.replay import MatchReplayer, ReplayConfig

from .interfaces import EventPublisher


def publish_replay(
    replayer: MatchReplayer,
    publisher: EventPublisher,
    config: ReplayConfig | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Publish one deterministic replay through any event-publisher adapter."""

    for event in replayer.iter_events(config=config, sleep=sleep):
        publisher.publish(event)
