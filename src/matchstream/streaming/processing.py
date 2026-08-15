"""Consumer processing boundary for stateful match projection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .interfaces import AcknowledgingEventConsumer

if TYPE_CHECKING:
    from matchstream.models import CanonicalEvent
    from matchstream.state import MatchProjector, ProjectionUpdate


@dataclass(frozen=True, slots=True)
class ProcessedEvent:
    """A consumer event whose projection has completed and offset was committed."""

    event: CanonicalEvent
    update: ProjectionUpdate


def project_next(
    consumer: AcknowledgingEventConsumer,
    projector: MatchProjector,
    timeout_seconds: float,
) -> ProcessedEvent | None:
    """Project one event, then commit only if projection succeeded.

    Projection exceptions deliberately propagate before ``commit`` is reached.
    A duplicate is a successful no-op, so its offset is eligible for commit.
    """

    event = consumer.poll(timeout_seconds)
    if event is None:
        return None
    update = projector.process(event)
    consumer.commit()
    return ProcessedEvent(event=event, update=update)
