"""Consumer processing boundary for stateful match projection."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .dlq import DeadLetterEvent, SourcePosition
from .interfaces import AcknowledgingEventConsumer
from .retry import ProcessingFailure, RetryPolicy, attempt_processing

if TYPE_CHECKING:
    from matchstream.models import CanonicalEvent
    from matchstream.state import MatchProjector, ProjectionUpdate


@dataclass(frozen=True, slots=True)
class ProcessedEvent:
    """A consumer event whose projection has completed and offset was committed."""

    event: CanonicalEvent
    update: ProjectionUpdate


@dataclass(frozen=True, slots=True)
class QuarantinedEvent:
    """A poison event whose DLQ delivery succeeded before source acknowledgement."""

    event: CanonicalEvent
    dead_letter: DeadLetterEvent


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


def project_next_with_dlq(
    consumer: AcknowledgingEventConsumer,
    projector: MatchProjector,
    dead_letter_publisher: object,
    retry_policy: RetryPolicy,
    timeout_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
) -> ProcessedEvent | QuarantinedEvent | None:
    """Project with bounded retries, quarantining only persistent poison events.

    The source offset is committed only after a normal durable update or a
    confirmed dead-letter publication. A DLQ publication failure propagates and
    intentionally leaves the source offset uncommitted.
    """

    event = consumer.poll(timeout_seconds)
    if event is None:
        return None
    source = getattr(consumer, "last_source_position", None)
    if not isinstance(source, SourcePosition):
        raise TypeError("consumer must expose source metadata for DLQ processing")
    result = attempt_processing(lambda: projector.process(event), retry_policy, sleep)
    if not isinstance(result, ProcessingFailure):
        consumer.commit()
        return ProcessedEvent(event=event, update=result)
    record = DeadLetterEvent.from_failure(
        event,
        source,
        result.classification,
        result.error,
        result.attempt_count,
    )
    publish = getattr(dead_letter_publisher, "publish", None)
    if not callable(publish):
        raise TypeError("dead_letter_publisher must provide publish(record)")
    publish(record)
    consumer.commit()
    return QuarantinedEvent(event=event, dead_letter=record)
