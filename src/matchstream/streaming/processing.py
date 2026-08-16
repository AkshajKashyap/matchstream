"""Consumer processing boundary for stateful match projection."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from matchstream.observability.logging import get_logger
from matchstream.observability.metrics import MatchStreamMetrics, metrics

from .dlq import DeadLetterEvent, SourcePosition
from .interfaces import AcknowledgingEventConsumer
from .retry import FailureClassification, ProcessingFailure, RetryPolicy, attempt_processing

logger = get_logger("processing")

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
    observer: MatchStreamMetrics | None = None,
) -> ProcessedEvent | None:
    """Project one event, then commit only if projection succeeded.

    Projection exceptions deliberately propagate before ``commit`` is reached.
    A duplicate is a successful no-op, so its offset is eligible for commit.
    """

    event = consumer.poll(timeout_seconds)
    if event is None:
        return None
    telemetry = observer or metrics()
    telemetry.events_received.labels(component="consumer").inc()
    started = time.monotonic()
    logger.info("event_received", extra=_event_context(event, operation="project"))
    try:
        with telemetry.timer(telemetry.projection_duration):
            update = projector.process(event)
        consumer.commit()
    except Exception:
        telemetry.processing_failures.labels(component="projection", failure_class="retriable").inc()
        telemetry.processing_duration.labels(result="failure").observe(time.monotonic() - started)
        logger.error(
            "projection_failure",
            extra={**_event_context(event, operation="project"), "failure_class": "retriable"},
        )
        raise
    telemetry.offsets_committed.labels(result="processed").inc()
    if update.applied:
        telemetry.events_processed.labels(result="success").inc()
        logger.info("durable_processing_success", extra=_event_context(event, operation="project"))
    else:
        telemetry.events_processed.labels(result="duplicate").inc()
        telemetry.duplicate_events.inc()
        logger.info("durable_duplicate", extra=_event_context(event, operation="project"))
    telemetry.processing_duration.labels(result="success" if update.applied else "duplicate").observe(
        time.monotonic() - started
    )
    logger.info("source_offset_committed", extra=_event_context(event, operation="commit"))
    return ProcessedEvent(event=event, update=update)


def project_next_with_dlq(
    consumer: AcknowledgingEventConsumer,
    projector: MatchProjector,
    dead_letter_publisher: object,
    retry_policy: RetryPolicy,
    timeout_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
    observer: MatchStreamMetrics | None = None,
) -> ProcessedEvent | QuarantinedEvent | None:
    """Project with bounded retries, quarantining only persistent poison events.

    The source offset is committed only after a normal durable update or a
    confirmed dead-letter publication. A DLQ publication failure propagates and
    intentionally leaves the source offset uncommitted.
    """

    event = consumer.poll(timeout_seconds)
    if event is None:
        return None
    telemetry = observer or metrics()
    telemetry.events_received.labels(component="consumer").inc()
    started = time.monotonic()
    logger.info("event_received", extra=_event_context(event, operation="resilient_project"))
    source = getattr(consumer, "last_source_position", None)
    if not isinstance(source, SourcePosition):
        raise TypeError("consumer must expose source metadata for DLQ processing")
    def on_retry(
        error: Exception, classification: FailureClassification, attempt: int, delay: float
    ) -> None:
        telemetry.retries.labels(component="projection", failure_class=classification.value).inc()
        logger.warning(
            "processing_retry",
            extra={
                **_event_context(event, operation="resilient_project"),
                "attempt": attempt,
                "failure_class": classification.value,
                "duration_ms": round(delay * 1000, 3),
            },
        )

    try:
        with telemetry.timer(telemetry.projection_duration):
            result = attempt_processing(lambda: projector.process(event), retry_policy, sleep, on_retry)
    except Exception:
        telemetry.processing_failures.labels(component="projection", failure_class="retriable").inc()
        telemetry.processing_duration.labels(result="failure").observe(time.monotonic() - started)
        logger.error(
            "projection_failure",
            extra={**_event_context(event, operation="resilient_project"), "failure_class": "retriable"},
        )
        raise
    if not isinstance(result, ProcessingFailure):
        consumer.commit()
        telemetry.offsets_committed.labels(result="processed").inc()
        if result.applied:
            telemetry.events_processed.labels(result="success").inc()
            logger.info("durable_processing_success", extra=_event_context(event, operation="resilient_project"))
        else:
            telemetry.events_processed.labels(result="duplicate").inc()
            telemetry.duplicate_events.inc()
            logger.info("durable_duplicate", extra=_event_context(event, operation="resilient_project"))
        telemetry.processing_duration.labels(result="success" if result.applied else "duplicate").observe(
            time.monotonic() - started
        )
        logger.info("source_offset_committed", extra=_event_context(event, operation="commit"))
        return ProcessedEvent(event=event, update=result)
    telemetry.processing_failures.labels(component="projection", failure_class=result.classification.value).inc()
    logger.warning(
        "poison_classified",
        extra={
            **_event_context(event, operation="resilient_project"),
            "attempt": result.attempt_count,
            "failure_class": result.classification.value,
        },
    )
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
    try:
        with telemetry.timer(telemetry.dlq_publish_duration):
            publish(record)
    except Exception:
        telemetry.dlq_publish_failures.inc()
        telemetry.processing_duration.labels(result="failure").observe(time.monotonic() - started)
        logger.error("dlq_publication_failure", extra=_event_context(event, operation="dlq_publish"))
        raise
    telemetry.dlq_published.inc()
    logger.info("dlq_published", extra=_event_context(event, operation="dlq_publish"))
    consumer.commit()
    telemetry.offsets_committed.labels(result="quarantined").inc()
    telemetry.processing_duration.labels(result="quarantined").observe(time.monotonic() - started)
    logger.info("source_offset_committed", extra=_event_context(event, operation="commit"))
    return QuarantinedEvent(event=event, dead_letter=record)


def _event_context(event: CanonicalEvent, operation: str) -> dict[str, object]:
    """Return useful correlation fields without logging the full event payload."""

    return {
        "component": "processing",
        "operation": operation,
        "event_id": event.event_id,
        "match_id": event.match_id,
        "event_sequence": event.sequence,
    }
