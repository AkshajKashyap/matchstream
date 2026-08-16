"""Prometheus metrics emitted at transport and durability boundaries."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram


class MatchStreamMetrics:
    """Bounded-cardinality metrics for one MatchStream process.

    Event IDs, match IDs, offsets, topics, and error messages intentionally stay in
    logs. Only finite operational dimensions are labels here.
    """

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self.registry = registry
        self.events_received = Counter(
            "matchstream_events_received",
            "Validated canonical events received by a component.",
            ["component"],
            registry=registry,
        )
        self.events_processed = Counter(
            "matchstream_events_processed",
            "Events whose projection completed, including durable duplicates.",
            ["result"],
            registry=registry,
        )
        self.duplicate_events = Counter(
            "matchstream_duplicate_events",
            "Durable duplicate event deliveries.",
            registry=registry,
        )
        self.processing_failures = Counter(
            "matchstream_processing_failures",
            "Processing failures by bounded failure class.",
            ["component", "failure_class"],
            registry=registry,
        )
        self.retries = Counter(
            "matchstream_retries",
            "In-process retry attempts by bounded failure class.",
            ["component", "failure_class"],
            registry=registry,
        )
        self.dlq_published = Counter(
            "matchstream_dlq_published",
            "Confirmed dead-letter publications.",
            registry=registry,
        )
        self.dlq_publish_failures = Counter(
            "matchstream_dlq_publish_failures",
            "Failed dead-letter publication attempts.",
            registry=registry,
        )
        self.offsets_committed = Counter(
            "matchstream_offsets_committed",
            "Successfully committed source offsets.",
            ["result"],
            registry=registry,
        )
        self.processing_duration = Histogram(
            "matchstream_event_processing_duration_seconds",
            "Wall-clock duration for a processing attempt sequence.",
            ["result"],
            registry=registry,
        )
        self.projection_duration = Histogram(
            "matchstream_projection_duration_seconds",
            "Wall-clock duration spent projecting an event.",
            registry=registry,
        )
        self.state_transition_duration = Histogram(
            "matchstream_state_transition_duration_seconds",
            "Wall-clock duration of the pure match-state transition.",
            registry=registry,
        )
        self.postgres_duration = Histogram(
            "matchstream_postgres_transaction_duration_seconds",
            "Wall-clock duration of PostgreSQL projection transactions.",
            registry=registry,
        )
        self.dlq_publish_duration = Histogram(
            "matchstream_dlq_publish_duration_seconds",
            "Wall-clock duration of dead-letter publication.",
            registry=registry,
        )
        self.kafka_commit_duration = Histogram(
            "matchstream_kafka_commit_duration_seconds",
            "Wall-clock duration of synchronous source offset commits.",
            registry=registry,
        )
        self.consumer_lag = Gauge(
            "matchstream_consumer_lag",
            "Aggregate committed consumer lag from the latest explicit inspection.",
            registry=registry,
        )

    @contextmanager
    def timer(self, histogram: Histogram, **labels: str) -> Iterator[None]:
        """Observe real elapsed time around a boundary operation."""

        started = time.monotonic()
        try:
            yield
        finally:
            histogram.labels(**labels).observe(time.monotonic() - started) if labels else histogram.observe(
                time.monotonic() - started
            )


_default_metrics: MatchStreamMetrics | None = None


def metrics() -> MatchStreamMetrics:
    """Return process-global metrics registered with Prometheus's default registry."""

    global _default_metrics
    if _default_metrics is None:
        _default_metrics = MatchStreamMetrics()
    return _default_metrics
