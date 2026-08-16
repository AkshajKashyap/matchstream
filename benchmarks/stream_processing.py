"""Measure actual Redpanda -> durable projector -> PostgreSQL local throughput."""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from uuid import uuid4

from prometheus_client import CollectorRegistry

from benchmarks.common import environment, latency_summary, write_json
from matchstream.models import CanonicalEvent, EntityReference
from matchstream.observability.metrics import MatchStreamMetrics
from matchstream.state import DurableMatchProjector
from matchstream.storage import DatabaseConfig, PostgresProjectionRepository
from matchstream.streaming.lag import inspect_consumer_lag
from matchstream.streaming.processing import project_next
from matchstream.streaming.redpanda import (
    BrokerConfig,
    RedpandaEventConsumer,
    RedpandaEventProducer,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=100, help="synthetic events per measured trial")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--warmup-events", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.events < 2 or arguments.trials < 1 or arguments.warmup_events < 2:
        parser.error("events, trials, and warmup-events must be at least 2")

    results = {
        "kind": "stream_processing",
        "label": "local development benchmark; synthetic canonical events",
        "method": "publish each event, then durably project and synchronously commit its source offset",
        "environment": environment(),
        "warmup": _run_trial(arguments.warmup_events, "warmup"),
        "trials": [_run_trial(arguments.events, f"trial-{index + 1}") for index in range(arguments.trials)],
    }
    write_json(arguments.output, results)
    print(f"wrote {arguments.output}")


def _run_trial(event_count: int, label: str) -> dict[str, object]:
    suffix = uuid4().hex
    topic = f"matchstream.benchmark.stream.{suffix}"
    group_id = f"matchstream-benchmark-stream-{suffix}"
    match_id = f"benchmark-stream-{suffix}"
    config = BrokerConfig(
        bootstrap_servers=os.environ.get("MATCHSTREAM_BROKER_ADDRESS", "localhost:19092"),
        topic=topic,
        group_id=group_id,
    )
    registry = CollectorRegistry()
    telemetry = MatchStreamMetrics(registry)
    repository = PostgresProjectionRepository(DatabaseConfig.from_environment(), observer=telemetry)
    repository.initialize_schema()
    events = _events(match_id, event_count)
    published_at: dict[str, float] = {}
    with RedpandaEventProducer(config, observer=telemetry) as producer:
        started = time.perf_counter()
        for event in events:
            producer.publish(event)
            published_at[event.event_id] = time.perf_counter()
        publish_elapsed = time.perf_counter() - started

    latencies_ms: list[float] = []
    processed = 0
    deadline = time.monotonic() + max(20, event_count / 2)
    projector = DurableMatchProjector(repository)
    drain_started = time.perf_counter()
    with RedpandaEventConsumer(config, observer=telemetry) as consumer:
        while processed < event_count and time.monotonic() < deadline:
            result = project_next(consumer, projector, 0.5, observer=telemetry)
            if result is None:
                continue
            processed += 1
            latencies_ms.append((time.perf_counter() - published_at[result.event.event_id]) * 1000)
    drain_elapsed = time.perf_counter() - drain_started
    lag_rows = inspect_consumer_lag(config, observer=telemetry)
    if processed != event_count:
        raise RuntimeError(f"{label}: processed {processed} of {event_count} events before the deadline")
    return {
        "label": label,
        "events_published": event_count,
        "events_durably_processed": processed,
        "publish_elapsed_seconds": round(publish_elapsed, 6),
        "drain_elapsed_seconds": round(drain_elapsed, 6),
        "observed_events_per_second": round(processed / drain_elapsed, 3),
        "publish_to_durable_latency": latency_summary(latencies_ms),
        "lag_after_drain": sum(row.lag for row in lag_rows),
        "metric_histograms": _histograms(registry),
    }


def _events(match_id: str, count: int) -> list[CanonicalEvent]:
    team = EntityReference(id="benchmark-home", name="Benchmark Home")
    return [
        CanonicalEvent(
            event_id=f"benchmark:{match_id}:{sequence}",
            match_id=match_id,
            sequence=sequence,
            period=1,
            match_clock_seconds=float(sequence),
            event_type="Starting XI" if sequence == 1 else "Pass",
            team=team,
            source="benchmark-synthetic",
            source_event_id=str(sequence),
        )
        for sequence in range(1, count + 1)
    ]


def _histograms(registry: CollectorRegistry) -> dict[str, dict[str, float]]:
    names = {
        "matchstream_event_processing_duration_seconds",
        "matchstream_postgres_transaction_duration_seconds",
    }
    summaries: dict[str, dict[str, float]] = {}
    for metric in registry.collect():
        if metric.name not in names:
            continue
        samples = {sample.name: sample.value for sample in metric.samples}
        count = samples.get(f"{metric.name}_count", 0.0)
        total = samples.get(f"{metric.name}_sum", 0.0)
        summaries[metric.name] = {
            "count": count,
            "sum_seconds": round(total, 6),
            "mean_ms": round(total * 1000 / count, 3) if count else 0.0,
        }
    return summaries


if __name__ == "__main__":
    main()
