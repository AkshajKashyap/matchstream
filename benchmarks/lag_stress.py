"""Create a bounded broker burst, observe actual lag, then durably drain it."""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from uuid import uuid4

from benchmarks.common import environment, write_json
from matchstream.models import CanonicalEvent, EntityReference
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
    parser.add_argument("--events", type=int, default=100)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.events < 2:
        parser.error("events must be at least 2")
    result = _run(arguments.events)
    write_json(arguments.output, result)
    print(f"wrote {arguments.output}")


def _run(event_count: int) -> dict[str, object]:
    suffix = uuid4().hex
    config = BrokerConfig(
        bootstrap_servers=os.environ.get("MATCHSTREAM_BROKER_ADDRESS", "localhost:19092"),
        topic=f"matchstream.benchmark.lag.{suffix}",
        group_id=f"matchstream-benchmark-lag-{suffix}",
    )
    match_id = f"benchmark-lag-{suffix}"
    events = _events(match_id, event_count)
    with RedpandaEventProducer(config) as producer:
        for event in events:
            producer.publish(event)
    peak_rows = inspect_consumer_lag(config)
    peak_lag = sum(row.lag for row in peak_rows)
    repository = PostgresProjectionRepository(DatabaseConfig.from_environment())
    repository.initialize_schema()
    projector = DurableMatchProjector(repository)
    processed = 0
    started = time.perf_counter()
    deadline = time.monotonic() + max(20, event_count / 2)
    with RedpandaEventConsumer(config) as consumer:
        while processed < event_count and time.monotonic() < deadline:
            result = project_next(consumer, projector, 0.5)
            if result is not None:
                processed += 1
    recovery_seconds = time.perf_counter() - started
    final_rows = inspect_consumer_lag(config)
    if processed != event_count:
        raise RuntimeError(f"processed {processed} of {event_count} burst events before the deadline")
    return {
        "kind": "consumer_lag_stress",
        "label": "local development benchmark; synthetic canonical events",
        "environment": environment(),
        "events_published": event_count,
        "events_durably_processed": processed,
        "peak_lag": peak_lag,
        "lag_after_drain": sum(row.lag for row in final_rows),
        "recovery_seconds": round(recovery_seconds, 6),
        "drain_events_per_second": round(processed / recovery_seconds, 3),
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


if __name__ == "__main__":
    main()
