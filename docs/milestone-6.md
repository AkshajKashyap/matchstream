# Milestone 6: Observability, health, metrics, and diagnostics

Milestone 6 makes the existing event-processing path understandable while it
runs. It does not change canonical events, match-state transition rules,
PostgreSQL idempotency, or DLQ ordering.

## Terms

**Observability** is the ability to understand runtime behavior from externally
visible signals. **Logs** are discrete records explaining what happened.
**Metrics** are numeric measurements aggregated over time. A **counter** only
increases, a **gauge** represents a current value, and a **histogram** records a
distribution such as latency. **Consumer lag** is the difference between a
Kafka partition's end offset and a consumer group's progress.

**Liveness** answers whether the local process is running. **Readiness** answers
whether it can currently perform its intended work against its dependencies.

## Structured logs

Configure machine-readable standard-library logging with:

    MATCHSTREAM_LOG_LEVEL=INFO python -m matchstream.streaming.cli resilient-project ...

Logs are JSON lines. Relevant calls include consumer startup/shutdown, event
receipt, durable success or duplicate, retry, poison classification, DLQ
publication or failure, source offset commit, projection failure, and database
health failure. Event ID, match ID, sequence, topic/partition/offset, group,
attempt, classification, and duration are included only when useful. Full event
payloads, database URLs, and credentials are never logged.

## Metrics and cardinality

The Prometheus endpoint exposes these primary counters:

- `matchstream_events_received_total`
- `matchstream_events_processed_total`
- `matchstream_duplicate_events_total`
- `matchstream_processing_failures_total`
- `matchstream_retries_total`
- `matchstream_dlq_published_total`
- `matchstream_dlq_publish_failures_total`
- `matchstream_offsets_committed_total`

Histograms measure event processing, projection, pure state transition,
PostgreSQL transaction, DLQ publication, and Kafka commit durations. The
aggregate lag from the latest explicit lag inspection is exposed as a gauge.

Metric labels are finite operational dimensions only: `component`, `result`,
and `failure_class`. **Cardinality** is the number of unique label-value
combinations; event IDs, match IDs, offsets, error messages, and topics are not
metric labels because they can grow without bound. They belong in structured
logs and CLI output instead.

## HTTP endpoints and health

Start the endpoint locally:

    make observability-serve

It serves:

- `GET /health/live` — always `200` while this endpoint process runs.
- `GET /health/ready` — `200` only when PostgreSQL accepts `SELECT 1` and
  Redpanda returns Kafka metadata containing the configured required topic;
  otherwise `503` with safe dependency categories.
- `GET /metrics` — Prometheus text exposition.

PostgreSQL readiness is a lightweight connectivity query, not a schema scan.
Redpanda readiness proves Kafka-client metadata communication and required-topic
availability; it is stronger than TCP reachability but does not prove that a
particular consumer can process every future record.

`resilient-project` can expose its own process metrics while it runs:

    python -m matchstream.streaming.cli resilient-project ... --metrics-port 9464

## CLI diagnostics

    python -m matchstream.streaming.cli health
    python -m matchstream.streaming.cli lag --topic matchstream.events.v1 --group-id matchstream-demo

The lag command queries committed group offsets and broker watermarks per
partition. It also reports an active consumer position when Kafka can provide
one. A one-shot inspection does not poll, consume, or commit records. For an
idle external group its current position can be unavailable; it is shown as
`null`, never invented from another value.

## Prometheus

Start the local scraper after Docker dependencies are running:

    docker compose up -d
    make observability-up

`infra/prometheus/prometheus.yml` scrapes `host.docker.internal:9464`, so run
the MatchStream observability endpoint or resilient projector on the host at
that port first. Prometheus is available at `http://localhost:9090`. No Grafana
is provisioned: a dashboard would add an interface without adding a new signal.

## Processing behavior and limits

Metrics and logs surround transport, processing, and storage boundaries. They
do not create remote calls per event or alter acknowledgements: normal success,
duplicate success, poison-to-DLQ, and DLQ-publication-failure retain their
Milestone 5 source-commit semantics. Timers report measured elapsed time only;
no performance values are fabricated.

OpenTelemetry is intentionally deferred. Prometheus metrics plus structured
event correlation already answer the operational questions in this single
process/local stack; distributed traces would add exporter and context
propagation complexity without a current multi-service request path.

Suggested future alert conditions are sustained lag, any DLQ rate, rising
processing failures, and readiness failures. This milestone does not add an
alerting stack.
