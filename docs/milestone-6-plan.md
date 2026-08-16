# Milestone 6 implementation plan

## Failure modes and signals

| Failure mode | Signal | Emission boundary |
| --- | --- | --- |
| PostgreSQL unavailable or transaction failure | readiness dependency failure, structured error, processing-failure counter, database-duration histogram | repository and resilient processing boundary |
| Redpanda unavailable or consumer failure | readiness dependency failure, structured error, processing-failure counter | broker adapters and health check |
| Slow processing | processing, transition, database, DLQ publish, and offset-commit histograms | processing/repository/transport boundaries |
| Retried processing | retry log and retry counter by failure class | retry helper |
| Poison event | poison log, processing-failure counter, DLQ-published counter | resilient processing boundary |
| DLQ publish failure | error log and distinct DLQ-publish-failure counter; source remains uncommitted | resilient processing boundary |
| Consumer falling behind | per-partition CLI lag report and bounded aggregate lag gauge | Kafka lag diagnostic |

## Logging and metrics design

Use standard-library logging with a JSON formatter and explicit contextual
fields. Event and match identity belongs in logs only; complete canonical event
payloads and secrets are never logged. The log level is configurable through
`MATCHSTREAM_LOG_LEVEL`.

Prometheus metrics use a small adapter shared by operational boundaries. The
pure transition remains dependency-free. Labels are deliberately bounded:
`component`, `result`, and `failure_class` are allowed. Event IDs, match IDs,
offsets, error messages, and individual topics are never metric labels.

## Readiness and scope

Liveness reports that the local HTTP process is running. Readiness performs a
small `SELECT 1` against PostgreSQL and Kafka metadata lookup through the
client; it reports only dependency categories, not credentials. The endpoint
does not perform schema scans or broker calls per processed event.

This milestone adds a metrics endpoint, health checks, CLI diagnostics,
Prometheus configuration, and focused test coverage. It does not add FastAPI,
Grafana, OpenTelemetry, alerting, distributed tracing, dashboard UI, or any
new business/analytics behavior.

## Consumer lag

The diagnostic queries group committed offsets and broker watermarks. It also
reports a consumer's current position when available. The aggregate lag gauge
uses no per-topic/match labels; the detailed partition report remains a CLI
operation to avoid accidental metric cardinality growth.
