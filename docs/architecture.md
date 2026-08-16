# MatchStream architecture

## Implemented event path

```text
StatsBomb JSON -> ingestion adapter -> CanonicalEvent -> deterministic replay
                                                       -> versioned Kafka record
                                                       -> Redpanda partition(match_id)
                                                       -> durable projector
                                                          -> PostgreSQL transaction
                                                             processed event identity
                                                             canonical event history
                                                             MatchState snapshot
                                                             pg_notify(match_id)
                                                       -> commit Kafka offset

PostgreSQL -> LISTEN/NOTIFY -> FastAPI -> HTTP snapshot/history
                                      -> best-effort match-scoped WebSocket -> React dashboard
```

The StatsBomb adapter is the only component that knows provider JSON. Canonical
events and wire records are independently versioned boundaries. `match_id` is
the Kafka key, so events for one match share a partition and retain partition
order; different matches have no cross-match order guarantee.

## Durability and failure path

```text
validated event -> transaction succeeds -> offset commit
                -> deterministic transition fails -> bounded retry
                                                   -> confirmed DLQ publish -> offset commit
                -> database/broker/DLQ failure -> no offset commit -> redelivery
```

The PostgreSQL transaction inserts event identity, appends canonical history,
updates the projection snapshot, and emits `pg_notify`. A duplicate identity is
a successful no-op. There is no distributed Kafka/PostgreSQL transaction, so a
crash after database commit and before offset commit causes redelivery; durable
identity prevents duplicate state application.

## Read and live-delivery model

PostgreSQL is authoritative. FastAPI has no Kafka-consumer ownership and never
derives football state. HTTP returns committed snapshots/history. Notifications
make the API reread committed state before broadcasting a `state_update`.
WebSockets are low-latency, best-effort delivery: clients receive a snapshot on
connect/reconnect and use `latest_sequence` plus HTTP history to recover from
stale or gapped updates.

## Observability and operations

Structured JSON logs correlate event identity without exposing payloads or
credentials. Prometheus metrics surround processing, PostgreSQL transactions,
offset commits, retry/DLQ behavior, API requests, and WebSocket delivery.
Readiness checks PostgreSQL and Redpanda; the lag command reads actual broker
watermarks and committed offsets. Canonical history supports explicit,
maintenance-mode deterministic rebuilds.

See [system design](system-design.md) for decisions and trade-offs and
[limitations](limitations.md) for the current operating boundary.
