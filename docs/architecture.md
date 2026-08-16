# MatchStream Architecture

## Initial Target

Historical football event data is replayed as a live event stream.

Planned flow:

1. Dataset ingestion
2. Match replay producer
3. Event broker
4. Stateful consumers
5. Analytics and prediction
6. Redis/PostgreSQL persistence
7. FastAPI/WebSocket serving
8. Live dashboard
9. Observability
10. Load and failure testing

## Current Milestone 4 boundary

```text
StatsBomb adapter -> CanonicalEvent -> deterministic replay -> versioned JSON
                                                        -> Kafka/Redpanda adapter
                                                        -> validated CanonicalEvent
                                                        -> PostgreSQL transaction
                                                           -> event identity
                                                           -> MatchState snapshot
                                                        -> commit offset on success
```

StatsBomb parsing and replay remain independent of the wire contract and broker
adapter. Events for the same match use `match_id` as their broker key so they
share a partition; this gives no ordering guarantee across different matches.
Projection state is process-local and is derived through pure state transitions.
Milestone 4 also supports a durable projector: state and processed identity are
committed atomically in PostgreSQL before an offset acknowledgement.

## Current Milestone 5 recovery boundary

projection failure -> bounded retry -> poison classification -> DLQ publish
-> source offset commit

durable canonical history -> offline rebuild -> replacement MatchState snapshot

## Current Milestone 6 operational boundary

```text
consumer -> structured event log -> processing/retry/DLQ metrics -> /metrics
       \-> PostgreSQL and Redpanda readiness checks -------------> /health/ready
       \-> committed offsets + broker watermarks ----------------> lag CLI
```

Instrumentation surrounds adapter boundaries only. The pure match-state
transition remains independent of logging, Prometheus, PostgreSQL, and Kafka.

## Current Milestone 7 application boundary

```text
Kafka consumer -> PostgreSQL durable projection --COMMIT--> pg_notify(match_id)
                                                         -> API LISTEN wake-up
                                                         -> read committed snapshot
                                                         -> match-scoped WebSocket clients

HTTP API -------------------------------------------------> read committed snapshot/history
```

The API owns presentation and best-effort live delivery, never football-state
correctness or Kafka consumer ownership. PostgreSQL remains authoritative; a
notification only causes the API to reread durable state before broadcasting.

## Current Milestone 8 dashboard boundary

```text
                     HTTP: match list, durable snapshot, event history
PostgreSQL <- FastAPI ------------------------------------------------> React/Vite dashboard
     ^               |                                                       |
     |               | WebSocket: snapshot, best-effort state_update         | renders score,
consumer commit      +-------------------------------------------------------+ timeline, status

```

The dashboard never connects to PostgreSQL, Kafka, or the replay producer.
It first obtains authoritative data over HTTP, then treats the WebSocket as a
notification channel. A snapshot replaces local match state after (re)connect;
state-update sequence gaps trigger an HTTP reconciliation and event-history
refresh. The timeline is bounded to recent events and does not claim analytics
the API does not provide.
