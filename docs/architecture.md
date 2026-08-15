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

## Current Milestone 3 boundary

```text
StatsBomb adapter -> CanonicalEvent -> deterministic replay -> versioned JSON
                                                        -> Kafka/Redpanda adapter
                                                        -> validated CanonicalEvent
                                                        -> duplicate check
                                                        -> MatchState projection
                                                        -> commit offset on success
```

StatsBomb parsing and replay remain independent of the wire contract and broker
adapter. Events for the same match use `match_id` as their broker key so they
share a partition; this gives no ordering guarantee across different matches.
Projection state is process-local and is derived through pure state transitions.
