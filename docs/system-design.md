# MatchStream system design

## Requirements and boundary

MatchStream turns historical football events into a replayable live stream with
durable current state. Its core properties are provider independence, per-match
ordering, recoverable state, observable failures, and a simple read/live UI
boundary. It deliberately excludes authentication, HA, and advanced analytics.

## Canonical events and transport

StatsBomb JSON is converted once into `CanonicalEvent`: stable event/match IDs,
sequence, period/clock, type, selected team/player/location/possession context,
small metadata, and source identity. Keeping this model independent prevents
the rest of the system from coupling to provider JSON. Versioned JSON protects
the broker boundary.

`match_id` is the Redpanda/Kafka key. Events for one match share a partition
and retain partition order; cross-match ordering is intentionally not promised.
Transitions additionally validate contiguous sequences, making reordering
visible rather than silently applying it.

## Projection and PostgreSQL

`MatchState` is a materialized projection: replaceable current state derived
from events. One PostgreSQL transaction records event identity, canonical
history, and the updated snapshot under a match advisory lock. Event ID is the
durable idempotency key; redelivery of a committed event is a no-op.

There is no distributed Kafka/PostgreSQL transaction. A crash after the DB
commit but before offset commit causes redelivery, and identity prevents double
state application. This is at-least-once delivery with idempotent durable state
application, not exactly-once delivery.

## Retry, DLQ, and rebuild

Deterministic transition failures receive bounded retries and then a confirmed
DLQ record before source progress advances. Database, broker, and DLQ failures
remain uncommitted for redelivery rather than being mislabeled poison data.
Stored canonical history can rebuild a snapshot through the same deterministic
transition function during explicit maintenance.

## API and live consistency

FastAPI only reads PostgreSQL; it neither consumes Kafka nor derives football
state. A committed transaction emits `pg_notify`, waking the API to reread
durable state before match-scoped broadcast. HTTP snapshot/history is truth.
WebSocket updates are best-effort notifications; reconnect snapshots and
sequence-aware history reconciliation recover from missed transient messages.

## Observability and scale trade-offs

Logs retain event correlation while Prometheus uses bounded labels. Health and
lag are explicit operational checks. Local benchmarks identify PostgreSQL
transaction time as the throughput constraint; no speculative optimization was
made. At greater scale, add partitions/consumers by match affinity, deliberate
database pooling, then shared API fan-out only when measured demand requires it.
