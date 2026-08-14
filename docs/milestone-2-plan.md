# Milestone 2 implementation plan

## Existing architecture and constraints

Milestone 1 converts local StatsBomb JSON into validated, source-independent
`CanonicalEvent` instances and replays them deterministically. Ingestion owns
provider details; `MatchReplayer` only orders and emits canonical events. The
existing event ID is globally scoped (`source:match_id:source_event_id`), and
the canonical model includes tuples and entity dataclasses that are not directly
JSON serializable.

Milestone 2 must preserve those boundaries: the wire contract must explicitly
encode/decode every canonical field, replay must remain broker-agnostic, and
Kafka code must not leak into ingestion or models. Deterministic JSON requires
stable key ordering and a defined representation for optional entities and
coordinate tuples.

## Steps

1. Add a versioned, deterministic JSON envelope and strict deserializer for
   `CanonicalEvent` values.
2. Add minimal publisher/consumer protocols, process-local duplicate detection,
   and a replay-to-publisher connector independent of Kafka.
3. Add a Redpanda adapter using `confluent-kafka`, configuration objects, and
   producer/consumer CLIs with explicit startup and shutdown handling.
4. Replace the placeholder Compose file with a one-node Redpanda service and
   add separately selected broker integration coverage.
5. Add focused unit tests, document the contract and delivery semantics, then
   run normal validation and broker checks if the local environment permits.
