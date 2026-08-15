# Milestone 5 implementation plan

## Current blocked-partition behavior

The durable consumer polls and validates an event, runs one PostgreSQL-backed
projection transaction, then commits its Kafka offset. A deterministic
`MatchTransitionError` rolls back the database transaction and prevents that
offset commit. Kafka redelivers the same record, so later records in its
partition cannot be reached.

## Steps

1. Add an explicit classifier: database/broker/unknown failures remain
   retriable and uncommitted; deterministic transition failures may be
   quarantined after bounded attempts.
2. Define a versioned dead-letter envelope with a deterministic ID derived from
   source topic/partition/offset and original event identity; add producer,
   consumer, inspection, and explicit replay adapters.
3. Add the resilient processing boundary: process/retry -> confirmed DLQ
   publish -> original source commit. A DLQ publish failure must leave the source
   offset uncommitted.
4. Persist canonical event history transactionally with durable state, then add
   an explicit offline rebuild operation that retains processed-event identity.
5. Add CLI commands, status inspection, focused unit coverage, and broker/
   PostgreSQL integration tests for quarantining, partition progress, replay,
   and rebuild determinism.
