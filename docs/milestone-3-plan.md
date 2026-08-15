# Milestone 3 implementation plan

## Existing lifecycle and constraints

StatsBomb ingestion creates source-independent `CanonicalEvent` values. The
replay engine deterministically orders them, while the v1 contract and Redpanda
adapter serialize, validate, and manually acknowledge them. The consumer returns
a validated event before its offset is committed. Existing `shot.outcome` data is
already retained as canonical metadata, so goal detection needs no wire-contract
change.

## Steps

1. Add a typed, validated, source-independent `MatchState` and a pure
   event-to-state transition function with explicit ordering failures.
2. Add a process-local multi-match projector that checks duplicate identity only
   after a transition succeeds, so failed events remain retryable.
3. Add a small processing function that projects first and commits a consumer
   offset only on successful projection (duplicates are safe success cases).
4. Extend the CLI with a stateful consumer, then test state, scoring, ordering,
   isolation, duplicate behavior, and the commit boundary without a broker.
5. Document state-projection and delivery semantics, run ordinary checks, and
   run broker verification only if the local Docker environment permits it.
