# Milestone 4 implementation plan

## Current failure windows

The current flow is poll -> wire deserialization/validation -> process-local
duplicate check -> pure transition -> in-memory state update -> Kafka offset
commit. A failure before the transition leaves no state change; a failure before
the Kafka commit causes redelivery. Because both state and duplicate identity are
currently process-local, a crash after state update but before commit loses both
and can apply a redelivery twice after restart.

Milestone 4 eliminates that window by committing the new snapshot and event ID
in one PostgreSQL transaction before Kafka acknowledgement. It cannot eliminate
Kafka redelivery after a committed database transaction but failed offset commit;
instead that redelivery becomes a durable no-op. It does not provide a distributed
database/Kafka transaction or exactly-once delivery.

## Steps

1. Validate the contiguous sequence invariant at ingestion, which is required
   by the existing strict transition function.
2. Add explicit MatchState JSON serialization and a PostgreSQL repository with
   schema setup, row/advisory locking, and one transaction for snapshot plus
   processed identity.
3. Add a durable projector and reuse the existing post-processing Kafka commit
   boundary without putting SQL in the pure transition.
4. Add PostgreSQL to Compose, database configuration, initialization/state CLI
   commands, and a durable consumer command.
5. Cover serialization, rollback, restart, duplicate redelivery, commit
   failures, PostgreSQL integration, and end-to-end broker/database processing.
