# Milestone 4: Durable projection state and idempotent crash recovery

Milestone 4 makes a match projection survive process restart and makes Kafka
redelivery safe after a database transaction succeeds but an offset commit does
not. PostgreSQL stores an explicit snapshot plus every event identity that has
successfully affected that snapshot.

```text
Kafka event log -> deserialize/validate -> PostgreSQL transaction
                                           |- reserve event_id
                                           |- load/lock MatchState
                                           |- pure transition
                                           |- save snapshot + event_id
                                           `- COMMIT
                                                    |
                                                    v
                                             commit Kafka offset
```

## Core terms

- **Transaction**: database operations that commit together or roll back
  together.
- **Durable state / snapshot**: the persisted current `MatchState`, which
  survives a consumer process restart.
- **Projection**: derived match state built from the immutable event log.
- **Idempotency**: applying the same logical event more than once has the same
  effect as applying it once.
- **Kafka offset**: a consumer group's acknowledged broker position, distinct
  from event identity and match state.
- **Redelivery**: receiving an event again because a prior delivery was not
  acknowledged to Kafka.

## PostgreSQL schema and setup

The repository creates its compact schema idempotently:

```text
match_states
  match_id          primary key
  state             JSONB validated MatchState snapshot
  latest_sequence
  last_event_id
  updated_at

processed_events
  event_id          primary key
  match_id
  sequence
  processed_at
```

`processed_events.event_id` is the durable idempotency constraint. `state` is
explicit JSON-compatible data created by `state_to_dict()` and validated by
`state_from_dict()`; no pickle or arbitrary Python object is persisted.

PostgreSQL is configured through `MATCHSTREAM_DATABASE_URL`, defaulting to the
development value in [.env.example](../.env.example). Docker Compose supplies a
PostgreSQL 17 service, persistent volume, and readiness check.

```bash
docker compose up -d
python -m matchstream.streaming.cli init-db
# or: make db-init
```

The implementation uses [`psycopg` 3](https://www.psycopg.org/psycopg3/), the
maintained PostgreSQL driver. Its direct cursor and transaction APIs keep this
small repository explicit; an ORM would not improve this milestone's few SQL
operations.

## Atomic durable processing

`PostgresProjectionRepository.process_event()` uses one PostgreSQL transaction:

1. Obtain a transaction-scoped advisory lock for the match ID. This also works
   before a match-state row exists.
2. Insert the event ID with `ON CONFLICT DO NOTHING`.
3. If the ID already exists, load the state and return a duplicate no-op.
4. Otherwise load and row-lock the snapshot, run the pure
   `MatchState + CanonicalEvent -> MatchState` transition, and upsert the new
   snapshot.
5. Commit the transaction containing both snapshot and identity.

If transition, serialization, or database work fails, the transaction rolls
back: neither a new snapshot nor a processed-event identity becomes durable.
The pure transition does not issue SQL.

The Kafka key remains `match_id`, so one consumer group normally has only one
active consumer for a match partition. The database still relies on the event-ID
primary key and match advisory lock rather than assuming that broker topology
alone prevents races.

## Offset acknowledgement and crash windows

The durable consumer path is:

```text
poll -> deserialize/validate -> DB transaction -> DB commit -> Kafka commit
                                      failure -> no Kafka commit
```

If database processing fails, `project_next()` never reaches `consumer.commit()`.
If database commit succeeds but Kafka commit fails, Kafka can redeliver the
event. The persisted event ID makes that redelivery a successful state no-op,
and a later offset acknowledgement is safe.

This provides idempotent state processing on top of possible at-least-once Kafka
delivery. It is not Kafka exactly-once delivery, a distributed transaction, or a
claim that every failure mode has global exactly-once semantics.

## Sequence and recovery

The durable projector preserves Milestone 3's strict ordering rule: after the
first observed event, the next event must be `latest_sequence + 1`. StatsBomb
ingestion now rejects non-contiguous source indexes after deterministic ordering,
so the canonical event streams produced by current ingestion/replay meet this
invariant. A projector may still establish a baseline from a mid-match first
event when a caller intentionally starts from a subset.

On restart, construct a new `DurableMatchProjector` using the same repository.
It loads the stored snapshot within the next transaction, rather than replaying
the entire match. Tests prove that continuous processing and processing half a
match, restarting, then processing the remainder produce the same state.

## Durable CLI demonstration

Initialize local infrastructure and schema:

```bash
docker compose up -d
python -m matchstream.streaming.cli init-db
```

Start a durable consumer for the first two events in one terminal:

```bash
python -m matchstream.streaming.cli durable-project \
  --topic matchstream.durable-demo --group-id milestone4-demo --max-events 2
```

Publish the fixture in another terminal:

```bash
python -m matchstream.streaming.cli produce \
  tests/fixtures/statsbomb_events.json --match-id demo-match \
  --topic matchstream.durable-demo --no-wait
```

Restart the durable consumer with the same group to process the remaining two
events, then inspect the snapshot:

```bash
python -m matchstream.streaming.cli durable-project \
  --topic matchstream.durable-demo --group-id milestone4-demo --max-events 2
python -m matchstream.streaming.cli state --match-id demo-match
```

To demonstrate duplicate safety, replay the same fixture to another topic while
using the same durable match ID. The `processed_events` primary key ensures its
four events do not change the persisted event count again.

## Verification

Regular checks do not require Docker:

```bash
pytest -q
ruff check .
```

With local Redpanda and PostgreSQL running:

```bash
make integration
```

Integration coverage includes schema creation, snapshot persistence/loading,
processed-event uniqueness, rollback on an invalid transition, restart recovery,
and replay through Redpanda into durable PostgreSQL state.

## Current guarantees and limits

After a database transaction has committed, a redelivery of its same canonical
event ID cannot mutate the durable projection twice. State is durable across
consumer process restart, and offsets are committed only after durable processing
succeeds.

The system does not provide distributed PostgreSQL/Kafka transactions, Kafka
exactly-once delivery, durable dead-letter handling, snapshot version migration,
cross-partition ordering, or high-availability database operations. A database
outage leaves the Kafka offset uncommitted and relies on later redelivery.
