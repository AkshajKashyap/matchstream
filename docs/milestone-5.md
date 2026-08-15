# Milestone 5: Poison events, dead-letter recovery, and rebuild tooling

A poison event is a valid canonical event that repeatedly cannot satisfy the deterministic projection rules and would otherwise block its Kafka partition. This milestone adds deliberate quarantine; it does not fix or discard the event.

## Retry and classification

Only deterministic MatchTransitionError failures are poison candidates. Database, broker, and unknown failures are retriable: they receive bounded attempts and then remain uncommitted rather than being misclassified as bad football events. The default RetryPolicy has three attempts and supports capped delays; tests inject no-delay timing.

Application retries are attempts in one consumer delivery. Kafka redelivery happens when an offset remains uncommitted; these are distinct.

## Dead-letter contract and ordering

DeadLetterEvent is deterministic JSON v1 with schema matchstream.dead-letter-event. It includes a deterministic record ID, complete original canonical event envelope, event/match identity, source topic/partition/offset, failure classification, concise error type/message, attempt count, and timestamp.

The record ID is SHA-256-derived from source topic, partition, offset, and event ID, and is used as the DLQ Kafka key. This makes duplicates identifiable but not impossible: a process can crash after DLQ acknowledgement and before source offset commit.

Normal source offset ordering is:

1. durable projection success -> commit source offset
2. durable duplicate -> commit source offset
3. persistent poison -> confirmed DLQ publish -> commit source offset
4. DLQ publish failure -> do not commit source offset

There is no distributed Kafka/PostgreSQL transaction and no exactly-once DLQ guarantee.

## CLI operations

Run resilient projection with an explicit DLQ topic:

    python -m matchstream.streaming.cli resilient-project --topic matchstream.events.v1 --dlq-topic matchstream.events.v1.dlq --group-id matchstream-resilient --max-attempts 3

Inspect and explicitly replay:

    python -m matchstream.streaming.cli dlq-list --dlq-topic matchstream.events.v1.dlq
    python -m matchstream.streaming.cli dlq-show --dlq-topic matchstream.events.v1.dlq --group-id operator-show-20260815 --record-id dlq:...
    python -m matchstream.streaming.cli dlq-replay --dlq-topic matchstream.events.v1.dlq --group-id operator-replay-20260815 --record-id dlq:... --target-topic matchstream.events.v1

Replay is always operator initiated and preserves original event identity. A previously successful event is a durable no-op; an unchanged poison event will quarantine again.
`dlq-list` commits records it lists, while `dlq-show` and `dlq-replay` do not commit the selected record. Use a fresh `--group-id` to rescan retained DLQ records after a prior listing.

## Offline rebuild and status

Successful durable processing now stores canonical history in projection_events within the same transaction as snapshot and processed identity. Rebuild locks the target match, applies that history in sequence order through the pure transition, and replaces only the snapshot. It deliberately retains processed_events.

Stop live consumers for the match first; rebuild is maintenance-mode only.

    python -m matchstream.streaming.cli rebuild --match-id demo-match
    python -m matchstream.streaming.cli status --match-id demo-match

Pre-Milestone-5 snapshots without stored history cannot be rebuilt automatically and fail clearly.

## Guarantees and limits

After confirmed poison quarantine, later records in the same source partition can progress and durable state is not mutated by the poison event. Rebuilding the same history is deterministic.

This does not provide automatic event repair, online rebuilds, distributed transactions, exactly-once delivery, exactly-once DLQ publication, or a web administration interface.

## Validation

    make check
    make integration

Integration coverage verifies valid event 1, a poison sequence-gap event at sequence 3, and later valid event 2: the poison event reaches a real Redpanda DLQ, later projection progresses, and rebuild returns the same durable snapshot.
