# Milestone 2: Versioned event contract and Redpanda transport

Milestone 2 turns validated `CanonicalEvent` values into an explicit transport
contract and proves the boundary needed to deliver them through a
Kafka-compatible broker. It adds no analytics, persistence, API, or dashboard.

```text
StatsBomb JSON -> parser -> CanonicalEvent -> MatchReplayer
                                              |
                                              v
                                      EventPublisher protocol
                                              |
                                              v
                    deterministic JSON v1 -> Redpanda topic (Kafka API)
                                              |
                                              v
                                      EventConsumer protocol
                                              |
                                              v
                                  validated CanonicalEvent -> application
```

## Wire contract and versioning

`matchstream.streaming.contract` owns serialization and is independent of both
StatsBomb and the broker. `serialize_event()` emits deterministic UTF-8 JSON
bytes: compact JSON, sorted object keys, UTF-8 text, and no `NaN`/`Infinity`.
`deserialize_event()` accepts bytes or text and validates the entire event before
returning it.

Every message has this envelope:

```json
{
  "event": { "...": "canonical event fields" },
  "schema": "matchstream.canonical-event",
  "schema_version": 1
}
```

The stable v1 `event` fields are `event_id`, `match_id`, `sequence`, `period`,
`match_clock_seconds`, `event_type`, `team`, `player`, `location`,
`possession_id`, `possession_team`, `metadata`, `source`, and
`source_event_id`. Entity references are `{id, name}` objects; coordinates are
JSON arrays and become tuples again in Python. Metadata is restricted to JSON
primitives, lists, and objects with string keys. Arbitrary Python objects,
pickle, dataclass auto-serialization, and raw StatsBomb blobs are not accepted.

The parser rejects missing fields, invalid types, malformed JSON, unsupported
schema IDs, and unsupported schema versions with clear contract errors. A future
breaking version can add a separate decoder/migration and then explicitly
support v1-to-v2 handling; no migration framework is needed yet.

## Broker boundary and Redpanda adapter

The small `EventPublisher` and `EventConsumer` protocols isolate the rest of the
application from Kafka client details. `publish_replay()` connects a
`MatchReplayer` to any publisher; it has no Redpanda-specific code.

`RedpandaEventProducer` serializes an event and publishes it. Its counterpart,
`RedpandaEventConsumer`, deserializes and validates the broker value before
returning a `CanonicalEvent`. The consumer disables automatic commits and exposes
`commit()` so a caller commits only after its own processing completes.

The adapter uses [`confluent-kafka`](https://docs.confluent.io/kafka-clients/python/current/overview.html),
the maintained Python binding to `librdkafka`. It is a mature Kafka-compatible
producer/consumer client and works with Redpanda's Kafka API. Broker settings are
provided by `BrokerConfig`, CLI flags, or these environment variables:

- `MATCHSTREAM_BROKER_ADDRESS` (default `localhost:19092`)
- `MATCHSTREAM_TOPIC` (default `matchstream.events.v1`)
- `MATCHSTREAM_CONSUMER_GROUP` (default `matchstream-demo`)

## Identity, duplicates, and ordering

`CanonicalEvent.event_id` is the delivery identity. StatsBomb ingestion creates
it as `statsbomb:{match_id}:{source_event_id}`; it is a stable field in the wire
contract and survives broker transport exactly.

`EventDeduplicator` is a reusable, process-local set keyed by `event_id`. It can
prevent repeated processing during the lifetime of one consumer process, but it
is neither durable nor shared across processes. It is not exactly-once
processing.

The producer uses the UTF-8 `match_id` as the Kafka message key. Kafka-compatible
brokers route the same key to one partition, so events from one MatchStream
replay are published in the replay's deterministic order to that partition.
The broker preserves append order within a partition, not a global order across
partitions. Concurrent producers for the same match can still interleave based
on broker arrival order; MatchStream does not coordinate them.

## Delivery guarantees and non-guarantees

The producer waits for broker delivery callbacks before `publish()` returns and
reports local queue, callback, or timeout failures. A caller that retries after
an uncertain failure can produce a duplicate. The consumer uses manual offset
commits; if it processes an event and crashes before committing, that event can
be delivered again. This is the normal foundation for at-least-once consumer
processing when the caller commits after successful work.

The implementation does not guarantee exactly-once delivery or processing,
durable deduplication, transactional consume-and-produce, cross-partition
ordering, or recovery from every broker/network failure. An at-most-once mode is
not implemented; committing before processing would be required and risks loss.

## Local Redpanda demo

Install dependencies and start the single local broker:

```bash
python -m pip install -e ".[dev]"
docker compose up -d
```

In one terminal, start a consumer before publishing:

```bash
python -m matchstream.streaming.cli consume \
  --topic matchstream.events.v1 --group-id milestone2-demo --max-events 4
```

In another terminal, replay the compact StatsBomb fixture with no delays:

```bash
python -m matchstream.streaming.cli produce \
  tests/fixtures/statsbomb_events.json --match-id demo-match \
  --topic matchstream.events.v1 --no-wait
```

The producer reports four published events. The consumer prints four validated
canonical events in sequence order. Redpanda is defined in
`docker-compose.yml`, exposes its Kafka API on `localhost:19092`, and includes a
health check. Stop it with `docker compose down`; add `-v` only when intentionally
discarding broker data.

## Tests

Normal tests do not need Docker:

```bash
pytest -q
ruff check .
```

With Redpanda running and the package installed, execute the real-broker test:

```bash
make integration
```

That test uses a unique topic and consumer group, publishes one canonical event,
consumes it through the Redpanda adapter, validates its identity, and commits its
offset.
