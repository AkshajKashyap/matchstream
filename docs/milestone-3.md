# Milestone 3: Stateful match projection and reliable consumer processing

Milestone 3 builds the first stream-processing layer. It derives a current,
deterministic `MatchState` from ordered canonical events while keeping football
state logic independent of Redpanda and Kafka.

```text
immutable event log
        |
        v
CanonicalEvent -> wire validation -> duplicate check -> pure state transition
                                                        |
                                                        v
                                               MatchProjector (match_id -> MatchState)
                                                        |
                                                        v
                                             successful processing -> commit offset
```

## Terms

- **Event log**: the immutable ordered stream stored by the broker.
- **Event identity**: `CanonicalEvent.event_id`, the stable identity of one
  football event.
- **Projection**: a derived, mutable-in-practice view built by applying events;
  `MatchProjector` keeps it in process memory.
- **State**: the current `MatchState` value for one match.
- **Offset**: a broker position committed by a consumer group after processing;
  it is not an event identity or application state.

## MatchState

`MatchState` is a typed, validated, source-independent frozen dataclass. It
contains the match ID, current period and period clock, latest sequence, total
processed event count, inferred home/away teams, score by team ID, latest
possession team, event counts by type, and the last processed event ID.

The state validates non-negative clocks/counts/scores, requires event counts to
sum to the total, and prevents two inferred sides from being the same team. It
is intentionally a compact current representation, not an analytics model.

Teams are inferred only from the first two distinct `Starting XI` events: the
first is home and the second is away. Other events identify a team but do not
reliably establish home/away. This avoids pretending that every source event
reveals a side.

## Pure transition and ordering policy

`apply_event(previous_state, event)` is a pure transition function: it returns a
new `MatchState` and has no broker, deduplication, or global-state dependency.
It rejects events for another match, stale sequences, same-period clock
regression, and period regression.

The first observed event establishes the sequence baseline, which allows a
projection to start mid-match. Thereafter the policy is strict: only exactly
`latest_sequence + 1` is accepted. A gap raises `SequenceGapError`; no buffering
or reordering is attempted. The broker key still keeps one match on one
partition, but local validation protects a projector from invalid input.

## Score reconstruction

A canonical event counts as a goal only when `event_type` is `Shot` and its
canonical `metadata.outcome.name` is `Goal` (case-insensitive). StatsBomb
ingestion already mapped this selected shot outcome into canonical metadata, so
the v1 wire contract did not need to change. A goal increments the scoring
event's `team` score; a recognized goal without a team fails clearly.

Normal goals and non-goal shots are covered. Own goals, awarded goals without a
shot, penalty shoot-out scoring, score corrections, and unusual provider-specific
scoring cases are intentionally not inferred yet.

## Duplicate handling and acknowledgement boundary

`MatchProjector` owns an `EventDeduplicator` keyed by `event_id`. It checks for a
known duplicate before transitioning, but records an identity **only after** the
transition succeeds. Therefore a failed event is not accidentally converted into
a successful duplicate on retry. A known duplicate is a safe no-op and can be
acknowledged.

`project_next()` implements the consumer boundary:

```text
receive -> deserialize/validate -> duplicate check -> transition -> commit
                                                     failure -> no commit
```

It calls `consumer.commit()` only after `MatchProjector.process()` returns.
Projection errors propagate and leave the offset uncommitted. If commit itself
fails after state has changed, a later redelivery is safely ignored only while
this process's deduplication memory remains alive.

## State lifetime and guarantees

The projector handles multiple interleaved matches with an independent
`match_id -> MatchState` mapping. It is deterministic for the same initial
state and valid ordered sequence. State and deduplication memory are lost on
restart; rebuilding requires replaying events from an appropriate broker offset.

This provides a process-local at-least-once processing foundation with manual
commits. It does **not** provide durable idempotency, exactly-once processing,
crash-safe snapshots, global ordering, reordering buffers, or a dead-letter
queue.

## Stateful Redpanda demo

With dependencies installed and Redpanda running, start the stateful consumer:

```bash
docker compose up -d
python -m matchstream.streaming.cli project \
  --topic matchstream.events.v1 --group-id milestone3-demo --max-events 4
```

Then publish the compact fixture in another terminal:

```bash
python -m matchstream.streaming.cli produce \
  tests/fixtures/statsbomb_events.json --match-id demo-match \
  --topic matchstream.events.v1 --no-wait
```

The fixture contains a home `Starting XI`, two passes, and a half start—no shot
or away `Starting XI`. The projector therefore prints four state updates with no
goal and an explicit unknown away side (`score 0-?`), rather than fabricating an
away team or score.

## Verification

Normal tests do not require Docker:

```bash
pytest -q
ruff check .
```

The real-broker transport and replay-to-projection tests remain separately
runnable when Docker is available:

```bash
make integration
```

The stateful CLI above manually verifies the complete replay-to-projection path
when a local Redpanda broker can run.
