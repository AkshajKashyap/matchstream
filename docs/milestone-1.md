# Milestone 1: Canonical events, StatsBomb ingestion, and deterministic replay

Milestone 1 provides a deliberately small foundation for MatchStream. It reads
a local StatsBomb Open Data event JSON array, converts each event into a stable
internal representation, and replays that representation in match order.

```text
StatsBomb event JSON
        |
        v
StatsBomb parser and validation       (src/matchstream/ingestion)
        |
        v
CanonicalEvent sequence               (src/matchstream/models)
        |
        v
MatchReplayer -- callback / iterator / async iterator
        |
        v
future broker publisher -> independent consumers
```

## Canonical event design

`CanonicalEvent` is a frozen, slotted standard-library dataclass. Dataclasses
are appropriate here because the model needs a small fixed Python-domain shape,
explicit validation, and no serialization or web-framework features. The model
itself remains dependency-free; a transport adapter can add dependencies at its
boundary without changing ingestion or replay.

Each event has these fields:

| Field | Meaning |
| --- | --- |
| `event_id` | Globally scoped internal ID: `statsbomb:{match_id}:{source_event_id}`. |
| `match_id` | Match identifier supplied by the caller, normalized to a string. |
| `sequence` | Provider event index, retained for tie-breaking and traceability. |
| `period` | Match period (for example, first or second half). |
| `match_clock_seconds` | Elapsed seconds within the period. |
| `event_type` | Provider-neutral event type label, such as `Pass`. |
| `team`, `player` | Optional `EntityReference(id, name)` values. |
| `location` | Optional starting coordinate tuple; 2D and 3D coordinates are supported. |
| `possession_id`, `possession_team` | Optional possession context. |
| `metadata` | Selected event detail: context flags, outcome, recipient, end location, technique/body part, and shot xG where present. |
| `source`, `source_event_id` | Original provider name and provider event identifier. |

The parser requires StatsBomb's `id`, `index`, `period`, `timestamp`, and
`type.name`; a caller supplies the match ID because an Open Data event file is
already scoped to one match. Team, player, coordinates, possession, and
event-specific payloads are safely optional. Malformed required data produces a
`StatsBombIngestionError` identifying the array position and problem.

The canonical metadata intentionally contains only selected details. MatchStream
does not retain a raw event blob, preventing future consumers from accidentally
depending on StatsBomb's full JSON schema.

## Ingestion and ordering

Use `load_statsbomb_events(path, match_id)` for a local JSON file, or
`convert_statsbomb_events(raw_events, match_id)` when the parsed array is
already in memory. The loader never downloads data.

Events are sorted deterministically by:

1. period;
2. period clock;
3. provider sequence/index;
4. canonical event ID; and
5. original array position if all prior fields tie.

This produces chronological replay while preserving provider ordering for
simultaneous events. Ingestion owns parsing and validation; replay knows nothing
about StatsBomb fields.

## Replay behavior

`MatchReplayer` accepts any iterable of `CanonicalEvent` values and sorts it
using the same key. It offers:

- `iter_events()` for synchronous pull-based integration;
- `replay(callback)` for a future broker-publisher adapter; and
- `aiter_events()` for asynchronous publishers.

With ordinary replay configuration, adjacent events in the same period wait for
their period-clock delta divided by `speed`. It intentionally does not simulate
half-time: a transition to a new period has no delay. `ReplayConfig(no_wait=True)`
eliminates all sleeping and is suited to tests, batch processing, and the demo.
The engine contains no football analytics logic.

## Demo

The compact fixture is intentionally representative rather than a real match:

```bash
python -m matchstream.demo tests/fixtures/statsbomb_events.json --match-id demo-match
```

It loads, canonicalizes, sorts, and prints the four fixture events in no-wait
mode.

## Assumptions and current limitations

- The loader targets the documented StatsBomb Open Data event-array structure;
  no Open Data files are bundled or downloaded automatically.
- Canonical coordinates preserve StatsBomb's coordinate values; this milestone
  does not normalize pitch dimensions or directions.
- The event type remains a validated string rather than a closed enum, so new
  provider event types do not force a schema release.
- The in-memory replayer has no persistence, deduplication, out-of-order
  recovery, broker delivery guarantee, or wall-clock scheduling beyond sleeps.

## How this prepares an event broker

An upcoming producer can consume `MatchReplayer.replay()` and serialize only
`CanonicalEvent` objects. Future consumers then depend on the canonical
contract, not StatsBomb JSON, so another source adapter or a broker can be added
without rewriting replay or downstream logic.
