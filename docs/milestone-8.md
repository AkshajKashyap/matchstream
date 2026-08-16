# Milestone 8: React dashboard frontend

## Scope delivered

`frontend/` is an independent React + TypeScript + Vite application. It gives
an operator a compact, responsive view of the match projections already
published by the FastAPI service:

- a selectable match list loaded from `GET /api/v1/matches`;
- a durable selected-match snapshot from `GET /api/v1/matches/{match_id}`;
- a recent, bounded event timeline from the matching `/events` endpoint;
- a live state lifecycle from `/stream`: `CONNECTING`, `LIVE`,
  `RECONNECTING`, or `DISCONNECTED`;
- score, period, clock, possession, processed-event count, available event
  counts, and a collapsed technical panel.

The interface shows only fields returned by the API. It intentionally does not
invent xG, possession percentages, predictions, team crests, or match context.

## Data flow and correctness boundary

```text
FastAPI HTTP -> list and durable snapshot -> dashboard state
       |                                      |
       +-> history pages --------------------> bounded timeline

FastAPI WebSocket -> snapshot / state_update -> current-state notification
                                              -> sequence gap? HTTP reconcile
```

HTTP reads are authoritative. The WebSocket is intentionally best effort:
after it delivers a `snapshot`, its state replaces local state; stale updates
are ignored; a forward sequence gap triggers a fresh durable snapshot and a
history refresh. Failed sockets retry with bounded exponential backoff (up to
six attempts). The component closes its socket and cancels outstanding HTTP
requests when a user changes matches or unmounts.

The recent timeline contains at most 50 events, deduplicated by canonical
event ID and sorted by sequence. This keeps the browser bounded while allowing
the API to remain the sole source of durable event history.

In this document, a **snapshot** is the current durable `MatchState` returned
by HTTP (and again on WebSocket connect). A **live update** is a best-effort
WebSocket notification containing newer committed state. Its **sequence
number** is the monotonic event progression used to reject stale data or
detect a gap. **Reconciliation** means refetching the durable snapshot and
ordered event history after that gap is detected.

## Configuration and local run

Run the existing API service first. The dashboard defaults to
`http://127.0.0.1:8000` and derives `ws://127.0.0.1:8000` automatically.
Copy `frontend/.env.example` to `frontend/.env.local` to set a different API
or WebSocket base URL.

```bash
make frontend-install
make frontend-dev
```

The API enables CORS only for explicit origins. Its local default is
`http://localhost:5173,http://127.0.0.1:5173`; deployment environments set:

```bash
MATCHSTREAM_CORS_ORIGINS=https://dashboard.example.test
```

`*` and an empty value are rejected. The API allows read-only `GET` requests;
the browser dashboard has no write surface.

## Frontend quality boundary

The visual hierarchy is match list, score/clock, available durable state, then
timeline. A dark field-inspired palette, keyboard focus indicators, semantic
controls, and a single-column mobile layout support practical live monitoring
without a chart-heavy control-room imitation. The technical connection data is
available but collapsed by default.

The replay interface remains the terminal CLI documented in earlier
milestones. This dashboard neither starts nor controls replay.

## Validation

Frontend tests use Vitest and React Testing Library. They cover deterministic
timeline merge/gap decisions, match selection/socket cleanup, WebSocket
snapshot transition to `LIVE`, and stale state-update rejection.

```bash
make frontend-test
make frontend-build
make check
make integration
```

The full live-path manual check is: start PostgreSQL/Redpanda and the API using
the following terminals. Use a unique topic and consumer group when repeating
the demo so retained records do not mix with the new replay.

**Terminal 1 — infrastructure**

```bash
docker compose up -d
```

**Terminal 2 — durable processor**

```bash
python -m matchstream.streaming.cli init-db
python -m matchstream.streaming.cli resilient-project \
  --topic matchstream-dashboard-demo \
  --group-id matchstream-dashboard-demo-projector
```

**Terminal 3 — API**

```bash
make api
```

**Terminal 4 — dashboard**

```bash
make frontend-install
make frontend-dev
```

**Terminal 5 — replay**

```bash
python -m matchstream.streaming.cli produce \
  --topic matchstream-dashboard-demo \
  --match-id dashboard-demo \
  --no-wait tests/fixtures/statsbomb_events.json
```

The included fixture is deliberately small: four events, no score change, and
one unknown away team. It is adequate for verifying state, ordering, and live
delivery but not for a visually rich scoring demo. No richer local open-data
match was available, and no dataset was downloaded automatically.

During implementation, this exact infrastructure path was exercised with an
isolated topic: the four events projected to PostgreSQL, `GET` returned the
durable sequence-4 state with the configured CORS header, and the API WebSocket
returned its protocol-v1 snapshot. Vite served the dashboard entry page. A
browser screenshot was not captured because no browser automation was
available; future real screenshots belong under `docs/assets/`.

## Known limitations

- The API does not currently expose fixture date, competition, venue, team
  crests, event commentary, analytics, or prediction data, so the dashboard
  cannot show them accurately.
- WebSocket updates are best effort. The client reconciles a detected forward
  sequence gap, but an entirely silent dropped connection is handled only when
  the browser reports the close/error and reconnects.
- The dashboard is intentionally unauthenticated because the existing API is
  unauthenticated; deployment authentication belongs to a later boundary.

## What comes next

Milestone 9 should add an explicitly scoped API/product boundary before
expanding dashboard content: authentication/authorization, deployment-safe
configuration, pagination or search for larger match lists, and APIs for any
new analytics only after the backend derives and persists them.
