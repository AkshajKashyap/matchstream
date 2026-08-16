# Milestone 8 implementation plan

## Frontend architecture

Create an independent `frontend/` React, TypeScript, and Vite application. A
small typed API client consumes only FastAPI HTTP endpoints; a match-session
hook owns socket lifecycle, snapshot/state reconciliation, and bounded timeline
state. Presentational components remain separate from transport logic. The
browser never accesses PostgreSQL, Kafka, or internal Python modules.

## Views and visual hierarchy

The single dashboard view has a restrained header, persistent match list,
prominent scoreboard/live status, basic event-count cards, an ordered timeline,
and a collapsed technical status panel. Desktop uses a left match rail and
content area; narrower widths stack the rail above the selected-match content.

## HTTP and WebSocket behavior

The dashboard loads matches, then fetches the selected durable snapshot and a
bounded recent history window. It opens one match-specific socket, closes it on
selection change/unmount, and shows `CONNECTING`, `LIVE`, `RECONNECTING`, or
`DISCONNECTED` from the real lifecycle. Reconnect delay is bounded exponential;
a reconnect snapshot replaces cached state authoritatively.

For a state update, stale/duplicate sequences are ignored. A sequence jump
triggers reconciliation: refetch current snapshot and fetch history after the
last locally known event. Normal updates incrementally fetch events after the
timeline's newest sequence. The in-browser timeline is capped.

## Available data and boundaries

The API supplies teams when inferred, team-ID keyed score, period, clock,
event counts, possession team, latest sequence, last event ID, update time, and
canonical event fields. The UI will not invent logos, competition metadata,
player information, score changes, xG, possession percentages, or charts.
The local StatsBomb fixture contains only four events and no goal, so it is used
for tests and the documented demo without fabrication.

## Missing browser requirement

The separate Vite development server requires CORS. Add only explicit,
environment-configured local origins to FastAPI. Replay remains a documented
terminal operation: the browser never executes CLI commands or controls a
broker directly.
