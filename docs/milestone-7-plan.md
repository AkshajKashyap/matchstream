# Milestone 7 implementation plan

## API boundary

The API exposes durable match summaries, full current snapshots, and bounded
canonical event history. It does not expose database rows, raw StatsBomb JSON,
Kafka offsets, DLQ administration, or projection mutation. PostgreSQL remains
the source of truth; the API never consumes the event topic.

## Live-update design

Use PostgreSQL `NOTIFY` after the projection transaction commits. A dedicated
API-process listener uses `LISTEN` and treats each notification only as a wake-up
signal. It re-reads the authoritative snapshot and broadcasts it to clients for
that match. This works with independently running projector and API processes,
requires no Redis, and does not turn notifications into durable state.

The listener starts before WebSocket clients are accepted. Per connection, the
manager registers the socket, reads and sends the durable snapshot, then marks
the connection ready. Notifications arriving during initialization are retained
as a pending signal and result in a fresh read after the snapshot. Duplicate
notifications are harmless because sequence numbers suppress stale updates.

## Recovery and connection behavior

A socket receives a `snapshot` first, followed by best-effort `state_update`
messages containing the durable state's latest sequence. On disconnect, its
connection is removed. Reconnection creates a new socket and receives a fresh
snapshot; clients needing a gap-free record use the paginated history endpoint.
Slow/failing sends are bounded with a short timeout and only remove the affected
connection. There are no durable socket queues or exactly-once claims.

## Signals and scope

Add bounded HTTP/WebSocket metrics using normalized route, method, status class,
and message type labels. Match ID, event ID, client address, and raw URL stay in
logs, never metric labels. API readiness checks PostgreSQL only; Redpanda is not
an API dependency. Reuse `/health/live`, `/health/ready`, and `/metrics` through
FastAPI, but do not merge or refactor the existing projector observability server.

This milestone adds FastAPI, Pydantic boundary schemas, storage reads, a
notification listener, tests, and a small API launch command. It excludes
Redis, frontend work, auth, Kafka consumption by the API, durable WebSocket
queues, and new football analytics.
