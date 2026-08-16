# Milestone 7: Live Match API and WebSocket state streaming

Milestone 7 adds an independently runnable FastAPI process. It reads current
state and canonical history from PostgreSQL; it does not consume Kafka and does
not calculate match state.

## Terms and consistency

A **REST API** is a request/response interface for retrieving durable state and
history. A **WebSocket** is a persistent bidirectional connection; MatchStream
uses it for server-to-client live messages. A **snapshot** is the current
durable materialized `MatchState`. A **notification** is only a signal that
state may have changed.

PostgreSQL is the **source of truth** for current projections. The API reads
committed PostgreSQL state for HTTP responses and before every WebSocket update.
WebSocket delivery is therefore an eventual convenience, not a durable event
log. This is why **PostgreSQL = truth; WebSocket notification = convenience**.

## Running the API

Start infrastructure and the independent processes:

    docker compose up -d
    make db-init
    make api

The API listens on `127.0.0.1:8000` by default; set `MATCHSTREAM_API_HOST` or
`MATCHSTREAM_API_PORT` to override it. FastAPI HTTP documentation is at
`/docs`; WebSocket routes are documented here rather than in OpenAPI.

## HTTP endpoints

- `GET /api/v1/matches?limit=50&after_match_id=...` returns a deterministic
  match-ID page of durable summaries. `limit` is 1–100.
- `GET /api/v1/matches/{match_id}` returns a durable current snapshot with
  period, clock, sequence, event count, teams, score, possession, event counts,
  last event ID, and commit timestamp.
- `GET /api/v1/matches/{match_id}/events?after_sequence=...&limit=50` returns a
  bounded canonical history page ordered by sequence. It is not raw StatsBomb
  JSON and has a maximum page size of 100.
- `GET /health/live`, `GET /health/ready`, and `GET /metrics` provide API
  liveness, PostgreSQL readiness, and Prometheus metrics.

Unknown matches return `404 {"detail":"match not found"}`. Storage failures
return `503 {"detail":"durable state unavailable"}` without database details.

## WebSocket protocol

Connect to:

    ws://127.0.0.1:8000/api/v1/matches/{match_id}/stream

The first message is always a current durable snapshot:

    {"type":"snapshot","protocol_version":1,"state":{"match_id":"...","latest_sequence":12,"...":"..."}}

Later state commits may produce:

    {"type":"state_update","protocol_version":1,"state":{"match_id":"...","latest_sequence":13,"...":"..."}}

Every state includes `latest_sequence`, allowing clients to notice a gap. On a
disconnect, the client establishes a new socket and receives a fresh durable
snapshot. For gap-free recovery, use the event-history endpoint; MatchStream
does not create durable per-client socket queues or promise delivery of every
transient WebSocket message.

## Post-commit update propagation

The projector calls PostgreSQL `pg_notify` within its successful state/history
transaction. PostgreSQL delivers that notification only after the transaction
commits. The API's dedicated `LISTEN` connection receives the match ID, rereads
the authoritative snapshot, and broadcasts it only to sockets subscribed to
that match. Duplicate or missed notifications cannot corrupt a client because
state is reread and sequence-gated.

The listener starts before the API accepts connections. Each socket is
registered before its snapshot read. A notification arriving between registration
and snapshot delivery is marked pending and causes another durable read after
the snapshot, closing the initial-subscription race. Slow/failing socket sends
time out and remove only that connection.

## Metrics and logs

API metrics include:

- `matchstream_http_requests_total` and
  `matchstream_http_request_duration_seconds`, labelled only by normalized route,
  method, and status class.
- `matchstream_websocket_connections`
- `matchstream_websocket_messages_total`, labelled by protocol message type.
- `matchstream_websocket_errors_total`, labelled by bounded operation.

Match IDs, event IDs, client addresses, and raw dynamic URLs are not metric
labels; they are available in structured JSON logs where needed. The API logs
startup/shutdown, socket connect/disconnect, notification read failures, and
storage read failures without logging complete states or histories.

## PostgreSQL query patterns

The API adds repository methods for one snapshot, match-ID ordered summaries,
and bounded history pages. Existing `projection_events(match_id, sequence)`
indexing supports history lookup and ordering; no new index is needed. API reads
do not write or claim Kafka offsets.

## Live local demonstration

Use separate terminals:

1. Infrastructure:

       docker compose up -d
       make db-init

2. Durable projector:

       python -m matchstream.streaming.cli resilient-project --topic matchstream.api.demo --dlq-topic matchstream.api.demo.dlq --group-id api-demo

3. API:

       make api

4. Replay producer (after the first three processes are ready):

       python -m matchstream.streaming.cli produce --topic matchstream.api.demo --match-id api-demo-match --speed 0.5 tests/fixtures/statsbomb_events.json

5. After the first projection appears in terminal 2, observe updates:

       make websocket-demo URL=ws://127.0.0.1:8000/api/v1/matches/api-demo-match/stream

The client prints the initial snapshot and subsequent durable state updates.

## Scope decisions and limitations

Redis is deliberately deferred: PostgreSQL notifications plus authoritative
snapshot reads meet this local multi-process milestone without another mutable
system. There is no authentication, frontend, Kafka consumer in the API,
distributed WebSocket fan-out, durable client queue, exactly-once socket
delivery, or horizontal API coordination yet.
