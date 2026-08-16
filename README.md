# MatchStream

A real-time football analytics and event-streaming platform.

MatchStream replays historical football matches as live event streams and
processes them through an event-driven backend to maintain game state,
calculate analytics, serve live predictions, and expose real-time updates.

## Planned Architecture

StatsBomb Data
→ Replay Producer
→ Event Bus
→ Analytics / State / Persistence Consumers
→ Redis + PostgreSQL
→ FastAPI + WebSockets
→ Live Dashboard

## Goals

- Real-time event processing
- Stateful football analytics
- Live win/draw/loss prediction
- Duplicate and out-of-order event handling
- Fault-tolerant consumers
- Observability and benchmarking
- Load and failure testing

## Milestone 1: canonical events, StatsBomb ingestion, and replay

Milestone 1 establishes the provider boundary and a deterministic replay loop;
it deliberately does not add an event broker, database, API, or analytics.
See [the Milestone 1 guide](docs/milestone-1.md) for the event schema, usage,
and assumptions. [The Milestone 2 guide](docs/milestone-2.md) describes the
versioned JSON contract and optional local Redpanda transport. [The Milestone 3
guide](docs/milestone-3.md) documents deterministic in-memory match-state
projection and the consumer acknowledgement boundary. [The Milestone 4 guide]
(docs/milestone-4.md) adds durable PostgreSQL snapshots and idempotent recovery
from Kafka redelivery. The Milestone 5 guide adds bounded poison-event
quarantine, DLQ operations, and offline projection rebuilds. [The Milestone 6
guide](docs/milestone-6.md) adds structured logs, Prometheus metrics, health
checks, and consumer-lag diagnostics. [The Milestone 7 guide](docs/milestone-7.md)
adds the durable-state REST API and best-effort WebSocket updates.

## Milestone 8: dashboard frontend

Milestone 8 adds a separate React, TypeScript, and Vite dashboard in
[`frontend/`](frontend/). It reads durable state through the existing FastAPI
HTTP API, then uses its per-match WebSocket only for live notifications. The
database remains invisible to the browser and remains authoritative.

```text
PostgreSQL durable projection
       | HTTP snapshot/history
       v
FastAPI API --------------------------> React dashboard
       | WebSocket snapshot/update              |
       +-----------------------------------------+
                         bounded timeline + live status
```

Start the backend separately, then configure and run the dashboard:

```bash
cp frontend/.env.example frontend/.env.local
make frontend-install
make frontend-dev
```

The default local origins are `http://localhost:5173` and
`http://127.0.0.1:5173`. Override them for an explicitly configured deployment
with `MATCHSTREAM_CORS_ORIGINS`; wildcard origins are deliberately rejected.
See [the Milestone 8 guide](docs/milestone-8.md) for the frontend contract,
recovery behavior, limitations, and validation commands.
