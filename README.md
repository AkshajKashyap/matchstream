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
versioned JSON contract and optional local Redpanda transport.
