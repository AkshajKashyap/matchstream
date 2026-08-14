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
