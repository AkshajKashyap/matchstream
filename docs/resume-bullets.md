# Factual resume bullet candidates

- Built an event-driven football replay platform with Redpanda, PostgreSQL,
  FastAPI/WebSockets, React, a typed canonical event model, and per-match order.
- Implemented transactional durable projection with processed-event identity,
  canonical history, deterministic rebuild, and crash-safe duplicate redelivery.
- Added bounded retry and poison-event DLQ recovery; real-service integration
  verifies later valid records continue after quarantine.
- Benchmarked the local pipeline at 37.9–44.9 synthetic events/s and measured
  24.1–29.1 ms durable-commit-to-WebSocket receipt for 1–8 local clients.
