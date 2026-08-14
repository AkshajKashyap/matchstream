# MatchStream Architecture

## Initial Target

Historical football event data is replayed as a live event stream.

Planned flow:

1. Dataset ingestion
2. Match replay producer
3. Event broker
4. Stateful consumers
5. Analytics and prediction
6. Redis/PostgreSQL persistence
7. FastAPI/WebSocket serving
8. Live dashboard
9. Observability
10. Load and failure testing
