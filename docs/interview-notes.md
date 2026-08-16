# Interview notes

- **Why Redpanda/Kafka?** Ordered, replayable partitioned delivery separates
  replay from projection.
- **Why `match_id` partitioning?** A match projection needs order; cross-match
  order is intentionally traded away.
- **Why PostgreSQL rather than Redis?** The requirement is transactional,
  durable identity/history/snapshot storage; Redis is unnecessary here.
- **What is a projection?** A replaceable current state derived from canonical
  events; canonical history supports rebuild.
- **Why duplicate delivery?** A crash after DB commit but before broker offset
  commit correctly causes redelivery.
- **Exactly once?** No. At-least-once delivery plus durable identity gives
  idempotent state application.
- **Why a DLQ?** A deterministic poison event should not block later records;
  source/failure context is retained for explicit repair/replay.
- **Why LISTEN/NOTIFY?** It wakes API processes after commit; PostgreSQL reads,
  not notifications, remain authoritative.
- **Missed WebSocket messages?** Snapshot/history reconcile durable truth.
- **Measured bottleneck?** PostgreSQL averaged 18.4–22.4 ms per transaction in
  local 80-event trials.
- **How to scale?** More match-affine partitions/consumers, deliberate DB
  pooling, then shared fan-out only when measurement warrants it.
