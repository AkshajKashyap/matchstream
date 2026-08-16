# MatchStream limitations

- One local Redpanda node and one local PostgreSQL instance: no HA, failover,
  backup policy, or disaster-recovery validation.
- Kafka and PostgreSQL are not a distributed transaction. Delivery is at least
  once; durable event IDs prevent duplicate state application after a commit.
- WebSocket delivery is best effort with no durable client queues or horizontal
  API fan-out coordination; HTTP snapshot/history is the recovery path.
- API/dashboard are unauthenticated and intended for local development.
- Scoring supports `Shot` events with outcome `Goal`; own goals, corrections,
  and unusual provider scoring need explicit handling before arbitrary demos.
- API reads open short-lived PostgreSQL connections. Local results identify
  transaction/read cost as a constraint, not a production capacity result.
- The dashboard has no analytics beyond API-supplied state/event counts.
- Benchmarks are local WSL2 measurements, not cloud-scale or browser-rendering
  performance claims.
