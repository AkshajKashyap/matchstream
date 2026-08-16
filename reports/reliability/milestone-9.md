# Milestone 9 reliability report

All scenarios were executed against local Compose PostgreSQL 17.11 and Redpanda
26.2.1. “Pass” means observed behavior matched the stated boundary, not that
the system has HA or production resilience.

| Failure | Expected | Observed behavior and recovery | Result |
| --- | --- | --- | --- |
| PostgreSQL outage | no durable success/offset commit | After stop, readiness reported PostgreSQL unavailable while Redpanda was reachable. Four records had lag 4 and no commit. After restore, the same group durably processed all four. | Pass |
| Redpanda outage | readiness and producer fail visibly | After stop, readiness reported broker unavailable; publish raised `BrokerTransportError`. Starting Redpanda restored readiness. | Pass |
| Consumer restart | state survives; later events continue | One bounded consumer processed sequences 1–2, then the same group processed 3–4 after restart. State ended at sequence 4. The DB-commit-before-offset redelivery case also passed in `test_postgres_durable.py`. | Pass |
| Poison / DLQ | retry, quarantine, later progress | Real-service `test_poison_dlq.py` passed: valid 1, poison 3, valid 2; one retry/DLQ publication, durable state at 2, deterministic rebuild. | Pass |
| API restart | processing independent; fresh reads | API was stopped while four fixture events projected. After restart, HTTP and a new WebSocket both returned durable sequence 4. | Pass |
| WebSocket reconnect | reconnect snapshot is authoritative | A real client received snapshots before and after API restart. Fan-out benchmark delivered all updates to 1, 4, and 8 clients. | Pass |

Headless-browser visual QA confirmed the real dashboard's desktop and 700 px
single-column states. Browser-level network disconnect/reconnect is not claimed
as visually executed; React reconnect/sequence behavior is covered by frontend
tests and API-level reconnect was observed with a real WebSocket client.
