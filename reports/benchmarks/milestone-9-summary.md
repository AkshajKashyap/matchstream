# Milestone 9 local-development benchmark summary

## Method

All measurements used the local Compose stack, not production or cloud
infrastructure. A 20-event warm-up preceded three measured 80-event stream
trials. Events were valid synthetic canonical events with unique IDs and
contiguous sequences; they are not representative football traffic. HTTP used
40 requests per endpoint/concurrency level. WebSocket rows measure direct
PostgreSQL durable commit to FastAPI receipt, intentionally separate from
broker-to-database timing. Raw JSON results are adjacent to this report.

## Environment

- WSL2 Linux `6.18.33.2-microsoft-standard-WSL2`, 12 logical CPUs
- Intel Core i7-1355U; 15 GiB RAM; Python 3.13.11; Node 20.18.2; Docker 29.7.2
- PostgreSQL 17.11; Redpanda 26.2.1

## Results

| Workload | Observed result | Errors |
| --- | --- | --- |
| Durable stream processing | 37.9–44.9 events/s across 3 × 80-event trials | 0 |
| Publish-to-durable latency | p50 972.6–1134.8 ms; p95 1695.1–2013.4 ms | 0 |
| PostgreSQL transaction metric | mean 18.4–22.4 ms/event | 0 |
| HTTP current state, concurrency 1 | p50 11.4 ms; p95 13.3 ms; 86.3 req/s | 0 |
| HTTP history, concurrency 8 | p50 200.9 ms; p95 208.2 ms; 39.4 req/s | 0 |
| WebSocket fan-out | all 1, 4, and 8 updates delivered; p50 24.1–29.1 ms | 0 |
| Lag burst then drain | peak 80; zero in 1.711 s; 46.7 events/s | 0 |

Publish-to-durable latency includes intentional queue dwell because the burst is
fully published before one consumer drains it. It is not per-event transaction
latency. The honest end-to-end segments are broker/burst-to-durable above and
durable-commit-to-WebSocket receipt (24.1–29.1 ms); no combined precise latency
is claimed.

## Bottleneck and limitations

Throughput varied roughly 18% across trials. PostgreSQL transaction time is the
clear local constraint: 18.4–22.4 ms transaction mean versus 20.1–24.2 ms total
processing mean. No optimization was made: this is a baseline, and changing the
one-event durable boundary needs a separately scoped correctness experiment.

## Reproduce

```bash
docker compose up -d
make db-init
python -m benchmarks.stream_processing --events 80 --warmup-events 20 --trials 3 \
  --output reports/benchmarks/stream-processing.json
python -m benchmarks.lag_stress --events 80 --output reports/benchmarks/lag-stress.json
make api
python -m benchmarks.http_api --match-id <durable-match-id> \
  --output reports/benchmarks/http-api.json
python -m benchmarks.websocket_fanout --output reports/benchmarks/websocket-fanout.json
```
