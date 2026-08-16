# Milestone 9 plan: reliability, benchmarks, and portfolio release

## Scope and non-goals

This milestone proves and documents the existing event-driven system. It does
not add product features, authentication, cloud infrastructure, Redis, or
advanced football analytics. Production behavior will not be optimized unless
a repeatable baseline identifies a specific bottleneck.

## Benchmark workloads and methodology

1. **Durable event processing:** generate valid, clearly labelled synthetic
   canonical events; publish them through Redpanda and consume them with the
   existing durable projector into PostgreSQL. Record a warm-up plus multiple
   trials: published/processed counts, elapsed time, observed throughput,
   processor and PostgreSQL histogram summaries, consumer lag, and drain time.
2. **HTTP reads:** use a small asynchronous Python client against a running
   FastAPI process. Exercise match list, durable snapshot, and event-history
   reads at modest increasing local concurrency. Record count, errors,
   requests/sec, and percentile latency from raw samples.
3. **WebSocket fan-out:** connect a modest number of clients to the actual API
   stream, trigger durable updates, and record connected/delivered/error counts
   plus receipt latency measured from the client-side trigger timestamp. This
   is reported separately from durable processing latency.
4. **Lag stress:** publish a bounded burst while its durable consumer is held
   back, inspect actual broker lag, start the consumer, and record peak lag,
   drain rate, and time to zero.

All results are labelled *local development benchmarks*. Trial inputs, raw JSON
results, host/runtime versions, warm-up, concurrency, and limitations will be
stored under `reports/benchmarks/`; precision will reflect sample sizes.

## Failure scenarios

Execute and record PostgreSQL outage, Redpanda outage, consumer restart,
poison-event/DLQ, API restart, and WebSocket reconnect cases. Each report row
will distinguish expected behavior from observed behavior. A scenario is never
marked passed unless it was run in this environment.

## Real-match and visual validation

Find one small-to-moderate StatsBomb Open Data event file with ordinary goals,
obtain it reproducibly without committing a large dataset, replay it through
the entire stack, and compare the final durable score with source match data.
Use it for a genuine dashboard inspection and screenshots only when a local
browser can capture them. The tiny test fixture remains unchanged.

## Environment and success criteria

Capture OS/WSL context, CPU/RAM, Python, Node, Docker, Redpanda, and PostgreSQL
versions. Success means reproducible tools and observed reports, green existing
tests, a buildable frontend, factual portfolio documentation, and a versioned
release. Missing local capabilities (for example browser capture) will be
recorded honestly rather than simulated.
