# Milestone 9: reliability, benchmarks, and release

## Real demo

The selected portfolio match is **France 4–3 Argentina**, 2018 FIFA World Cup
round of 16, StatsBomb Open Data match `7580`. The public event source is
[StatsBomb Open Data](https://github.com/statsbomb/open-data); the 3,549-event
file is deliberately untracked:

```bash
make real-demo-download
```

The local full-system run used:

```bash
# terminal 1
docker compose up -d && make db-init

# terminal 2
python -m matchstream.streaming.cli resilient-project \
  --topic matchstream.demo --dlq-topic matchstream.demo.dlq --group-id matchstream-demo

# terminal 3
make api

# terminal 4
make frontend-install && make frontend-dev

# terminal 5
python -m matchstream.streaming.cli produce --topic matchstream.demo \
  --match-id statsbomb-2018-france-argentina --speed 120 \
  data/raw/statsbomb-2018-france-argentina-7580.json
```

The observed run projected all 3,549 events and stored France 4–3 Argentina,
matching the source result. The match was chosen because all seven goals are
normal supported `Shot`/`Goal` events; the 2018 final was rejected because its
own-goal semantics are outside the current scoring boundary.

## Benchmarks and reliability

The small scripts in `benchmarks/` generate raw JSON under
`reports/benchmarks/`; they use valid synthetic canonical events only where a
controlled load is required. See [benchmark results](../reports/benchmarks/milestone-9-summary.md)
and [reliability results](../reports/reliability/milestone-9.md). They are local
development measurements, not production-scale claims.

## Visual QA and screenshots

Headless Chromium selected the real match from the live dashboard, confirmed
the score display `France 4 – 3 Argentina`, period 2, clock `50:03`, timeline,
technical panel, and `LIVE` status. A 700 px viewport confirmed the single
column layout. The resulting actual desktop captures are:

- [main dashboard](assets/france-argentina-dashboard.png)
- [technical panel](assets/france-argentina-technical.png)

An initial capture exposed fractional-clock floating-point noise; `formatClock`
now intentionally renders elapsed whole seconds and has regression coverage.
The browser-level disconnect/reconnect is not claimed as visually tested; the
API-level reconnect was observed with a real WebSocket client and frontend
logic is tested separately.

## Release checks

```bash
make release-check
make integration
matchstream --version
matchstream project-info
```

This release is `1.0.0`: its documented local system boundary, reliability
behavior, and measured scope are stable enough for a portfolio release, while
the limitations remain explicit.
