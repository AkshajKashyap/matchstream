# Contributing

Use Python 3.11+ and a current Node LTS release. Keep provider parsing,
canonical events, transport, projection, API, and frontend concerns separate.
Do not add generated datasets, credentials, or performance claims without raw
evidence.

Before proposing a change:

```bash
make release-check
make integration
```

Integration requires the local Compose services. Benchmark changes must retain
raw machine-readable results and label local measurements honestly.
