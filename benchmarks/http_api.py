"""Benchmark the three public HTTP read endpoints against a running API process."""

from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path

import httpx

from benchmarks.common import environment, latency_summary, write_json


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--match-id", required=True)
    parser.add_argument("--requests", type=int, default=60, help="requests per endpoint/concurrency level")
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 4, 8])
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.requests < 1 or any(value < 1 for value in arguments.concurrency):
        parser.error("requests and each concurrency value must be positive")
    results = asyncio.run(_run(arguments))
    write_json(arguments.output, results)
    print(f"wrote {arguments.output}")


async def _run(arguments: argparse.Namespace) -> dict[str, object]:
    base_url = arguments.api_base_url.rstrip("/")
    endpoints = {
        "match_list": "/api/v1/matches?limit=50",
        "match_state": f"/api/v1/matches/{arguments.match_id}",
        "event_history": f"/api/v1/matches/{arguments.match_id}/events?after_sequence=0&limit=50",
    }
    rows: list[dict[str, object]] = []
    for concurrency in arguments.concurrency:
        for name, path in endpoints.items():
            rows.append(await _exercise(base_url, name, path, arguments.requests, concurrency))
    return {
        "kind": "http_api",
        "label": "local development benchmark",
        "method": "async HTTP clients record wall-clock request latency at bounded local concurrency",
        "environment": environment(),
        "requests_per_endpoint": arguments.requests,
        "rows": rows,
    }


async def _exercise(
    base_url: str, name: str, path: str, request_count: int, concurrency: int
) -> dict[str, object]:
    semaphore = asyncio.Semaphore(concurrency)
    latencies_ms: list[float] = []
    failures = 0
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(base_url=base_url, limits=limits, timeout=10) as client:
        async def request_once() -> None:
            nonlocal failures
            async with semaphore:
                started = time.perf_counter()
                try:
                    response = await client.get(path)
                    if response.status_code != 200:
                        failures += 1
                except httpx.HTTPError:
                    failures += 1
                finally:
                    latencies_ms.append((time.perf_counter() - started) * 1000)

        started = time.perf_counter()
        await asyncio.gather(*(request_once() for _ in range(request_count)))
    elapsed = time.perf_counter() - started
    return {
        "endpoint": name,
        "concurrency": concurrency,
        "requests": request_count,
        "errors": failures,
        "error_rate": round(failures / request_count, 5),
        "requests_per_second": round(request_count / elapsed, 3),
        "latency": latency_summary(latencies_ms),
    }


if __name__ == "__main__":
    main()
