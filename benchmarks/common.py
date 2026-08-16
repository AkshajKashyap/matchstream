"""Shared helpers for MatchStream's deliberately small benchmark scripts."""

from __future__ import annotations

import json
import math
import platform
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def percentile(samples_ms: list[float], percentile_value: float) -> float | None:
    """Return an interpolated percentile, or ``None`` when there are no samples."""

    if not samples_ms:
        return None
    ordered = sorted(samples_ms)
    position = (len(ordered) - 1) * percentile_value
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def latency_summary(samples_ms: list[float]) -> dict[str, float | int | None]:
    """Summarize observed latency samples without pretending sparse data is precise."""

    return {
        "sample_count": len(samples_ms),
        "p50_ms": _rounded(percentile(samples_ms, 0.50)),
        "p95_ms": _rounded(percentile(samples_ms, 0.95)),
        "p99_ms": _rounded(percentile(samples_ms, 0.99)) if len(samples_ms) >= 100 else None,
    }


def environment() -> dict[str, str | None]:
    """Capture available host/runtime information for local-result interpretation."""

    return {
        "captured_at_utc": datetime.now(UTC).isoformat(),
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "cpu": _command_output(["lscpu", "--parse=MODELNAME"]),
        "memory": _command_output(["free", "-h"]),
        "docker": _command_output(["docker", "--version"]),
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write a stable, human-inspectable machine-readable benchmark result."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _command_output(command: list[str]) -> str | None:
    try:
        result = subprocess.run(command, capture_output=True, check=False, text=True, timeout=3)
    except (OSError, subprocess.TimeoutExpired):
        return None
    output = (result.stdout or result.stderr).strip().replace("\n", " | ")
    return output[:500] or None


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 3)
