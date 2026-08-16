"""Structured application logging without a third-party logging dependency."""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

_STANDARD_FIELDS = set(logging.makeLogRecord({}).__dict__)


class JsonFormatter(logging.Formatter):
    """Render log records as one JSON object per line with explicit extras."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_FIELDS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, sort_keys=True)


def configure_logging(level: str | None = None) -> None:
    """Configure MatchStream's root logger for machine-readable output.

    Callers may override ``MATCHSTREAM_LOG_LEVEL``; configuration is deliberately
    explicit so importing a library module never changes a host application's logs.
    """

    resolved_level = (level or os.environ.get("MATCHSTREAM_LOG_LEVEL", "INFO")).upper()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("matchstream")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(resolved_level)
    logger.propagate = False


def get_logger(component: str) -> logging.Logger:
    """Return a component logger; callers supply event-specific context as ``extra``."""

    return logging.getLogger(f"matchstream.{component}")
