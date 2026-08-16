"""Runnable Uvicorn entry point for the independent MatchStream API process."""

from __future__ import annotations

import os

import uvicorn

from matchstream.observability.logging import configure_logging

from .app import create_app


def main() -> None:
    """Run the local API without starting a Kafka consumer."""

    configure_logging()
    uvicorn.run(
        create_app(),
        host=os.environ.get("MATCHSTREAM_API_HOST", "127.0.0.1"),
        port=int(os.environ.get("MATCHSTREAM_API_PORT", "8000")),
        log_config=None,
    )


if __name__ == "__main__":
    main()
