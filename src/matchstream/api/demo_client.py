"""Minimal command-line WebSocket observer for the MatchStream API."""

from __future__ import annotations

import argparse
import asyncio
import json

import websockets


async def observe(url: str, messages: int) -> None:
    """Print the requested number of API WebSocket messages without a GUI client."""

    async with websockets.connect(url) as socket:
        for _ in range(messages):
            print(json.dumps(json.loads(await socket.recv()), sort_keys=True))


def main() -> None:
    """Run the small operator/demo WebSocket client."""

    parser = argparse.ArgumentParser(description="Print MatchStream WebSocket state messages")
    parser.add_argument("url", help="for example ws://127.0.0.1:8000/api/v1/matches/demo/stream")
    parser.add_argument("--messages", type=int, default=5, help="number of messages to print")
    arguments = parser.parse_args()
    if arguments.messages < 1:
        parser.error("--messages must be positive")
    asyncio.run(observe(arguments.url, arguments.messages))


if __name__ == "__main__":
    main()
