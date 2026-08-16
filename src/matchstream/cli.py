"""Small package-level commands alongside the operational streaming CLI."""

from __future__ import annotations

import json
import sys

from . import __version__


def main() -> None:
    """Print stable package metadata or delegate operational commands unchanged."""

    arguments = sys.argv[1:]
    if arguments == ["--version"]:
        print(__version__)
        return
    if arguments == ["project-info"]:
        print(
            json.dumps(
                {
                    "name": "MatchStream",
                    "version": __version__,
                    "description": "Event-driven historical football replay platform",
                    "documentation": "docs/system-design.md",
                },
                sort_keys=True,
            )
        )
        return
    from .streaming.cli import main as streaming_main

    streaming_main()
