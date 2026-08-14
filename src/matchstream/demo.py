"""Small command-line demonstration of canonical conversion and replay."""

from __future__ import annotations

import argparse
from pathlib import Path

from matchstream.ingestion import load_statsbomb_events
from matchstream.replay import MatchReplayer, ReplayConfig


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay a StatsBomb event file without waiting")
    parser.add_argument("input", type=Path, help="StatsBomb Open Data event JSON file")
    parser.add_argument("--match-id", required=True, help="Match identifier to attach to canonical events")
    arguments = parser.parse_args()

    events = load_statsbomb_events(arguments.input, arguments.match_id)
    for event in MatchReplayer(events).iter_events(ReplayConfig(no_wait=True)):
        print(
            f"{event.period}:{event.match_clock_seconds:06.3f} "
            f"sequence={event.sequence} type={event.event_type} id={event.event_id}"
        )


if __name__ == "__main__":
    main()
