"""Fetch the documented StatsBomb Open Data portfolio demo without committing it."""

from __future__ import annotations

import argparse
from pathlib import Path
from urllib.request import urlretrieve

EVENTS_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/7580.json"
DEFAULT_OUTPUT = Path("data/raw/statsbomb-2018-france-argentina-7580.json")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--force", action="store_true", help="replace an existing file")
    arguments = parser.parse_args()
    if arguments.output.exists() and not arguments.force:
        parser.error(f"{arguments.output} already exists; use --force to replace it")
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    urlretrieve(EVENTS_URL, arguments.output)
    print(f"downloaded {EVENTS_URL} to {arguments.output}")


if __name__ == "__main__":
    main()
