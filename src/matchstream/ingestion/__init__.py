"""External data source adapters."""

from .statsbomb import (
    StatsBombIngestionError,
    convert_statsbomb_event,
    convert_statsbomb_events,
    load_statsbomb_events,
)

__all__ = [
    "StatsBombIngestionError",
    "convert_statsbomb_event",
    "convert_statsbomb_events",
    "load_statsbomb_events",
]
