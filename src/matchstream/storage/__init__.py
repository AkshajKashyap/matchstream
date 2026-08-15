"""Durable storage implementations and serialization boundaries."""

from .postgres import (
    DatabaseConfig,
    DatabaseConfigurationError,
    DurableStorageError,
    PostgresProjectionRepository,
)
from .repository import DurableProjectionResult, ProjectionRepository, Transition
from .serialization import (
    STATE_SCHEMA_VERSION,
    StateSerializationError,
    state_from_dict,
    state_to_dict,
)

__all__ = [
    "STATE_SCHEMA_VERSION",
    "DatabaseConfig",
    "DatabaseConfigurationError",
    "DurableProjectionResult",
    "DurableStorageError",
    "PostgresProjectionRepository",
    "ProjectionRepository",
    "StateSerializationError",
    "Transition",
    "state_from_dict",
    "state_to_dict",
]
