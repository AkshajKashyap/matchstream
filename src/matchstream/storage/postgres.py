"""PostgreSQL implementation of atomic durable match projection."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from matchstream.models import CanonicalEvent
from matchstream.state.models import MatchState

from .repository import DurableProjectionResult, Transition
from .serialization import StateSerializationError, state_from_dict, state_to_dict

DEFAULT_DATABASE_DSN = "postgresql://matchstream:matchstream@localhost:5432/matchstream"


class DatabaseConfigurationError(ValueError):
    """Raised when database settings are unusable."""


class DurableStorageError(RuntimeError):
    """Raised when a durable projection cannot be safely read or written."""


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    """PostgreSQL connection settings for MatchStream's durable projection store."""

    dsn: str = DEFAULT_DATABASE_DSN

    def __post_init__(self) -> None:
        if not isinstance(self.dsn, str) or not self.dsn.strip():
            raise DatabaseConfigurationError("dsn must be a non-empty PostgreSQL connection string")

    @classmethod
    def from_environment(cls) -> DatabaseConfig:
        """Load the development default or ``MATCHSTREAM_DATABASE_URL``."""

        return cls(os.environ.get("MATCHSTREAM_DATABASE_URL", DEFAULT_DATABASE_DSN))


class PostgresProjectionRepository:
    """Persist MatchState snapshots and event identities in PostgreSQL transactions."""

    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config

    def initialize_schema(self) -> None:
        """Create the minimal schema required by a fresh MatchStream database."""

        try:
            with psycopg.connect(self._config.dsn) as connection, connection.cursor() as cursor:
                for statement in _SCHEMA_STATEMENTS:
                    cursor.execute(statement)
        except psycopg.Error as error:
            raise DurableStorageError(f"could not initialize PostgreSQL schema: {error}") from error

    def load_state(self, match_id: str) -> MatchState | None:
        """Load and validate the latest durable snapshot for a match."""

        try:
            with psycopg.connect(self._config.dsn) as connection, connection.cursor() as cursor:
                cursor.execute("SELECT state FROM match_states WHERE match_id = %s", (match_id,))
                row = cursor.fetchone()
        except psycopg.Error as error:
            raise DurableStorageError(f"could not load state for {match_id!r}: {error}") from error
        if row is None:
            return None
        return _decode_state(row[0], match_id)

    def process_event(
        self, event: CanonicalEvent, transition: Transition
    ) -> DurableProjectionResult:
        """Atomically reserve identity, transition state, and persist both results.

        A transaction-scoped advisory lock serializes updates to one match even
        before its first ``match_states`` row exists. The event-ID primary key is
        the durable idempotency boundary.
        """

        try:
            with (
                psycopg.connect(self._config.dsn) as connection,
                connection.transaction(),
                connection.cursor() as cursor,
            ):
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (event.match_id,))
                cursor.execute(
                    """
                            INSERT INTO processed_events (event_id, match_id, sequence)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (event_id) DO NOTHING
                            RETURNING event_id
                            """,
                    (event.event_id, event.match_id, event.sequence),
                )
                inserted = cursor.fetchone() is not None
                cursor.execute(
                    "SELECT state FROM match_states WHERE match_id = %s FOR UPDATE",
                    (event.match_id,),
                )
                row = cursor.fetchone()
                previous = (
                    MatchState.initial(event.match_id)
                    if row is None
                    else _decode_state(row[0], event.match_id)
                )
                if not inserted:
                    _ensure_duplicate_matches_event(cursor, event)
                    if row is None:
                        raise DurableStorageError(
                            f"processed event {event.event_id!r} has no durable match state"
                        )
                    return DurableProjectionResult(state=previous, applied=False)

                next_state = transition(previous, event)
                cursor.execute(
                    """
                            INSERT INTO match_states (match_id, state, latest_sequence, last_event_id)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (match_id) DO UPDATE SET
                                state = EXCLUDED.state,
                                latest_sequence = EXCLUDED.latest_sequence,
                                last_event_id = EXCLUDED.last_event_id,
                                updated_at = CURRENT_TIMESTAMP
                            """,
                    (
                        next_state.match_id,
                        Jsonb(state_to_dict(next_state)),
                        next_state.latest_sequence,
                        next_state.last_event_id,
                    ),
                )
                return DurableProjectionResult(state=next_state, applied=True)
        except (psycopg.Error, StateSerializationError) as error:
            raise DurableStorageError(
                f"could not durably process event {event.event_id!r}: {error}"
            ) from error


def _ensure_duplicate_matches_event(cursor: Any, event: CanonicalEvent) -> None:
    cursor.execute("SELECT match_id FROM processed_events WHERE event_id = %s", (event.event_id,))
    row = cursor.fetchone()
    if row is None or row[0] != event.match_id:
        raise DurableStorageError(f"event identity {event.event_id!r} conflicts with another match")


def _decode_state(value: object, match_id: str) -> MatchState:
    try:
        state = state_from_dict(value)
    except StateSerializationError as error:
        raise DurableStorageError(f"stored state for {match_id!r} is invalid: {error}") from error
    if state.match_id != match_id:
        raise DurableStorageError(f"stored state match_id does not match row {match_id!r}")
    return state


_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS match_states (
        match_id TEXT PRIMARY KEY,
        state JSONB NOT NULL,
        latest_sequence BIGINT,
        last_event_id TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS processed_events (
        event_id TEXT PRIMARY KEY,
        match_id TEXT NOT NULL,
        sequence BIGINT NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    "CREATE INDEX IF NOT EXISTS processed_events_match_id_idx ON processed_events (match_id)",
)
