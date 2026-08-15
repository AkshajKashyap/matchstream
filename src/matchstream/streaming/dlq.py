"""Versioned dead-letter records independent of Kafka client details."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from matchstream.models import CanonicalEvent

from .contract import EventContractError, deserialize_event, serialize_event

DLQ_SCHEMA = "matchstream.dead-letter-event"
DLQ_SCHEMA_VERSION = 1


class DeadLetterError(ValueError):
    """Raised when a dead-letter record cannot be serialized or validated."""


class FailureClassification(StrEnum):
    """Whether a failure may be quarantined after bounded attempts."""

    POISON = "poison"
    RETRIABLE = "retriable"


@dataclass(frozen=True, slots=True)
class SourcePosition:
    """Kafka-compatible location of the normal-topic record being handled."""

    topic: str
    partition: int
    offset: int

    def __post_init__(self) -> None:
        if not self.topic.strip():
            raise DeadLetterError("source topic must be non-empty")
        if self.partition < 0 or self.offset < 0:
            raise DeadLetterError("source partition and offset must be non-negative")


@dataclass(frozen=True, slots=True)
class DeadLetterEvent:
    """Inspectable quarantine record for one failed canonical event delivery."""

    record_id: str
    original_event: CanonicalEvent
    source: SourcePosition
    classification: FailureClassification
    error_type: str
    error_message: str
    attempt_count: int
    created_at: str

    def __post_init__(self) -> None:
        if self.record_id != dead_letter_id(self.original_event, self.source):
            raise DeadLetterError("record_id must match the deterministic source/event identity")
        if not self.error_type.strip() or not self.error_message.strip():
            raise DeadLetterError("dead-letter error type and message must be non-empty")
        if self.attempt_count < 1:
            raise DeadLetterError("attempt_count must be positive")
        try:
            datetime.fromisoformat(self.created_at)
        except ValueError as error:
            raise DeadLetterError("created_at must be an ISO-8601 timestamp") from error

    @classmethod
    def from_failure(
        cls,
        event: CanonicalEvent,
        source: SourcePosition,
        classification: FailureClassification,
        error: Exception,
        attempt_count: int,
        created_at: datetime | None = None,
    ) -> DeadLetterEvent:
        """Build a compact record without storing an exception traceback."""

        timestamp = created_at or datetime.now(UTC)
        return cls(
            record_id=dead_letter_id(event, source),
            original_event=event,
            source=source,
            classification=classification,
            error_type=type(error).__name__,
            error_message=str(error)[:500] or type(error).__name__,
            attempt_count=attempt_count,
            created_at=timestamp.isoformat(),
        )


def dead_letter_id(event: CanonicalEvent, source: SourcePosition) -> str:
    """Return a stable ID for a source record's quarantine representation."""

    material = f"{source.topic}:{source.partition}:{source.offset}:{event.event_id}".encode()
    return f"dlq:{hashlib.sha256(material).hexdigest()}"


def serialize_dead_letter(record: DeadLetterEvent) -> bytes:
    """Serialize a dead-letter record as deterministic UTF-8 JSON."""

    original_envelope = json.loads(serialize_event(record.original_event))
    payload = {
        "schema": DLQ_SCHEMA,
        "schema_version": DLQ_SCHEMA_VERSION,
        "record": {
            "record_id": record.record_id,
            "original_event": original_envelope,
            "original_event_id": record.original_event.event_id,
            "match_id": record.original_event.match_id,
            "source_topic": record.source.topic,
            "source_partition": record.source.partition,
            "source_offset": record.source.offset,
            "classification": record.classification.value,
            "error_type": record.error_type,
            "error_message": record.error_message,
            "attempt_count": record.attempt_count,
            "created_at": record.created_at,
        },
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def deserialize_dead_letter(payload: bytes | str) -> DeadLetterEvent:
    """Deserialize and validate a dead-letter record."""

    try:
        text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        raw = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DeadLetterError(f"dead-letter payload is invalid JSON: {error}") from error
    if not isinstance(raw, dict) or raw.get("schema") != DLQ_SCHEMA:
        raise DeadLetterError("unsupported dead-letter schema")
    if raw.get("schema_version") != DLQ_SCHEMA_VERSION:
        raise DeadLetterError("unsupported dead-letter schema version")
    record = raw.get("record")
    if not isinstance(record, dict):
        raise DeadLetterError("dead-letter record must be an object")
    try:
        event = deserialize_event(json.dumps(record["original_event"]))
        return DeadLetterEvent(
            record_id=_string(record.get("record_id"), "record_id"),
            original_event=event,
            source=SourcePosition(
                topic=_string(record.get("source_topic"), "source_topic"),
                partition=_integer(record.get("source_partition"), "source_partition"),
                offset=_integer(record.get("source_offset"), "source_offset"),
            ),
            classification=FailureClassification(record.get("classification")),
            error_type=_string(record.get("error_type"), "error_type"),
            error_message=_string(record.get("error_message"), "error_message"),
            attempt_count=_integer(record.get("attempt_count"), "attempt_count"),
            created_at=_string(record.get("created_at"), "created_at"),
        )
    except (KeyError, TypeError, ValueError, EventContractError) as error:
        raise DeadLetterError(f"invalid dead-letter record: {error}") from error


def _string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeadLetterError(f"{field_name} must be a non-empty string")
    return value


def _integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DeadLetterError(f"{field_name} must be an integer")
    return value
