"""Kafka-compatible adapter for publishing and inspecting dead-letter records."""

from __future__ import annotations

from typing import Any, Self

from .dlq import DeadLetterEvent, deserialize_dead_letter, serialize_dead_letter
from .redpanda import (
    BrokerConfig,
    BrokerTransportError,
    _consumer_client,
    _is_retriable_poll_error,
    _producer_client,
)


class RedpandaDeadLetterProducer:
    """Publish versioned dead-letter records with deterministic record-ID keys."""

    def __init__(self, config: BrokerConfig, client: Any | None = None) -> None:
        self._config = config
        self._client = client if client is not None else _producer_client(config)
        self._closed = False

    def publish(self, record: DeadLetterEvent) -> None:
        if self._closed:
            raise BrokerTransportError("dead-letter producer is closed")
        errors: list[object] = []

        def on_delivery(error: object, _message: object) -> None:
            if error is not None:
                errors.append(error)

        try:
            self._client.produce(
                self._config.topic,
                key=record.record_id.encode("utf-8"),
                value=serialize_dead_letter(record),
                on_delivery=on_delivery,
            )
            queued = self._client.flush(self._config.delivery_timeout_seconds)
        except Exception as error:
            raise BrokerTransportError(f"failed to publish dead-letter record {record.record_id}: {error}") from error
        if errors or queued:
            detail = errors[0] if errors else "delivery timeout"
            raise BrokerTransportError(f"dead-letter delivery failed for {record.record_id}: {detail}")

    def close(self) -> None:
        if self._closed:
            return
        try:
            queued = self._client.flush(self._config.delivery_timeout_seconds)
        except Exception as error:
            raise BrokerTransportError(f"failed to close dead-letter producer: {error}") from error
        self._closed = True
        if queued:
            raise BrokerTransportError("dead-letter producer closed with undelivered records")

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        try:
            self.close()
        except BrokerTransportError:
            if exc_type is None:
                raise


class RedpandaDeadLetterConsumer:
    """Consume and validate dead-letter records for operational inspection."""

    def __init__(self, config: BrokerConfig, client: Any | None = None) -> None:
        self._client = client if client is not None else _consumer_client(config)
        self._client.subscribe([config.topic])
        self._closed = False
        self._last_message: Any | None = None

    def poll(self, timeout_seconds: float) -> DeadLetterEvent | None:
        if self._closed:
            raise BrokerTransportError("dead-letter consumer is closed")
        message = self._client.poll(timeout_seconds)
        if message is None or (message.error() is not None and _is_retriable_poll_error(message.error())):
            return None
        if message.error() is not None:
            raise BrokerTransportError(f"broker returned a dead-letter message error: {message.error()}")
        try:
            record = deserialize_dead_letter(message.value())
        except ValueError as error:
            raise BrokerTransportError(f"invalid dead-letter message: {error}") from error
        self._last_message = message
        return record

    def commit(self) -> None:
        if self._last_message is None:
            raise BrokerTransportError("cannot commit before consuming a dead-letter record")
        self._client.commit(message=self._last_message, asynchronous=False)

    def close(self) -> None:
        if not self._closed:
            self._client.close()
            self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()
