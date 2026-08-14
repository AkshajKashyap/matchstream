"""Kafka-compatible Redpanda adapter for the MatchStream wire contract."""

from __future__ import annotations

import os
from dataclasses import dataclass
from math import isfinite
from typing import Any, Self

from matchstream.models import CanonicalEvent

from .contract import EventContractError, deserialize_event, serialize_event


class BrokerConfigurationError(ValueError):
    """Raised when a broker setting is unusable."""


class BrokerTransportError(RuntimeError):
    """Raised for client creation, delivery, polling, or shutdown failures."""


class BrokerMessageError(BrokerTransportError):
    """Raised when a broker message cannot become a validated canonical event."""


@dataclass(frozen=True, slots=True)
class BrokerConfig:
    """Connection and topic settings for a Kafka-compatible broker."""

    bootstrap_servers: str = "localhost:19092"
    topic: str = "matchstream.events.v1"
    group_id: str = "matchstream-demo"
    delivery_timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        for field_name, value in (
            ("bootstrap_servers", self.bootstrap_servers),
            ("topic", self.topic),
            ("group_id", self.group_id),
        ):
            if not isinstance(value, str) or not value.strip():
                raise BrokerConfigurationError(f"{field_name} must be a non-empty string")
        if (
            isinstance(self.delivery_timeout_seconds, bool)
            or not isinstance(self.delivery_timeout_seconds, (int, float))
            or not isfinite(self.delivery_timeout_seconds)
            or self.delivery_timeout_seconds <= 0
        ):
            raise BrokerConfigurationError("delivery_timeout_seconds must be a finite number greater than zero")

    @classmethod
    def from_environment(cls) -> BrokerConfig:
        """Build configuration from documented environment variables."""

        return cls(
            bootstrap_servers=os.environ.get("MATCHSTREAM_BROKER_ADDRESS", "localhost:19092"),
            topic=os.environ.get("MATCHSTREAM_TOPIC", "matchstream.events.v1"),
            group_id=os.environ.get("MATCHSTREAM_CONSUMER_GROUP", "matchstream-demo"),
        )


def match_partition_key(event: CanonicalEvent) -> bytes:
    """Use match ID as the Kafka key so a match stays on one partition."""

    return event.match_id.encode("utf-8")


class RedpandaEventProducer:
    """Publish canonical events through the versioned MatchStream contract."""

    def __init__(self, config: BrokerConfig, client: Any | None = None) -> None:
        self._config = config
        self._client = client if client is not None else _producer_client(config)
        self._closed = False

    def publish(self, event: CanonicalEvent) -> None:
        if self._closed:
            raise BrokerTransportError("producer is closed")
        delivery_errors: list[object] = []

        def on_delivery(error: object, _message: object) -> None:
            if error is not None:
                delivery_errors.append(error)

        try:
            self._client.produce(
                self._config.topic,
                key=match_partition_key(event),
                value=serialize_event(event),
                on_delivery=on_delivery,
            )
            queued_messages = self._client.flush(self._config.delivery_timeout_seconds)
        except Exception as error:
            raise BrokerTransportError(f"failed to publish event {event.event_id}: {error}") from error
        if delivery_errors:
            raise BrokerTransportError(
                f"broker did not deliver event {event.event_id}: {delivery_errors[0]}"
            )
        if queued_messages:
            raise BrokerTransportError(
                f"broker did not deliver event {event.event_id} before the delivery timeout"
            )

    def close(self) -> None:
        if self._closed:
            return
        try:
            queued_messages = self._client.flush(self._config.delivery_timeout_seconds)
        except Exception as error:
            raise BrokerTransportError(f"failed to close producer: {error}") from error
        self._closed = True
        if queued_messages:
            raise BrokerTransportError("producer closed with undelivered messages")

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        try:
            self.close()
        except BrokerTransportError:
            if exc_type is None:
                raise


class RedpandaEventConsumer:
    """Consume broker messages as validated canonical events."""

    def __init__(self, config: BrokerConfig, client: Any | None = None) -> None:
        self._client = client if client is not None else _consumer_client(config)
        self._client.subscribe([config.topic])
        self._closed = False
        self._last_message: Any | None = None

    def poll(self, timeout_seconds: float) -> CanonicalEvent | None:
        if self._closed:
            raise BrokerTransportError("consumer is closed")
        try:
            message = self._client.poll(timeout_seconds)
        except Exception as error:
            raise BrokerTransportError(f"failed to poll broker: {error}") from error
        if message is None:
            return None
        if message.error() is not None:
            if _is_partition_eof(message.error()):
                return None
            raise BrokerTransportError(f"broker returned a message error: {message.error()}")
        try:
            event = deserialize_event(message.value())
        except EventContractError as error:
            raise BrokerMessageError(f"invalid event message from broker: {error}") from error
        self._last_message = message
        return event

    def commit(self) -> None:
        """Synchronously commit the most recently returned valid message."""

        if self._last_message is None:
            raise BrokerTransportError("cannot commit before consuming a valid message")
        try:
            self._client.commit(message=self._last_message, asynchronous=False)
        except Exception as error:
            raise BrokerTransportError(f"failed to commit consumer offset: {error}") from error

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._client.close()
        except Exception as error:
            raise BrokerTransportError(f"failed to close consumer: {error}") from error
        self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        try:
            self.close()
        except BrokerTransportError:
            if exc_type is None:
                raise


def _producer_client(config: BrokerConfig) -> Any:
    kafka = _kafka_module()
    try:
        return kafka.Producer({"bootstrap.servers": config.bootstrap_servers})
    except Exception as error:
        raise BrokerTransportError(f"could not create producer: {error}") from error


def _consumer_client(config: BrokerConfig) -> Any:
    kafka = _kafka_module()
    try:
        return kafka.Consumer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "group.id": config.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
    except Exception as error:
        raise BrokerTransportError(f"could not create consumer: {error}") from error


def _kafka_module() -> Any:
    try:
        import confluent_kafka
    except ImportError as error:
        raise BrokerTransportError(
            "confluent-kafka is required for Redpanda transport; install MatchStream dependencies"
        ) from error
    return confluent_kafka


def _is_partition_eof(error: object) -> bool:
    try:
        from confluent_kafka import KafkaError
    except ImportError:
        return False
    return getattr(error, "code", lambda: None)() == KafkaError._PARTITION_EOF
