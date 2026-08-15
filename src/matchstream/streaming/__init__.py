"""Versioned event delivery contracts and broker adapters."""

from .contract import (
    EventContractError,
    UnsupportedSchemaVersion,
    deserialize_event,
    serialize_event,
)
from .deduplication import EventDeduplicator
from .interfaces import AcknowledgingEventConsumer, EventConsumer, EventPublisher
from .redpanda import (
    BrokerConfig,
    BrokerConfigurationError,
    BrokerMessageError,
    BrokerTransportError,
    RedpandaEventConsumer,
    RedpandaEventProducer,
    match_partition_key,
)
from .replay import publish_replay

__all__ = [
    "AcknowledgingEventConsumer",
    "BrokerConfig",
    "BrokerConfigurationError",
    "BrokerMessageError",
    "BrokerTransportError",
    "EventConsumer",
    "EventContractError",
    "EventDeduplicator",
    "EventPublisher",
    "ProcessedEvent",
    "RedpandaEventConsumer",
    "RedpandaEventProducer",
    "UnsupportedSchemaVersion",
    "deserialize_event",
    "match_partition_key",
    "project_next",
    "publish_replay",
    "serialize_event",
]


def __getattr__(name: str) -> object:
    """Lazily import processing helpers to avoid a state/transport import cycle."""

    if name in {"ProcessedEvent", "QuarantinedEvent", "project_next", "project_next_with_dlq"}:
        from .processing import (
            ProcessedEvent,
            QuarantinedEvent,
            project_next,
            project_next_with_dlq,
        )

        return {
            "ProcessedEvent": ProcessedEvent,
            "QuarantinedEvent": QuarantinedEvent,
            "project_next": project_next,
            "project_next_with_dlq": project_next_with_dlq,
        }[name]
    raise AttributeError(name)
