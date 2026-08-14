from __future__ import annotations

import pytest

from matchstream.models import CanonicalEvent
from matchstream.replay import MatchReplayer, ReplayConfig
from matchstream.streaming import (
    BrokerConfig,
    BrokerConfigurationError,
    EventDeduplicator,
    RedpandaEventConsumer,
    RedpandaEventProducer,
    deserialize_event,
    match_partition_key,
    publish_replay,
)
from matchstream.streaming.contract import serialize_event


def _event(event_id: str = "statsbomb:match-1:event-1", sequence: int = 1) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=event_id,
        match_id="match-1",
        sequence=sequence,
        period=1,
        match_clock_seconds=float(sequence),
        event_type="Pass",
        team=None,
        source_event_id=event_id.rsplit(":", maxsplit=1)[-1],
    )


class _FakeProducer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes, bytes]] = []
        self.flush_calls: list[float] = []

    def produce(self, topic: str, *, key: bytes, value: bytes, on_delivery: object) -> None:
        self.calls.append((topic, key, value))
        on_delivery(None, None)  # type: ignore[operator]

    def flush(self, timeout: float) -> int:
        self.flush_calls.append(timeout)
        return 0


class _FakeMessage:
    def __init__(self, value: bytes) -> None:
        self._value = value

    def error(self) -> None:
        return None

    def value(self) -> bytes:
        return self._value


class _FakeConsumer:
    def __init__(self, messages: list[_FakeMessage]) -> None:
        self.messages = messages
        self.subscriptions: list[list[str]] = []
        self.committed: list[_FakeMessage] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions.append(topics)

    def poll(self, timeout: float) -> _FakeMessage | None:
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, message: _FakeMessage, asynchronous: bool) -> None:
        assert asynchronous is False
        self.committed.append(message)

    def close(self) -> None:
        self.closed = True


class _CollectingPublisher:
    def __init__(self) -> None:
        self.events: list[CanonicalEvent] = []

    def publish(self, event: CanonicalEvent) -> None:
        self.events.append(event)

    def close(self) -> None:
        pass


def test_broker_config_reads_environment_and_validates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MATCHSTREAM_BROKER_ADDRESS", "redpanda:9092")
    monkeypatch.setenv("MATCHSTREAM_TOPIC", "matches.v1")
    monkeypatch.setenv("MATCHSTREAM_CONSUMER_GROUP", "workers")

    assert BrokerConfig.from_environment() == BrokerConfig("redpanda:9092", "matches.v1", "workers")
    with pytest.raises(BrokerConfigurationError, match="bootstrap_servers"):
        BrokerConfig(bootstrap_servers="")


def test_match_key_routes_a_match_consistently() -> None:
    assert match_partition_key(_event("statsbomb:match-1:event-1")) == b"match-1"
    assert match_partition_key(_event("statsbomb:match-1:event-2")) == b"match-1"


def test_producer_serializes_event_and_uses_match_key() -> None:
    client = _FakeProducer()
    producer = RedpandaEventProducer(BrokerConfig(topic="events"), client=client)
    event = _event()

    producer.publish(event)
    producer.close()

    assert client.calls[0][0:2] == ("events", b"match-1")
    assert deserialize_event(client.calls[0][2]) == event
    assert client.flush_calls == [10.0, 10.0]


def test_consumer_deserializes_valid_event_and_commits_after_processing() -> None:
    event = _event()
    client = _FakeConsumer([_FakeMessage(serialize_event(event))])
    consumer = RedpandaEventConsumer(BrokerConfig(topic="events"), client=client)

    received = consumer.poll(0.1)
    consumer.commit()
    consumer.close()

    assert received == event
    assert client.subscriptions == [["events"]]
    assert len(client.committed) == 1
    assert client.closed is True


def test_process_local_deduplicator_uses_event_id() -> None:
    deduplicator = EventDeduplicator()

    assert deduplicator.accept(_event()) is True
    assert deduplicator.accept(_event()) is False
    assert deduplicator.accept(_event("statsbomb:match-1:event-2")) is True
    assert deduplicator.size == 2


def test_publish_replay_depends_only_on_publisher_boundary() -> None:
    publisher = _CollectingPublisher()
    replayer = MatchReplayer([_event("statsbomb:match-1:event-2", 2), _event(sequence=1)])

    publish_replay(replayer, publisher, ReplayConfig(no_wait=True))

    assert [event.sequence for event in publisher.events] == [1, 2]
