from __future__ import annotations

from matchstream.streaming.redpanda import _is_retriable_poll_error


class _Error:
    def __init__(self, code: int) -> None:
        self._code = code

    def code(self) -> int:
        return self._code


def test_unknown_topic_is_retriable_while_an_auto_created_topic_is_pending() -> None:
    from confluent_kafka import KafkaError

    assert _is_retriable_poll_error(_Error(KafkaError.UNKNOWN_TOPIC_OR_PART))
    assert _is_retriable_poll_error(_Error(KafkaError._PARTITION_EOF))
    assert _is_retriable_poll_error(_Error(KafkaError._ALL_BROKERS_DOWN)) is False
