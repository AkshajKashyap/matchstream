"""Kafka consumer-group lag inspection kept separate from normal consumption."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from matchstream.observability.logging import get_logger
from matchstream.observability.metrics import MatchStreamMetrics, metrics

from .redpanda import BrokerConfig, BrokerTransportError, _consumer_client, _kafka_module

logger = get_logger("lag")


@dataclass(frozen=True, slots=True)
class PartitionLag:
    """Actual offsets for one topic partition at one point in time."""

    partition: int
    committed_offset: int | None
    current_position: int | None
    end_offset: int
    lag: int


def lag_from_offsets(committed_offset: int | None, beginning_offset: int, end_offset: int) -> int:
    """Calculate backlog from a group commit, treating no commit as the beginning."""

    progress = beginning_offset if committed_offset is None else committed_offset
    return max(end_offset - progress, 0)


def inspect_consumer_lag(
    config: BrokerConfig,
    client: Any | None = None,
    observer: MatchStreamMetrics | None = None,
) -> list[PartitionLag]:
    """Query Kafka metadata, committed group offsets, watermarks, and positions.

    The one-shot diagnostic does not poll records or change source commits. A
    position is usually unavailable for an idle external consumer group; that is
    reported as ``None`` rather than fabricated from a commit.
    """

    kafka = _kafka_module()
    consumer = client if client is not None else _consumer_client(config)
    try:
        metadata = consumer.list_topics(config.topic, timeout=2.0)
        topic = metadata.topics.get(config.topic)
        if topic is None or getattr(topic, "error", None) is not None:
            raise BrokerTransportError(f"topic {config.topic!r} is unavailable for lag inspection")
        partitions = [kafka.TopicPartition(config.topic, number) for number in sorted(topic.partitions)]
        committed = consumer.committed(partitions, timeout=2.0)
        try:
            positions = consumer.position(partitions)
        except Exception:  # noqa: BLE001 - position is optional for an idle group
            logger.debug("consumer_position_unavailable", extra={"component": "lag", "operation": "position"})
            positions = []
        position_by_partition = {
            item.partition: item.offset for item in positions if getattr(item, "offset", -1) >= 0
        }
        rows: list[PartitionLag] = []
        for item in committed:
            beginning, end = consumer.get_watermark_offsets(item, timeout=2.0)
            committed_offset = item.offset if item.offset >= 0 else None
            rows.append(
                PartitionLag(
                    partition=item.partition,
                    committed_offset=committed_offset,
                    current_position=position_by_partition.get(item.partition),
                    end_offset=end,
                    lag=lag_from_offsets(committed_offset, beginning, end),
                )
            )
        (observer or metrics()).consumer_lag.set(sum(row.lag for row in rows))
        return rows
    except BrokerTransportError:
        raise
    except Exception as error:
        raise BrokerTransportError(f"could not inspect consumer lag: {error}") from error
    finally:
        if client is None:
            try:
                consumer.close()
            except Exception:  # noqa: BLE001 - close cannot obscure a lag inspection error
                logger.warning("lag_consumer_close_failed", extra={"component": "lag", "operation": "close"})
