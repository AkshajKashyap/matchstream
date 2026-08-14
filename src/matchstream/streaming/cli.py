"""Local CLI flows for publishing and consuming MatchStream events via Redpanda."""

from __future__ import annotations

import argparse
from pathlib import Path

from matchstream.ingestion import load_statsbomb_events
from matchstream.replay import MatchReplayer, ReplayConfig

from .deduplication import EventDeduplicator
from .redpanda import BrokerConfig, RedpandaEventConsumer, RedpandaEventProducer
from .replay import publish_replay


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish or consume MatchStream events via Redpanda")
    subcommands = parser.add_subparsers(dest="command", required=True)
    _add_produce_arguments(subcommands.add_parser("produce", help="replay a StatsBomb file to Redpanda"))
    _add_consume_arguments(subcommands.add_parser("consume", help="print validated Redpanda events"))
    arguments = parser.parse_args()

    config = BrokerConfig(
        bootstrap_servers=arguments.bootstrap_servers,
        topic=arguments.topic,
        group_id=getattr(arguments, "group_id", BrokerConfig().group_id),
    )
    if arguments.command == "produce":
        _produce(arguments, config)
    else:
        _consume(arguments, config)


def _add_common_broker_arguments(parser: argparse.ArgumentParser) -> None:
    defaults = BrokerConfig.from_environment()
    parser.add_argument("--bootstrap-servers", default=defaults.bootstrap_servers)
    parser.add_argument("--topic", default=defaults.topic)


def _add_produce_arguments(parser: argparse.ArgumentParser) -> None:
    _add_common_broker_arguments(parser)
    parser.add_argument("input", type=Path, help="StatsBomb Open Data event JSON file")
    parser.add_argument("--match-id", required=True)
    parser.add_argument("--speed", type=float, default=1.0)
    parser.add_argument("--no-wait", action="store_true")


def _add_consume_arguments(parser: argparse.ArgumentParser) -> None:
    _add_common_broker_arguments(parser)
    parser.add_argument("--group-id", default=BrokerConfig.from_environment().group_id)
    parser.add_argument("--max-events", type=int, default=0, help="stop after this many messages; 0 waits forever")
    parser.add_argument("--poll-timeout", type=float, default=1.0)


def _produce(arguments: argparse.Namespace, config: BrokerConfig) -> None:
    events = load_statsbomb_events(arguments.input, arguments.match_id)
    replayer = MatchReplayer(events)
    replay_config = ReplayConfig(speed=arguments.speed, no_wait=arguments.no_wait)
    with RedpandaEventProducer(config) as producer:
        publish_replay(replayer, producer, replay_config)
    print(f"published {len(events)} events to {config.topic}")


def _consume(arguments: argparse.Namespace, config: BrokerConfig) -> None:
    deduplicator = EventDeduplicator()
    consumed = 0
    with RedpandaEventConsumer(config) as consumer:
        while arguments.max_events == 0 or consumed < arguments.max_events:
            event = consumer.poll(arguments.poll_timeout)
            if event is None:
                continue
            if deduplicator.accept(event):
                print(
                    f"{event.match_id} sequence={event.sequence} "
                    f"type={event.event_type} id={event.event_id}"
                )
            else:
                print(f"duplicate ignored id={event.event_id}")
            consumer.commit()
            consumed += 1


if __name__ == "__main__":
    main()
