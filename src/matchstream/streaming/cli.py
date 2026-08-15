"""Local CLI flows for publishing and consuming MatchStream events via Redpanda."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from matchstream.ingestion import load_statsbomb_events
from matchstream.replay import MatchReplayer, ReplayConfig
from matchstream.state import DurableMatchProjector, MatchProjector, MatchState
from matchstream.storage import DatabaseConfig, PostgresProjectionRepository, state_to_dict

from .deduplication import EventDeduplicator
from .processing import project_next
from .redpanda import BrokerConfig, RedpandaEventConsumer, RedpandaEventProducer
from .replay import publish_replay


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish or consume MatchStream events via Redpanda")
    subcommands = parser.add_subparsers(dest="command", required=True)
    _add_produce_arguments(subcommands.add_parser("produce", help="replay a StatsBomb file to Redpanda"))
    _add_consume_arguments(subcommands.add_parser("consume", help="print validated Redpanda events"))
    _add_consume_arguments(subcommands.add_parser("project", help="project validated events into match state"))
    durable_parser = subcommands.add_parser("durable-project", help="project events into PostgreSQL")
    _add_consume_arguments(durable_parser)
    _add_database_argument(durable_parser)
    initialize_parser = subcommands.add_parser("init-db", help="initialize the PostgreSQL schema")
    _add_database_argument(initialize_parser)
    state_parser = subcommands.add_parser("state", help="print a durable match-state snapshot")
    state_parser.add_argument("--match-id", required=True)
    _add_database_argument(state_parser)
    arguments = parser.parse_args()

    if arguments.command == "init-db":
        _repository(arguments).initialize_schema()
        print("PostgreSQL schema initialized")
        return
    if arguments.command == "state":
        _print_durable_state(arguments)
        return

    broker_config = BrokerConfig(
        bootstrap_servers=arguments.bootstrap_servers,
        topic=arguments.topic,
        group_id=getattr(arguments, "group_id", BrokerConfig().group_id),
    )
    if arguments.command == "produce":
        _produce(arguments, broker_config)
    elif arguments.command == "consume":
        _consume(arguments, broker_config)
    elif arguments.command == "project":
        _project(arguments, broker_config)
    else:
        _durable_project(arguments, broker_config)


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


def _add_database_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database-url", default=DatabaseConfig.from_environment().dsn)


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


def _project(arguments: argparse.Namespace, config: BrokerConfig) -> None:
    projector = MatchProjector()
    consumed = 0
    with RedpandaEventConsumer(config) as consumer:
        while arguments.max_events == 0 or consumed < arguments.max_events:
            processed = project_next(consumer, projector, arguments.poll_timeout)
            if processed is None:
                continue
            state = processed.update.state
            print(
                f"{state.match_id} | {state.current_period}:{state.current_match_clock_seconds:06.3f} "
                f"| {processed.event.event_type} | score {_score_summary(state)} "
                f"| events {state.total_processed_events}"
            )
            consumed += 1


def _durable_project(arguments: argparse.Namespace, config: BrokerConfig) -> None:
    projector = DurableMatchProjector(_repository(arguments))
    consumed = 0
    with RedpandaEventConsumer(config) as consumer:
        while arguments.max_events == 0 or consumed < arguments.max_events:
            processed = project_next(consumer, projector, arguments.poll_timeout)
            if processed is None:
                continue
            if processed.update.applied:
                _print_projection_update(processed.update.state, processed.event.event_type)
            else:
                print(
                    f"{processed.event.match_id} | duplicate ignored "
                    f"id={processed.event.event_id} "
                    f"| events {processed.update.state.total_processed_events}"
                )
            consumed += 1


def _repository(arguments: argparse.Namespace) -> PostgresProjectionRepository:
    return PostgresProjectionRepository(DatabaseConfig(arguments.database_url))


def _print_durable_state(arguments: argparse.Namespace) -> None:
    state = _repository(arguments).load_state(arguments.match_id)
    if state is None:
        print(f"no durable state for match {arguments.match_id!r}")
        return
    print(json.dumps(state_to_dict(state), sort_keys=True))


def _print_projection_update(state: MatchState, event_type: str) -> None:
    print(
        f"{state.match_id} | {state.current_period}:{state.current_match_clock_seconds:06.3f} "
        f"| {event_type} | score {_score_summary(state)} "
        f"| events {state.total_processed_events}"
    )


def _score_summary(state: MatchState) -> str:
    # Keep unknown sides explicit rather than inventing a home/away scoreline.
    home = str(state.score_for(state.home_team)) if state.home_team is not None else "?"
    away = str(state.score_for(state.away_team)) if state.away_team is not None else "?"
    return f"{home}-{away}"


if __name__ == "__main__":
    main()
