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
from .dlq_transport import RedpandaDeadLetterConsumer, RedpandaDeadLetterProducer
from .processing import QuarantinedEvent, project_next, project_next_with_dlq
from .redpanda import BrokerConfig, RedpandaEventConsumer, RedpandaEventProducer
from .replay import publish_replay
from .retry import RetryPolicy


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish or consume MatchStream events via Redpanda")
    subcommands = parser.add_subparsers(dest="command", required=True)
    _add_produce_arguments(subcommands.add_parser("produce", help="replay a StatsBomb file to Redpanda"))
    _add_consume_arguments(subcommands.add_parser("consume", help="print validated Redpanda events"))
    _add_consume_arguments(subcommands.add_parser("project", help="project validated events into match state"))
    durable_parser = subcommands.add_parser("durable-project", help="project events into PostgreSQL")
    _add_consume_arguments(durable_parser)
    _add_database_argument(durable_parser)
    resilient_parser = subcommands.add_parser("resilient-project", help="durably project with dead-letter handling")
    _add_consume_arguments(resilient_parser)
    _add_database_argument(resilient_parser)
    _add_dead_letter_arguments(resilient_parser)
    initialize_parser = subcommands.add_parser("init-db", help="initialize the PostgreSQL schema")
    _add_database_argument(initialize_parser)
    state_parser = subcommands.add_parser("state", help="print a durable match-state snapshot")
    state_parser.add_argument("--match-id", required=True)
    _add_database_argument(state_parser)
    status_parser = subcommands.add_parser("status", help="print concise durable projection status")
    status_parser.add_argument("--match-id", required=True)
    _add_database_argument(status_parser)
    rebuild_parser = subcommands.add_parser("rebuild", help="offline rebuild from durable canonical history")
    rebuild_parser.add_argument("--match-id", required=True)
    _add_database_argument(rebuild_parser)
    for command in ("dlq-list", "dlq-show", "dlq-replay"):
        dlq_parser = subcommands.add_parser(command, help=f"{command} operational command")
        _add_common_broker_arguments(dlq_parser)
        _add_dead_letter_arguments(dlq_parser)
        dlq_parser.add_argument(
            "--group-id",
            default=f"{BrokerConfig.from_environment().group_id}-dlq-operator",
            help="consumer group used to inspect the DLQ; use a fresh group to rescan retained records",
        )
        dlq_parser.add_argument("--max-events", type=int, default=100)
        if command in {"dlq-show", "dlq-replay"}:
            dlq_parser.add_argument("--record-id", required=True)
        if command == "dlq-replay":
            dlq_parser.add_argument("--target-topic", required=True)
    arguments = parser.parse_args()

    if arguments.command == "init-db":
        _repository(arguments).initialize_schema()
        print("PostgreSQL schema initialized")
        return
    if arguments.command == "state":
        _print_durable_state(arguments)
        return
    if arguments.command == "status":
        print(json.dumps(_repository(arguments).projection_status(arguments.match_id), default=str))
        return
    if arguments.command == "rebuild":
        state = _repository(arguments).rebuild_match(arguments.match_id)
        print(json.dumps(state_to_dict(state), sort_keys=True))
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
    elif arguments.command == "durable-project":
        _durable_project(arguments, broker_config)
    elif arguments.command == "resilient-project":
        _resilient_project(arguments, broker_config)
    else:
        _dead_letter_command(arguments, broker_config)


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


def _add_dead_letter_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dlq-topic", default=f"{BrokerConfig.from_environment().topic}.dlq")
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--retry-delay", type=float, default=0.0)


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


def _resilient_project(arguments: argparse.Namespace, config: BrokerConfig) -> None:
    projector = DurableMatchProjector(_repository(arguments))
    dlq_config = BrokerConfig(
        bootstrap_servers=config.bootstrap_servers,
        topic=arguments.dlq_topic,
        group_id=config.group_id,
    )
    policy = RetryPolicy(max_attempts=arguments.max_attempts, base_delay_seconds=arguments.retry_delay)
    consumed = 0
    with RedpandaEventConsumer(config) as consumer, RedpandaDeadLetterProducer(dlq_config) as publisher:
        while arguments.max_events == 0 or consumed < arguments.max_events:
            result = project_next_with_dlq(
                consumer, projector, publisher, policy, arguments.poll_timeout
            )
            if result is None:
                continue
            if isinstance(result, QuarantinedEvent):
                print(f"quarantined {result.event.event_id} as {result.dead_letter.record_id}")
            elif result.update.applied:
                _print_projection_update(result.update.state, result.event.event_type)
            else:
                print(f"{result.event.match_id} | durable duplicate ignored id={result.event.event_id}")
            consumed += 1


def _dead_letter_command(arguments: argparse.Namespace, config: BrokerConfig) -> None:
    dlq_config = BrokerConfig(
        bootstrap_servers=config.bootstrap_servers,
        topic=arguments.dlq_topic,
        group_id=config.group_id,
    )
    with RedpandaDeadLetterConsumer(dlq_config) as consumer:
        for _ in range(arguments.max_events):
            record = consumer.poll(1.0)
            if record is None:
                continue
            if arguments.command == "dlq-list":
                consumer.commit()
                print(
                    f"{record.record_id} match={record.original_event.match_id} "
                    f"source={record.source.topic}:{record.source.partition}:{record.source.offset} "
                    f"error={record.error_type}"
                )
            elif record.record_id == arguments.record_id:
                if arguments.command == "dlq-show":
                    print(json.dumps({"record_id": record.record_id, "event_id": record.original_event.event_id,
                                      "match_id": record.original_event.match_id, "error": record.error_message,
                                      "attempts": record.attempt_count}, sort_keys=True))
                else:
                    with RedpandaEventProducer(
                        BrokerConfig(
                            bootstrap_servers=config.bootstrap_servers,
                            topic=arguments.target_topic,
                            group_id=config.group_id,
                        )
                    ) as producer:
                        producer.publish(record.original_event)
                    print(f"replayed {record.original_event.event_id} to {arguments.target_topic}")
                return
    if arguments.command != "dlq-list":
        raise RuntimeError(f"dead-letter record {arguments.record_id!r} was not found")


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
