"""Operational logging, metrics, health, and diagnostics adapters."""

__all__ = [
    "HealthStatus",
    "JsonFormatter",
    "MatchStreamMetrics",
    "ObservabilityServer",
    "PostgresHealthCheck",
    "ReadinessService",
    "RedpandaHealthCheck",
    "configure_logging",
    "get_logger",
    "metrics",
]


def __getattr__(name: str) -> object:
    """Avoid importing storage/transport dependencies when only metrics are needed."""

    if name in {"HealthStatus", "PostgresHealthCheck", "ReadinessService", "RedpandaHealthCheck"}:
        from .health import HealthStatus, PostgresHealthCheck, ReadinessService, RedpandaHealthCheck

        return {
            "HealthStatus": HealthStatus,
            "PostgresHealthCheck": PostgresHealthCheck,
            "ReadinessService": ReadinessService,
            "RedpandaHealthCheck": RedpandaHealthCheck,
        }[name]
    if name in {"JsonFormatter", "configure_logging", "get_logger"}:
        from .logging import JsonFormatter, configure_logging, get_logger

        return {"JsonFormatter": JsonFormatter, "configure_logging": configure_logging, "get_logger": get_logger}[name]
    if name in {"MatchStreamMetrics", "metrics"}:
        from .metrics import MatchStreamMetrics, metrics

        return {"MatchStreamMetrics": MatchStreamMetrics, "metrics": metrics}[name]
    if name == "ObservabilityServer":
        from .server import ObservabilityServer

        return ObservabilityServer
    raise AttributeError(name)
