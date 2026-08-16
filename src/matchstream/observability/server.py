"""Minimal standard-library HTTP endpoint for health and Prometheus metrics."""

from __future__ import annotations

import json
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any

from prometheus_client import REGISTRY, CollectorRegistry, generate_latest

from .health import ReadinessService


class ObservabilityServer:
    """Serve liveness, readiness, and metrics without introducing an API framework."""

    def __init__(
        self,
        readiness: ReadinessService,
        host: str = "127.0.0.1",
        port: int = 9464,
        registry: CollectorRegistry = REGISTRY,
    ) -> None:
        self._readiness = readiness
        self._registry = registry
        self._server = ThreadingHTTPServer((host, port), self._handler())
        self._thread: Thread | None = None

    @property
    def address(self) -> tuple[str, int]:
        return self._server.server_address[0], self._server.server_address[1]

    def start(self) -> None:
        if self._thread is None:
            self._thread = Thread(target=self._server.serve_forever, daemon=True)
            self._thread.start()

    def serve_forever(self) -> None:
        self._server.serve_forever()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        readiness = self._readiness
        registry = self._registry

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/health/live":
                    _write_json(self, 200, {"status": "live"})
                    return
                if self.path == "/health/ready":
                    ready, statuses = readiness.ready()
                    _write_json(
                        self,
                        200 if ready else 503,
                        {
                            "status": "ready" if ready else "not_ready",
                            "dependencies": [asdict(status) for status in statuses],
                        },
                    )
                    return
                if self.path == "/metrics":
                    payload = generate_latest(registry)
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                _write_json(self, 404, {"error": "not_found"})

            def log_message(self, _format: str, *_args: Any) -> None:
                return

        return Handler


def _write_json(handler: BaseHTTPRequestHandler, status: int, payload: object) -> None:
    body = json.dumps(payload, default=lambda item: item.__dict__).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)
