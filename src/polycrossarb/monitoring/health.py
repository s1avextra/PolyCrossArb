"""Lightweight HTTP health endpoint for remote monitoring.

Runs a tiny HTTP server on a background thread. Returns JSON with
bot status so you can monitor from your phone or an uptime service.

Usage:
    health = HealthServer(port=8080)
    health.set_status_fn(pipeline.status)  # any callable -> dict
    health.start()  # non-blocking, runs on background thread
    ...
    health.stop()
"""
from __future__ import annotations

import json
import logging
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Callable

log = logging.getLogger(__name__)


class _Handler(BaseHTTPRequestHandler):
    """Minimal handler — serves GET / and GET /health."""

    _status_fn: Callable[[], dict[str, Any]] | None = None
    _start_time: float = 0.0

    def do_GET(self):
        if self.path not in ("/", "/health"):
            self.send_error(404)
            return

        status: dict[str, Any] = {}
        if self._status_fn:
            try:
                status = self._status_fn()
            except Exception as e:
                status = {"error": str(e)[:200]}

        status["uptime_seconds"] = round(time.time() - self._start_time)
        status["healthy"] = True

        body = json.dumps(status, indent=2, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        # Suppress default stderr logging
        pass


class HealthServer:
    """Background HTTP server exposing bot health."""

    def __init__(self, port: int = 8080):
        self._port = port
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._status_fn: Callable[[], dict[str, Any]] | None = None

    def set_status_fn(self, fn: Callable[[], dict[str, Any]]) -> None:
        """Set the function called on each health check request."""
        self._status_fn = fn

    def start(self) -> None:
        """Start the health server on a background thread."""
        _Handler._status_fn = self._status_fn
        _Handler._start_time = time.time()

        try:
            self._server = HTTPServer(("0.0.0.0", self._port), _Handler)
        except OSError as e:
            log.warning("Health server failed to bind port %d: %s", self._port, e)
            return

        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        log.info("Health server started on port %d", self._port)

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            log.info("Health server stopped")
