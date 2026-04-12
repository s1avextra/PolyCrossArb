"""Structured JSON logging with optional remote shipping.

Two modes:
  - Development (default): colored console output via structlog
  - Production (LOG_FORMAT=json): JSON lines to stdout, ready for
    any log aggregator (Datadog, Betterstack, CloudWatch, etc.)

Remote shipping:
  Set LOG_REMOTE_URL to ship logs via HTTP POST (JSONL batches).
  Compatible with Betterstack, Axiom, Logtail, etc.

Usage:
    from polycrossarb.monitoring.logging_config import configure_logging
    configure_logging()  # call once at startup
"""
from __future__ import annotations

import atexit
import json
import logging
import os
import queue
import threading
import time
from typing import Any

import structlog


class _RemoteHandler(logging.Handler):
    """Async HTTP log shipper — batches and sends in background thread."""

    def __init__(self, url: str, batch_size: int = 50, flush_interval: float = 10.0):
        super().__init__()
        self._url = url
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._queue: queue.Queue[dict] = queue.Queue(maxsize=10000)
        self._thread = threading.Thread(target=self._shipper, daemon=True)
        self._thread.start()
        atexit.register(self._flush)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            entry = {
                "ts": record.created,
                "level": record.levelname,
                "logger": record.name,
                "msg": record.getMessage(),
            }
            # Include structlog extra fields if present
            if hasattr(record, "_structlog_extra"):
                entry.update(record._structlog_extra)
            self._queue.put_nowait(entry)
        except queue.Full:
            pass  # drop on overflow rather than blocking

    def _shipper(self) -> None:
        import httpx

        batch: list[dict] = []
        while True:
            try:
                item = self._queue.get(timeout=self._flush_interval)
                batch.append(item)
                if len(batch) >= self._batch_size:
                    self._send(batch)
                    batch = []
            except queue.Empty:
                if batch:
                    self._send(batch)
                    batch = []

    def _send(self, batch: list[dict]) -> None:
        import httpx

        payload = "\n".join(json.dumps(e, default=str) for e in batch)
        try:
            httpx.post(
                self._url,
                content=payload,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
        except Exception:
            pass  # silent — logging infra shouldn't crash the bot

    def _flush(self) -> None:
        remaining: list[dict] = []
        while not self._queue.empty():
            try:
                remaining.append(self._queue.get_nowait())
            except queue.Empty:
                break
        if remaining:
            self._send(remaining)


def configure_logging(level: str = "INFO") -> None:
    """Configure structured logging for the application.

    Reads from environment:
      LOG_FORMAT: "json" for production, anything else for dev console
      LOG_REMOTE_URL: HTTP endpoint for remote log shipping
      LOG_LEVEL: override log level (default: INFO)
    """
    log_format = os.environ.get("LOG_FORMAT", "dev")
    remote_url = os.environ.get("LOG_REMOTE_URL", "")
    level = os.environ.get("LOG_LEVEL", level)

    if log_format == "json":
        # Production: JSON lines to stdout
        structlog.configure(
            processors=[
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.stdlib.BoundLogger,
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
        )
    else:
        # Development: colored console
        structlog.configure(
            processors=[
                structlog.stdlib.add_log_level,
                structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.stdlib.BoundLogger,
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
        )

    # Standard library logging
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))
    logging.getLogger("polycrossarb").setLevel(getattr(logging, level.upper(), logging.INFO))
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)

    # Remote shipping
    if remote_url:
        handler = _RemoteHandler(remote_url)
        handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        logging.getLogger("polycrossarb").addHandler(handler)
        logging.getLogger(__name__).info("Remote log shipping enabled: %s", remote_url[:40])
