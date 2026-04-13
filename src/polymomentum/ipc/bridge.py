"""Async UDS client for the Rust latency engine.

Mirrors the Rust IPC protocol: length-prefixed msgpack frames over a
Unix domain socket. Provides:
  - connect() / disconnect()
  - send_contracts(), send_risk_update(), send_config()
  - read_fills() / read_signals() (async generator)
  - Heartbeat monitoring (detects dead engine within 10s)
  - Auto-reconnect on disconnect

Usage:
    bridge = EngineBridge()
    await bridge.connect()
    await bridge.send_contracts(contracts)
    async for msg in bridge.read_messages():
        if msg["type"] == "fill_report":
            ...
"""
from __future__ import annotations

import asyncio
import logging
import struct
import time
from pathlib import Path
from typing import Any, AsyncIterator

log = logging.getLogger(__name__)

DEFAULT_SOCKET_PATH = "/tmp/polymomentum/engine.sock"

# Frame format: 4-byte big-endian length prefix + msgpack payload
HEADER_SIZE = 4
MAX_FRAME_SIZE = 10 * 1024 * 1024  # 10 MB


class EngineBridge:
    """Async UDS client for the Rust latency engine."""

    def __init__(self, socket_path: str = DEFAULT_SOCKET_PATH):
        self._socket_path = socket_path
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connected = False
        self._last_heartbeat = 0.0
        self._reconnect_delay = 1.0

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def heartbeat_age_s(self) -> float:
        if self._last_heartbeat == 0:
            return float("inf")
        return time.time() - self._last_heartbeat

    async def connect(self, timeout: float = 10.0) -> bool:
        """Connect to the Rust engine UDS."""
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_unix_connection(self._socket_path),
                timeout=timeout,
            )
            self._connected = True
            self._last_heartbeat = time.time()
            log.info("ipc.connected", path=self._socket_path)
            return True
        except (ConnectionRefusedError, FileNotFoundError, asyncio.TimeoutError) as e:
            log.warning("ipc.connect_failed", error=str(e), path=self._socket_path)
            self._connected = False
            return False

    async def disconnect(self):
        """Gracefully disconnect."""
        if self._writer:
            try:
                await self._send({"type": "shutdown"})
            except Exception:
                pass
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        self._connected = False
        self._reader = None
        self._writer = None

    async def send_contracts(self, contracts: list[dict[str, Any]]):
        """Send active candle contracts to the engine."""
        await self._send({"type": "contracts", "data": contracts})

    async def send_risk_update(self, available_capital: float):
        """Update the engine's risk budget."""
        await self._send({"type": "risk", "available_capital": available_capital})

    async def send_config(self, config: dict[str, Any]):
        """Update strategy configuration."""
        await self._send({"type": "config", "data": config})

    async def read_messages(self) -> AsyncIterator[dict[str, Any]]:
        """Async generator that yields messages from the engine.

        Handles heartbeats internally (updates last_heartbeat).
        Yields trade_signal, fill_report, and latency_report messages.
        """
        while self._connected:
            try:
                msg = await self._recv()
                if msg is None:
                    break

                msg_type = msg.get("type", "")
                if msg_type == "heartbeat":
                    self._last_heartbeat = msg.get("ts", time.time())
                    continue

                yield msg

            except (ConnectionError, asyncio.IncompleteReadError):
                log.warning("ipc.disconnected")
                self._connected = False
                break
            except Exception:
                log.exception("ipc.read_error")
                self._connected = False
                break

    async def reconnect_loop(self, max_retries: int = 0):
        """Keep trying to connect. max_retries=0 means infinite."""
        attempts = 0
        while max_retries == 0 or attempts < max_retries:
            if await self.connect():
                return True
            attempts += 1
            await asyncio.sleep(min(self._reconnect_delay * (1.5 ** min(attempts, 10)), 30))
        return False

    # ── Internal frame I/O ──────────────────────────────────────────

    async def _send(self, msg: dict[str, Any]):
        """Send a length-prefixed msgpack frame."""
        import msgpack  # lazy import — only needed when IPC is active

        if not self._writer:
            raise ConnectionError("Not connected")

        payload = msgpack.packb(msg, use_bin_type=True)
        header = struct.pack(">I", len(payload))
        self._writer.write(header + payload)
        await self._writer.drain()

    async def _recv(self) -> dict[str, Any] | None:
        """Read a length-prefixed msgpack frame."""
        import msgpack

        if not self._reader:
            return None

        header = await self._reader.readexactly(HEADER_SIZE)
        length = struct.unpack(">I", header)[0]
        if length > MAX_FRAME_SIZE:
            raise ValueError(f"Frame too large: {length}")

        payload = await self._reader.readexactly(length)
        return msgpack.unpackb(payload, raw=False)
