#!/usr/bin/env python3
"""Run the WebSocket-based event-driven pipeline.

Usage:
  python -m scripts.run_ws_pipeline [--mode paper|live]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

sys.path.insert(0, "src")

import structlog

from polycrossarb.execution.executor import ExecutionMode
from polycrossarb.pipeline_ws import WebSocketPipeline

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logging.basicConfig(level=logging.WARNING)
logging.getLogger("polycrossarb").setLevel(logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)


async def main(mode: str, duration: int | None):
    exec_mode = ExecutionMode.LIVE if mode == "live" else ExecutionMode.PAPER
    pipeline = WebSocketPipeline(mode=exec_mode)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, pipeline.stop)

    if duration:
        # Run for a fixed duration then stop
        async def stop_after():
            await asyncio.sleep(duration)
            pipeline.stop()

        asyncio.create_task(stop_after())

    await pipeline.run()

    s = pipeline.status()
    print(f"\n{'='*60}")
    print(f"  WebSocket Pipeline Results")
    print(f"  Bankroll: ${s.get('initial_bankroll', 0):.2f} → ${s.get('effective_bankroll', 0):.2f}")
    print(f"  P&L: ${s.get('total_pnl', 0):.4f}  (fees: ${s.get('total_fees', 0):.4f})")
    print(f"  Trades: {s.get('trade_count', 0)}  |  Arb checks: {s.get('arb_checks', 0)}")
    ws = s.get('ws_stats', {})
    print(f"  WS messages: {ws.get('messages_received', 0)}  |  Uptime: {ws.get('uptime_s', 0)}s")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PolyCrossArb WebSocket pipeline")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--duration", type=int, default=None, help="Run for N seconds then stop")
    args = parser.parse_args()

    asyncio.run(main(args.mode, args.duration))
