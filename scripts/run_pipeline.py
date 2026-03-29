#!/usr/bin/env python3
"""Run the arbitrage pipeline.

Usage:
  python -m scripts.run_pipeline [--mode paper|live] [--cycles N] [--interval 30]
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
from polycrossarb.pipeline import Pipeline

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


async def main(mode: str, cycles: int | None, interval: float):
    from polycrossarb.config import settings
    settings.scan_interval_seconds = interval

    exec_mode = ExecutionMode.LIVE if mode == "live" else ExecutionMode.PAPER
    pipeline = Pipeline(mode=exec_mode)

    # Handle graceful shutdown
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, pipeline.stop)

    await pipeline.run(max_cycles=cycles)

    # Print final status
    s = pipeline.status()
    print(f"\n{'='*60}")
    print(f"  Pipeline stopped after {s['cycles']} cycles")
    print(f"  Bankroll: ${s.get('initial_bankroll', 0):.2f} → ${s.get('effective_bankroll', 0):.2f}")
    print(f"  P&L: ${s.get('total_pnl', 0):.4f}  (fees paid: ${s.get('total_fees', 0):.4f})")
    print(f"  Exposure: ${s.get('total_exposure', 0):.2f} / ${s.get('max_exposure', 0):.2f}")
    print(f"  Trades: {s.get('total_trades', 0)}  |  Open positions: {s.get('open_positions', 0)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PolyCrossArb pipeline")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--cycles", type=int, default=None, help="Max cycles (None=infinite)")
    parser.add_argument("--interval", type=float, default=30, help="Scan interval in seconds")
    args = parser.parse_args()

    asyncio.run(main(args.mode, args.cycles, args.interval))
