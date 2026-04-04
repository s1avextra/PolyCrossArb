#!/usr/bin/env python3
"""Run the BTC candle trading pipeline.

Trades "Bitcoin Up or Down" 5/15-minute markets using price
momentum from 4 exchanges.

Usage:
  python scripts/run_candle.py [--mode paper|live] [--duration N]
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
from polycrossarb.crypto.candle_pipeline import CandlePipeline

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
    pipeline = CandlePipeline(mode=exec_mode)

    loop = asyncio.get_event_loop()
    for sig_name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_name, pipeline.stop)

    if duration:
        async def stop_after():
            await asyncio.sleep(duration)
            pipeline.stop()
        asyncio.create_task(stop_after())

    await pipeline.run()

    s = pipeline.status()
    print(f"\n{'='*50}")
    print(f"  Candle Pipeline Results")
    print(f"  BTC: ${s['btc_price']:,.2f} | Sources: {s['sources']}")
    print(f"  Contracts: {s['contracts']}")
    print(f"  Trades: {s['trades']}")
    print(f"  Expected: ${s.get('expected_profit', 0):.2f}")
    print(f"  Realized: ${s.get('realized_profit', 0):.2f}")
    print(f"  Win Rate: {s.get('win_rate', '?')}")
    print(f"{'='*50}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--duration", type=int, default=None)
    args = parser.parse_args()
    asyncio.run(main(args.mode, args.duration))
