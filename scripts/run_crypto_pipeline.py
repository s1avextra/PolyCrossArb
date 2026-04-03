#!/usr/bin/env python3
"""Run the crypto cross-exchange arbitrage pipeline.

Usage:
  python -m scripts.run_crypto_pipeline [--mode paper|live] [--duration N] [--min-edge 0.03]
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
from polycrossarb.crypto.pipeline import CryptoPipeline

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


async def main(mode: str, duration: int | None, min_edge: float):
    exec_mode = ExecutionMode.LIVE if mode == "live" else ExecutionMode.PAPER
    pipeline = CryptoPipeline(mode=exec_mode, min_edge=min_edge)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, pipeline.stop)

    if duration:
        async def stop_after():
            await asyncio.sleep(duration)
            pipeline.stop()
        asyncio.create_task(stop_after())

    await pipeline.run()

    s = pipeline.status()
    print(f"\n{'='*60}")
    print(f"  Crypto Pipeline Results")
    print(f"  BTC Price: ${s['btc_price']:,.2f}")
    print(f"  Volatility: {s['volatility']}")
    print(f"  Contracts: {s['contracts']}")
    print(f"  Cycles: {s['cycles']}")
    print(f"  Edges Found: {s['edges_found']}")
    print(f"  Trades: {s['trades']}")
    print(f"  Expected Profit: ${s['total_profit']:.2f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PolyCrossArb Crypto Pipeline")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--duration", type=int, default=None, help="Run for N seconds")
    parser.add_argument("--min-edge", type=float, default=0.03, help="Min edge to trade, e.g. 0.03")
    args = parser.parse_args()

    asyncio.run(main(args.mode, args.duration, args.min_edge))
