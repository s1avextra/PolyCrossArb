#!/usr/bin/env python3
"""Run the weather information-edge trading pipeline.

Usage:
  python -m scripts.run_weather_pipeline [--mode paper|live] [--duration N]
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
from polycrossarb.weather.pipeline import WeatherPipeline

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


async def main(mode: str, duration: int | None):
    exec_mode = ExecutionMode.LIVE if mode == "live" else ExecutionMode.PAPER
    pipeline = WeatherPipeline(mode=exec_mode)

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
    print(f"  Weather Pipeline Results")
    print(f"  Events scanned: {s['events']}")
    print(f"  Predictions made: {s['predictions']}")
    print(f"  Trades: {s['trades']}")
    print(f"  Expected profit: ${s['total_profit']:.2f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PolyCrossArb Weather Pipeline")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--duration", type=int, default=None, help="Run for N seconds")
    args = parser.parse_args()

    asyncio.run(main(args.mode, args.duration))
