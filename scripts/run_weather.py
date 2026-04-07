#!/usr/bin/env python3
"""Standalone weather information-edge pipeline.

Trades temperature markets on Polymarket using free weather APIs.
Completely independent — own bankroll tracking, own risk limits.

Usage:
  python scripts/run_weather.py [--mode paper|live] [--bankroll N] [--duration N]

  # Tune parameters:
  python scripts/run_weather.py --mode paper --min-confidence 0.85 --max-position-pct 0.15
"""
from __future__ import annotations

import argparse
import asyncio
import signal
import sys
import time

sys.path.insert(0, "src")

from polycrossarb.execution.executor import ExecutionMode
from polycrossarb.monitoring.logging_config import configure_logging
from polycrossarb.risk.manager import RiskManager
from polycrossarb.weather.pipeline import WeatherPipeline

configure_logging()


async def main(args):
    exec_mode = ExecutionMode.LIVE if args.mode == "live" else ExecutionMode.PAPER

    risk = RiskManager(
        initial_bankroll=args.bankroll,
        max_per_market=args.bankroll * args.max_position_pct,
        state_dir="logs/weather",
    )

    pipeline = WeatherPipeline(mode=exec_mode, risk_manager=risk)

    # Override tunable parameters
    pipeline._WEATHER_POLL = args.poll_interval
    pipeline._PREDICT_INTERVAL = args.predict_interval

    loop = asyncio.get_event_loop()
    for sig_name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_name, pipeline.stop)

    if args.duration:
        async def stop_after():
            await asyncio.sleep(args.duration)
            pipeline.stop()
        asyncio.create_task(stop_after())

    print(f"{'='*55}")
    print(f"  WEATHER PIPELINE — {'LIVE' if args.mode == 'live' else 'PAPER'}")
    print(f"{'='*55}")
    print(f"  Bankroll:        ${args.bankroll:.2f}")
    print(f"  Min confidence:  {args.min_confidence:.0%}")
    print(f"  Max position:    {args.max_position_pct:.0%} of bankroll")
    print(f"  Kelly fraction:  {args.kelly_fraction}")
    print(f"  Poll interval:   {args.poll_interval}s")
    print(f"  Predict interval:{args.predict_interval}s")
    print(f"{'='*55}")
    print()

    start = time.time()
    await pipeline.run()
    elapsed = time.time() - start

    s = pipeline.status()
    print(f"\n{'='*55}")
    print(f"  WEATHER RESULTS ({elapsed/60:.0f} min)")
    print(f"{'='*55}")
    print(f"  Events scanned:  {s['events']}")
    print(f"  Predictions:     {s['predictions']}")
    print(f"  Trades:          {s['trades']}")
    print(f"  Expected P&L:    ${s['total_profit']:.2f}")
    print(f"  Bankroll:        ${risk.effective_bankroll:.2f}")
    print(f"{'='*55}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather Trading Pipeline")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--bankroll", type=float, default=6.03)
    parser.add_argument("--duration", type=int, default=None, help="Seconds to run")
    parser.add_argument("--min-confidence", type=float, default=0.80)
    parser.add_argument("--max-position-pct", type=float, default=0.15)
    parser.add_argument("--kelly-fraction", type=float, default=0.25)
    parser.add_argument("--poll-interval", type=int, default=30)
    parser.add_argument("--predict-interval", type=int, default=60)
    args = parser.parse_args()
    asyncio.run(main(args))
