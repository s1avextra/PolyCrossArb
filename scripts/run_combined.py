#!/usr/bin/env python3
"""Run weather + crypto strategies concurrently with shared risk.

Usage:
  python -m scripts.run_combined [--mode paper|live] [--duration N]
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
from polycrossarb.risk.manager import RiskManager
from polycrossarb.tracking.pnl_tracker import PnLTracker
from polycrossarb.weather.pipeline import WeatherPipeline
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


async def main(mode: str, duration: int | None):
    exec_mode = ExecutionMode.LIVE if mode == "live" else ExecutionMode.PAPER

    # Shared risk manager — both strategies draw from the same bankroll
    risk = RiskManager()

    # P&L tracker — monitors resolutions
    tracker = PnLTracker(risk)

    # Both strategies share risk manager
    weather = WeatherPipeline(mode=exec_mode, risk_manager=risk)
    crypto = CryptoPipeline(mode=exec_mode, risk_manager=risk)

    # Shutdown handler
    def stop_all():
        weather.stop()
        crypto.stop()
        tracker.stop()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_all)

    if duration:
        async def stop_after():
            await asyncio.sleep(duration)
            stop_all()
        asyncio.create_task(stop_after())

    print(f"Starting combined pipeline ({mode} mode)")
    print(f"  Bankroll: ${risk.effective_bankroll:.2f}")
    print(f"  Strategies: weather + crypto")
    print()

    await asyncio.gather(
        weather.run(),
        crypto.run(),
        tracker.run_loop(),
        return_exceptions=True,
    )

    # Final report
    ws = weather.status()
    cs = crypto.status()
    print(f"\n{'='*60}")
    print(f"  Combined Pipeline Results")
    print(f"  Weather: {ws['trades']} trades, ${ws['total_profit']:.2f} profit")
    print(f"  Crypto:  {cs['trades']} trades, ${cs['total_profit']:.2f} profit")
    print(f"  Resolved: {tracker.n_resolved} positions, ${tracker.total_resolved_pnl:.2f} P&L")
    if tracker.n_resolved > 0:
        print(f"  Win rate: {tracker.win_rate:.0%}")
    print(f"  Bankroll: ${risk.effective_bankroll:.2f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PolyCrossArb Combined Pipeline")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--duration", type=int, default=None, help="Run for N seconds")
    args = parser.parse_args()

    asyncio.run(main(args.mode, args.duration))
