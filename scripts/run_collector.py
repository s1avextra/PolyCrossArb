#!/usr/bin/env python3
"""Start continuous data collection for future backtests.

Usage:
  python scripts/run_collector.py
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys

sys.path.insert(0, "src")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

from polymomentum.backtest.collector import DataCollector


async def main():
    collector = DataCollector()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, collector.stop)

    print("Starting data collector (Ctrl+C to stop)...")
    print("  BTC ticks → data/live/btc_ticks_YYYYMMDD.csv")
    print("  Candle contracts → data/live/candle_contracts.csv")
    print()

    await collector.run()

    s = collector.status()
    print(f"\nCollected {s['ticks_collected']} ticks, {s['contracts_collected']} contracts")


if __name__ == "__main__":
    asyncio.run(main())
