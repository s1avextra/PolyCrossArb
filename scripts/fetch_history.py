#!/usr/bin/env python3
"""Fetch historical BTC 1-second klines from Binance.

Usage:
  python scripts/fetch_history.py [--days 7] [--symbol BTCUSDT]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys

sys.path.insert(0, "src")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

from polycrossarb.backtest.data_fetcher import fetch_klines


async def main(days: int, symbol: str):
    print(f"Fetching {days} days of {symbol} 1-second klines from Binance...")
    print(f"Estimated: ~{days * 86400:,} rows, ~{days * 86400 // 1000} API calls")
    print()

    path = await fetch_klines(days=days, output_dir="data", symbol=symbol)
    print(f"\nDone! Saved to: {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--symbol", default="BTCUSDT")
    args = parser.parse_args()
    asyncio.run(main(args.days, args.symbol))
