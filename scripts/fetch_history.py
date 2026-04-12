#!/usr/bin/env python3
"""Fetch historical klines from Binance for BTC, ETH, and SOL.

Downloads 1-second resolution data for backtesting. No auth required.
Run on any machine (local or VPS) before running the L2 sweep.

Usage:
    # Fetch all three assets, last 7 days
    python scripts/fetch_history.py --days 7

    # Fetch specific asset
    python scripts/fetch_history.py --days 7 --symbols BTCUSDT

    # Fetch specific date range
    python scripts/fetch_history.py --start 2026-04-09 --end 2026-04-12
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "src")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

ALL_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

FILENAMES = {
    "BTCUSDT": "data/btcusdt_1s_7d.csv",
    "ETHUSDT": "data/ethusdt_1s_7d.csv",
    "SOLUSDT": "data/solusdt_1s_7d.csv",
}


def fetch_binance_klines(
    symbol: str,
    start: datetime,
    end: datetime,
    interval: str = "1s",
) -> list[tuple[int, float]]:
    """Fetch klines from Binance REST API. Returns [(timestamp_ms, close), ...]."""
    import httpx

    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    ticks: list[tuple[int, float]] = []
    cursor = start_ms
    requests = 0

    with httpx.Client(timeout=15.0) as client:
        while cursor < end_ms:
            try:
                resp = client.get(
                    "https://api.binance.com/api/v3/klines",
                    params={
                        "symbol": symbol,
                        "interval": interval,
                        "startTime": cursor,
                        "endTime": end_ms,
                        "limit": 1000,
                    },
                )
                resp.raise_for_status()
                klines = resp.json()
                requests += 1
            except Exception as e:
                log.warning("Binance fetch failed: %s (retrying in 2s)", str(e)[:60])
                time.sleep(2)
                continue

            if not klines:
                break

            for k in klines:
                try:
                    ts = int(k[0])
                    close = float(k[4])
                    ticks.append((ts, close))
                except (ValueError, TypeError, IndexError):
                    continue

            last_ts = int(klines[-1][0])
            if last_ts <= cursor:
                break
            cursor = last_ts + 1

            if len(klines) < 1000:
                break

            # Progress every 50 requests
            if requests % 50 == 0:
                pct = (cursor - start_ms) / max(end_ms - start_ms, 1) * 100
                log.info("  %s: %d ticks, %.0f%% (%d requests)", symbol, len(ticks), pct, requests)

    # Deduplicate and sort
    seen: set[int] = set()
    unique = []
    for ts, p in sorted(ticks):
        if ts not in seen:
            seen.add(ts)
            unique.append((ts, p))

    return unique


def save_csv(ticks: list[tuple[int, float]], path: str) -> None:
    """Save ticks as CSV compatible with BTCHistory.load_csv()."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        for ts, price in ticks:
            w.writerow([ts, price, price, price, price, 0])


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch BTC/ETH/SOL klines from Binance")
    parser.add_argument("--days", type=int, default=7, help="Days of history (default 7)")
    parser.add_argument("--start", default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--symbols", default=",".join(ALL_SYMBOLS),
                        help=f"Comma-separated symbols (default: {','.join(ALL_SYMBOLS)})")
    parser.add_argument("--interval", default="1s", help="Kline interval (default 1s)")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",")]

    if args.start and args.end:
        start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    else:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=args.days)

    # Add 2h padding for momentum warm-up
    fetch_start = start - timedelta(hours=2)
    fetch_end = end + timedelta(hours=1)
    total_hours = (fetch_end - fetch_start).total_seconds() / 3600

    print(f"\n  Fetching {len(symbols)} symbols at {args.interval} resolution")
    print(f"  Range: {fetch_start.strftime('%Y-%m-%d %H:%M')} → {fetch_end.strftime('%Y-%m-%d %H:%M')} ({total_hours:.0f}h)")
    print(f"  Symbols: {symbols}")
    print()

    for symbol in symbols:
        t0 = time.time()
        log.info("Fetching %s...", symbol)
        ticks = fetch_binance_klines(symbol, fetch_start, fetch_end, args.interval)
        dt = time.time() - t0

        path = FILENAMES.get(symbol, f"data/{symbol.lower()}_{args.interval}_{args.days}d.csv")
        save_csv(ticks, path)
        size_kb = Path(path).stat().st_size / 1024

        log.info("  %s: %d ticks in %.1fs → %s (%.0f KB)",
                 symbol, len(ticks), dt, path, size_kb)

    print(f"\nDone! Files saved to data/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
