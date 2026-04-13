"""Fetch historical BTC 1-second klines from Binance REST API.

Binance public API — no API key needed.
Endpoint: GET /api/v3/klines?symbol=BTCUSDT&interval=1s&limit=1000

Rate limit: 1200 requests/minute (weight 2 per klines call).
Each call returns up to 1000 candles = 1000 seconds ≈ 16.7 minutes.
For 7 days: ~605 calls, takes ~5 minutes with conservative pacing.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import time
from pathlib import Path

import httpx

log = logging.getLogger(__name__)

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1s"
MAX_LIMIT = 1000  # max candles per request
RATE_DELAY = 0.15  # seconds between requests (conservative)


async def fetch_klines(
    days: int = 7,
    output_dir: str = "data",
    symbol: str = SYMBOL,
) -> Path:
    """Download historical 1-second BTC klines from Binance.

    Args:
        days: Number of days of history to fetch.
        output_dir: Directory to save CSV output.
        symbol: Trading pair (default BTCUSDT).

    Returns:
        Path to the output CSV file.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    csv_path = out / f"{symbol.lower()}_1s_{days}d.csv"

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - (days * 86400 * 1000)

    total_seconds = days * 86400
    total_calls = (total_seconds // MAX_LIMIT) + 1

    log.info("Fetching %d days of %s 1s klines (~%d calls)", days, symbol, total_calls)

    rows_written = 0
    current_ms = start_ms

    async with httpx.AsyncClient(timeout=30) as client:
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp", "open", "high", "low", "close", "volume",
            ])

            call_count = 0
            while current_ms < now_ms:
                params = {
                    "symbol": symbol,
                    "interval": INTERVAL,
                    "startTime": current_ms,
                    "limit": MAX_LIMIT,
                }

                try:
                    resp = await client.get(BINANCE_KLINES_URL, params=params)
                    resp.raise_for_status()
                    data = resp.json()
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        log.warning("Rate limited, sleeping 30s")
                        await asyncio.sleep(30)
                        continue
                    raise
                except httpx.RequestError as e:
                    log.warning("Request error: %s, retrying in 5s", e)
                    await asyncio.sleep(5)
                    continue

                if not data:
                    break

                for candle in data:
                    # Binance kline format: [open_time, open, high, low, close, volume, ...]
                    ts_ms = candle[0]
                    writer.writerow([
                        ts_ms,
                        candle[1],  # open
                        candle[2],  # high
                        candle[3],  # low
                        candle[4],  # close
                        candle[5],  # volume
                    ])
                    rows_written += 1

                # Advance past last candle
                current_ms = data[-1][0] + 1000  # +1 second

                call_count += 1
                if call_count % 100 == 0:
                    pct = min(100, (current_ms - start_ms) / (now_ms - start_ms) * 100)
                    log.info("Progress: %d calls, %d rows, %.0f%%",
                             call_count, rows_written, pct)

                await asyncio.sleep(RATE_DELAY)

    log.info("Saved %d rows to %s", rows_written, csv_path)
    return csv_path


async def fetch_klines_multi(
    symbols: list[str],
    days: int = 7,
    output_dir: str = "data",
) -> dict[str, Path]:
    """Fetch klines for multiple symbols sequentially."""
    results = {}
    for sym in symbols:
        path = await fetch_klines(days=days, output_dir=output_dir, symbol=sym)
        results[sym] = path
    return results
