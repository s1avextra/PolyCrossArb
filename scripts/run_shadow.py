#!/usr/bin/env python3
"""Shadow mode: run all strategies with latency measurement.

Measures real tick-to-decision latency, API response times,
price feed health, and signal quality — without placing trades.

Usage:
  uv run python scripts/run_shadow.py --duration 900
"""
from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, "src")

from polycrossarb.monitoring.logging_config import configure_logging

configure_logging()

import structlog
from polycrossarb.config import settings
from polycrossarb.crypto.candle_pipeline import CandlePipeline
from polycrossarb.crypto.price_feed import CryptoPriceFeed
from polycrossarb.data.client import PolymarketClient
from polycrossarb.data.local_book import LocalBookManager
from polycrossarb.execution.executor import ExecutionMode
from polycrossarb.risk.manager import RiskManager

log = structlog.get_logger(__name__)


class LatencyTracker:
    """Measures and reports latencies for all operations."""

    def __init__(self):
        self.measurements: dict[str, list[float]] = {}
        self.start_time = time.time()

    def record(self, name: str, ms: float):
        self.measurements.setdefault(name, []).append(ms)

    def report(self) -> str:
        elapsed = time.time() - self.start_time
        lines = [
            f"\n{'='*65}",
            f"  LATENCY REPORT ({elapsed/60:.1f} minutes)",
            f"{'='*65}",
        ]
        for name, values in sorted(self.measurements.items()):
            if not values:
                continue
            avg = sum(values) / len(values)
            p50 = sorted(values)[len(values) // 2]
            p99 = sorted(values)[int(len(values) * 0.99)]
            mn, mx = min(values), max(values)
            lines.append(
                f"  {name:30s}  avg={avg:7.1f}ms  p50={p50:7.1f}ms  "
                f"p99={p99:7.1f}ms  min={mn:6.1f}  max={mx:6.1f}  n={len(values)}"
            )
        lines.append(f"{'='*65}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        result = {}
        for name, values in self.measurements.items():
            if not values:
                continue
            result[name] = {
                "avg_ms": round(sum(values) / len(values), 2),
                "p50_ms": round(sorted(values)[len(values) // 2], 2),
                "p99_ms": round(sorted(values)[int(len(values) * 0.99)], 2),
                "min_ms": round(min(values), 2),
                "max_ms": round(max(values), 2),
                "count": len(values),
            }
        return result


async def measure_api_latencies(tracker: LatencyTracker, client: PolymarketClient):
    """Measure Gamma and CLOB API response times."""
    import httpx

    # Gamma API
    markets = []
    t0 = time.time()
    try:
        markets = await client.fetch_all_active_markets(min_liquidity=100, limit=10)
        tracker.record("gamma_api_fetch", (time.time() - t0) * 1000)
    except Exception:
        tracker.record("gamma_api_fetch_ERROR", (time.time() - t0) * 1000)

    # CLOB book endpoint
    if markets:
        token_id = None
        for m in markets:
            for o in m.outcomes:
                if o.token_id:
                    token_id = o.token_id
                    break
            if token_id:
                break

        if token_id:
            t0 = time.time()
            try:
                async with httpx.AsyncClient() as hc:
                    resp = await hc.get(
                        f"{settings.poly_base_url}/book",
                        params={"token_id": token_id},
                        timeout=10,
                    )
                    resp.raise_for_status()
                tracker.record("clob_book_fetch", (time.time() - t0) * 1000)
            except Exception:
                tracker.record("clob_book_fetch_ERROR", (time.time() - t0) * 1000)

            # CLOB price endpoint
            t0 = time.time()
            try:
                async with httpx.AsyncClient() as hc:
                    resp = await hc.get(
                        f"{settings.poly_base_url}/price",
                        params={"token_id": token_id, "side": "buy"},
                        timeout=10,
                    )
                    resp.raise_for_status()
                tracker.record("clob_price_fetch", (time.time() - t0) * 1000)
            except Exception:
                tracker.record("clob_price_fetch_ERROR", (time.time() - t0) * 1000)


async def measure_price_feed(tracker: LatencyTracker, duration: int):
    """Measure price feed latency and reliability."""
    feed = CryptoPriceFeed()

    async def track_feed():
        # Wait for first price
        while feed.btc_price == 0:
            await asyncio.sleep(0.1)

        start = time.time()
        prev_price = feed.btc_price
        tick_count = 0

        while time.time() - start < duration:
            t0 = time.time()
            price = feed.btc_price
            age = feed.age_ms
            sources = feed.n_live_sources

            if price != prev_price:
                tick_count += 1
                tracker.record("price_feed_staleness", age)
                tracker.record("price_feed_sources", sources)
                tracker.record("cross_exchange_spread", feed.cross_exchange_spread)
                prev_price = price

            await asyncio.sleep(0.1)  # 100ms sample rate

        return tick_count

    # Run feed + tracker concurrently
    feed_task = asyncio.create_task(feed.start())
    tick_count = await track_feed()
    feed.stop()

    return tick_count


async def measure_scan_cycle(tracker: LatencyTracker, client: PolymarketClient):
    """Measure full scan cycle latency."""
    from polycrossarb.crypto.candle_scanner import scan_candle_markets
    from polycrossarb.arb.detector import detect_cross_market_arbs

    # Full market fetch + scan
    t0 = time.time()
    markets = await client.fetch_all_active_markets(min_liquidity=0)
    tracker.record("full_market_fetch", (time.time() - t0) * 1000)

    # Candle scan
    t0 = time.time()
    contracts = scan_candle_markets(markets, max_hours=2.0, min_liquidity=50)
    tracker.record("candle_scan", (time.time() - t0) * 1000)

    # Arb detection
    t0 = time.time()
    arbs = detect_cross_market_arbs(markets, exclusive_only=True)
    tracker.record("arb_detection", (time.time() - t0) * 1000)

    return len(markets), len(contracts), len(arbs)


async def main(duration: int):
    tracker = LatencyTracker()
    client = PolymarketClient()

    print(f"{'='*65}")
    print(f"  SHADOW MODE — {duration}s latency measurement")
    print(f"{'='*65}")
    print()

    # Phase 1: API latencies (3 rounds)
    print("Phase 1: Measuring API latencies...")
    for i in range(3):
        await measure_api_latencies(tracker, client)
        print(f"  Round {i+1}/3 complete")
        await asyncio.sleep(2)  # avoid rate limits

    # Phase 2: Full scan cycle
    print("Phase 2: Measuring scan cycle...")
    n_markets, n_candles, n_arbs = await measure_scan_cycle(tracker, client)
    print(f"  Markets: {n_markets}, Candle contracts: {n_candles}, Arb opps: {n_arbs}")

    # Phase 3: Run candle pipeline in paper mode with timing
    print(f"Phase 3: Running candle pipeline for {duration}s...")
    risk = RiskManager(initial_bankroll=100.0, state_dir="/tmp/shadow_state")
    pipeline = CandlePipeline(
        mode=ExecutionMode.PAPER,
        min_confidence=0.55,
        risk_manager=risk,
        log_dir="/tmp/shadow_logs",
    )

    stop_event = asyncio.Event()

    async def run_pipeline_timed():
        await asyncio.sleep(duration)
        pipeline.stop()
        stop_event.set()

    asyncio.create_task(run_pipeline_timed())

    # Measure pipeline startup
    t0 = time.time()

    try:
        await asyncio.wait_for(pipeline.run(), timeout=duration + 30)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass

    pipeline_elapsed = time.time() - t0

    # Record pipeline stats
    status = pipeline.status()
    tracker.record("pipeline_runtime_s", pipeline_elapsed)

    # Phase 4: Measure price feed independently
    print("Phase 4: Measuring price feed quality (30s)...")
    tick_count = await measure_price_feed(tracker, min(30, duration))
    print(f"  Price ticks: {tick_count}")

    await client.close()

    # Print results
    print(tracker.report())

    # Pipeline summary
    print(f"\n  PIPELINE SUMMARY")
    print(f"  BTC:         ${status.get('btc_price', 0):,.2f}")
    print(f"  Sources:     {status.get('sources', 0)}")
    print(f"  Contracts:   {status.get('contracts', 0)}")
    print(f"  Trades:      {status.get('trades', 0)}")
    print(f"  Wins:        {status.get('wins', 0)}")
    print(f"  Losses:      {status.get('losses', 0)}")
    print(f"  Win Rate:    {status.get('win_rate', 'N/A')}")
    print(f"  P&L:         ${status.get('realized_profit', 0):+.2f}")
    print(f"  Bankroll:    ${status.get('bankroll', 0):.2f}")

    # Save detailed results
    results = {
        "timestamp": time.time(),
        "duration_s": duration,
        "latencies": tracker.to_dict(),
        "pipeline_status": status,
        "markets": n_markets,
        "candle_contracts": n_candles,
        "arb_opportunities": n_arbs,
        "price_ticks": tick_count,
    }
    Path("logs").mkdir(exist_ok=True)
    with open("logs/shadow_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  Results saved to logs/shadow_results.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=900, help="Seconds to run")
    args = parser.parse_args()
    asyncio.run(main(args.duration))
