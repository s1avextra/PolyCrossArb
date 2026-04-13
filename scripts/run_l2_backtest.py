#!/usr/bin/env python3
"""L2 backtest runner — replays candle strategy on real Polymarket history.

End-to-end pipeline:
  1. Fetch BTC kline history from Binance for the date range
  2. Discover candle markets via Gamma API (cached)
  3. Download PMXT order book parquet files from r2.pmxt.dev (cached)
  4. Replay events through MomentumDetector + decide_candle_trade
  5. Apply latency-shifted one-tick taker fills
  6. Resolve fills against actual BTC outcomes
  7. Print metrics by zone, confidence, asset, window length

Usage:
  uv run python scripts/run_l2_backtest.py --start 2026-04-10T12 --hours 4
  uv run python scripts/run_l2_backtest.py --start 2026-04-10T00 --hours 24 --vps dublin
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "src")

from polymomentum.monitoring.logging_config import configure_logging

configure_logging()

import structlog
from polymomentum.backtest.btc_history import BTCHistory
from polymomentum.backtest.candle_registry import CandleRegistry
from polymomentum.backtest.candle_resolver import format_report, resolve_backtest
from polymomentum.backtest.candle_strategy import (
    CandleStrategyAdapter,
    StrategyConfig,
)
from polymomentum.backtest.l2_replay import L2BacktestEngine
from polymomentum.backtest.latency_model import (
    StaticLatencyConfig,
    preset_dublin_vps,
    preset_macbook_us,
    preset_swiss_vps,
)
from polymomentum.backtest.pmxt_loader import PMXTLoader

log = structlog.get_logger(__name__)


def parse_dt(s: str) -> datetime:
    """Parse ISO datetime, assume UTC if no timezone."""
    if "T" not in s:
        s += "T00"
    if len(s) == 13:  # "2026-04-10T12"
        s += ":00:00"
    elif len(s) == 16:  # "2026-04-10T12:00"
        s += ":00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def main():
    parser = argparse.ArgumentParser(description="L2 backtest runner")
    parser.add_argument("--start", required=True, help="Start datetime (ISO, UTC)")
    parser.add_argument("--hours", type=int, default=1, help="Hours to backtest")
    parser.add_argument("--min-confidence", type=float, default=0.60)
    parser.add_argument("--min-edge", type=float, default=0.07)
    parser.add_argument("--position-size", type=float, default=5.0)
    parser.add_argument("--fee-rate", type=float, default=0.072,
                        help="Polymarket taker fee rate (default crypto: 0.072)")
    parser.add_argument(
        "--vps", choices=["macbook", "dublin", "swiss", "zero"], default="macbook",
        help="Latency preset",
    )
    parser.add_argument("--btc-csv", default="data/btcusdt_1s_7d.csv",
                        help="BTC tick CSV (uses Binance fetch as fallback)")
    parser.add_argument("--asset", default="", help="Filter to specific asset (BTC/ETH/SOL)")
    parser.add_argument("--use-implied-vol", action="store_true",
                        help="Compute realized vol from BTC history (else use --vol)")
    parser.add_argument("--vol", type=float, default=0.50)
    parser.add_argument("--cache-dir", default="data/pmxt_cache")
    parser.add_argument("--output", default="logs/l2_backtest_results.json")
    args = parser.parse_args()

    start = parse_dt(args.start)
    end = start + timedelta(hours=args.hours)

    print(f"{'='*70}")
    print(f"  L2 BACKTEST")
    print(f"  Range:           {start} -> {end} ({args.hours}h)")
    print(f"  Min confidence:  {args.min_confidence:.0%}")
    print(f"  Min edge:        {args.min_edge:.0%}")
    print(f"  Position size:   ${args.position_size:.2f}")
    print(f"  Fee rate:        {args.fee_rate:.4f}")
    print(f"  VPS preset:      {args.vps}")
    print(f"{'='*70}\n")

    # ── Phase 1: Load BTC history ───────────────────────────────
    t0 = time.time()
    btc = BTCHistory()
    print("Phase 1: Loading BTC history...")
    if Path(args.btc_csv).exists():
        added = btc.load_csv(args.btc_csv)
        print(f"  Loaded {added} ticks from {args.btc_csv}")
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    needs_fetch = (
        btc.n_ticks == 0
        or btc.first_timestamp() > start_ms
        or btc.last_timestamp() < end_ms
    )
    if needs_fetch:
        # Fetch from Binance for the missing range
        print("  Fetching BTC klines from Binance (CSV doesn't cover range)...")
        # Add padding on each side for vol calc
        fetch_start = start - timedelta(hours=2)
        fetch_end = end + timedelta(hours=1)
        added = btc.load_from_binance(fetch_start, fetch_end, interval="1m")
        print(f"  Fetched {added} klines from Binance")

    print(f"  Total: {btc.n_ticks} ticks, span {btc.time_span_seconds/3600:.1f}h")
    print(f"  Range: {btc.first_timestamp()/1000:.0f} -> {btc.last_timestamp()/1000:.0f}")
    print(f"  Phase 1 time: {time.time()-t0:.1f}s\n")

    # Sanity check
    sample_btc = btc.price_at_seconds(start.timestamp())
    print(f"  BTC at {start}: ${sample_btc:,.2f}")

    # ── Phase 2: Fetch candle market registry ──────────────────
    t0 = time.time()
    print("Phase 2: Fetching candle market registry...")
    registry = CandleRegistry()
    registry.fetch_range(
        start - timedelta(hours=1),  # padding for windows that started earlier
        end + timedelta(hours=1),
    )
    print(f"  Registry: {registry.n_contracts} contracts, {registry.n_tokens} token IDs")

    if args.asset:
        before = registry.n_contracts
        # Rebuild registry filtered by asset
        filtered = [c for c in registry._by_condition.values() if c.asset == args.asset.upper()]
        registry._by_condition = {c.condition_id: c for c in filtered}
        registry._by_token = {}
        for c in filtered:
            registry._by_token[c.up_token_id] = c
            registry._by_token[c.down_token_id] = c
        print(f"  Filtered to {args.asset}: {registry.n_contracts}/{before} contracts")

    if registry.n_contracts == 0:
        print("ERROR: no candle contracts in range — Gamma may not have data this old")
        return 1
    print(f"  Phase 2 time: {time.time()-t0:.1f}s\n")

    # ── Phase 3: Run L2 replay ──────────────────────────────────
    t0 = time.time()
    print("Phase 3: Replaying L2 events...")

    latency_map = {
        "macbook": preset_macbook_us(),
        "dublin": preset_dublin_vps(),
        "swiss": preset_swiss_vps(),
        "zero": StaticLatencyConfig(),
    }
    latency = latency_map[args.vps]
    print(f"  Latency: {latency}")

    config = StrategyConfig(
        min_confidence=args.min_confidence,
        min_edge=args.min_edge,
        position_size_usd=args.position_size,
        fee_rate=args.fee_rate,
        realized_vol=args.vol,
        use_implied_vol=args.use_implied_vol,
    )

    adapter = CandleStrategyAdapter(
        registry=registry,
        btc_history=btc,
        config=config,
    )

    loader = PMXTLoader(cache_dir=args.cache_dir)
    engine = L2BacktestEngine(
        loader=loader,
        latency=latency,
    )

    engine.replay(
        start=start,
        end=end,
        token_ids=registry.all_token_ids(),
        strategy=adapter.on_event,
        fee_rate=args.fee_rate,
    )

    print(f"  Phase 3 time: {time.time()-t0:.1f}s")
    s = engine.summary()
    print(f"  Engine summary: {json.dumps(s, indent=2)}")
    a = adapter.summary()
    print(f"\n  Adapter summary: {json.dumps(a, indent=2, default=str)}")

    # ── Phase 4: Resolve fills and report ──────────────────────
    t0 = time.time()
    print("\nPhase 4: Resolving fills against BTC outcomes...")
    results = resolve_backtest(engine, adapter, btc)
    print(f"  Phase 4 time: {time.time()-t0:.1f}s\n")

    print(format_report(results))

    # Save detailed results
    output_data = {
        "config": {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "hours": args.hours,
            "min_confidence": args.min_confidence,
            "min_edge": args.min_edge,
            "position_size": args.position_size,
            "fee_rate": args.fee_rate,
            "vps": args.vps,
            "asset_filter": args.asset,
            "use_implied_vol": args.use_implied_vol,
        },
        "engine_summary": s,
        "adapter_summary": a,
        "results": {
            "n_trades": results.n_trades,
            "n_wins": results.n_wins,
            "n_losses": results.n_losses,
            "win_rate": results.win_rate,
            "total_pnl": results.total_pnl,
            "total_fees": results.total_fees,
            "total_slippage": results.total_slippage,
            "sharpe": results.sharpe,
            "by_zone": results.by_zone,
            "by_confidence": results.by_confidence_bucket,
            "by_asset": results.by_asset,
            "by_window_minutes": {str(k): v for k, v in results.by_window_minutes.items()},
        },
    }
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2, default=str)
    print(f"\nResults saved to {output_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
