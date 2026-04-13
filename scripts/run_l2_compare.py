#!/usr/bin/env python3
"""Run the L2 candle backtest under multiple latency presets and compare.

Shares the expensive setup (BTC history load, candle registry fetch,
PMXT loader cache) across runs so the only delta between presets is the
latency model + fill timing — exactly what we want to measure.

Usage:
    uv run python scripts/run_l2_compare.py \
        --start 2026-04-10T12 --hours 4 \
        --presets macbook,dublin \
        --output logs/l2_compare.json
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
from polymomentum.backtest.candle_strategy import CandleStrategyAdapter, StrategyConfig
from polymomentum.backtest.l2_replay import L2BacktestEngine
from polymomentum.backtest.latency_model import (
    StaticLatencyConfig,
    preset_dublin_vps,
    preset_local,
    preset_macbook_us,
    preset_swiss_vps,
)
from polymomentum.backtest.pmxt_loader import PMXTLoader

log = structlog.get_logger(__name__)

PRESETS = {
    "macbook": preset_macbook_us(),
    "dublin": preset_dublin_vps(),
    "swiss": preset_swiss_vps(),
    "zero": preset_local(),
}


def parse_dt(s: str) -> datetime:
    if "T" not in s:
        s += "T00"
    if len(s) == 13:
        s += ":00:00"
    elif len(s) == 16:
        s += ":00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def run_one_preset(
    preset_name: str,
    latency: StaticLatencyConfig,
    *,
    start: datetime,
    end: datetime,
    config: StrategyConfig,
    registry: CandleRegistry,
    btc: BTCHistory,
    loader: PMXTLoader,
) -> dict:
    print(f"\n{'='*70}")
    print(f"  RUN: preset={preset_name}  latency={latency}")
    print(f"{'='*70}")

    adapter = CandleStrategyAdapter(
        registry=registry,
        btc_history=btc,
        config=config,
    )
    engine = L2BacktestEngine(loader=loader, latency=latency)

    t0 = time.time()
    engine.replay(
        start=start,
        end=end,
        token_ids=registry.all_token_ids(),
        strategy=adapter.on_event,
        fee_rate=config.fee_rate,
    )
    replay_s = time.time() - t0

    eng = engine.summary()
    adp = adapter.summary()

    t1 = time.time()
    results = resolve_backtest(engine, adapter, btc)
    resolve_s = time.time() - t1

    print(f"\n  preset={preset_name} replay={replay_s:.1f}s resolve={resolve_s:.1f}s")
    print(format_report(results))

    return {
        "preset": preset_name,
        "latency": {
            "base_ms": latency.base_latency_ms,
            "insert_ms": latency.insert_latency_ms,
            "total_insert_ms": latency.total_insert_ms(),
        },
        "engine": eng,
        "adapter": adp,
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
        "timing": {"replay_s": replay_s, "resolve_s": resolve_s},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--hours", type=int, default=2)
    parser.add_argument("--min-confidence", type=float, default=0.60)
    parser.add_argument("--min-edge", type=float, default=0.07)
    parser.add_argument("--position-size", type=float, default=5.0)
    parser.add_argument("--fee-rate", type=float, default=0.072)
    parser.add_argument("--vol", type=float, default=0.50)
    parser.add_argument("--use-implied-vol", action="store_true")
    parser.add_argument(
        "--presets", default="macbook,dublin",
        help="Comma-separated preset names (macbook,dublin,swiss,zero)",
    )
    parser.add_argument("--btc-csv", default="data/btcusdt_1s_7d.csv")
    parser.add_argument("--cache-dir", default="data/pmxt_cache")
    parser.add_argument("--output", default="logs/l2_compare.json")
    args = parser.parse_args()

    start = parse_dt(args.start)
    end = start + timedelta(hours=args.hours)

    presets = [p.strip() for p in args.presets.split(",") if p.strip()]
    for p in presets:
        if p not in PRESETS:
            print(f"Unknown preset: {p}. Choose from {list(PRESETS)}", file=sys.stderr)
            return 2

    print(f"\n  L2 COMPARISON")
    print(f"  Range:    {start} → {end} ({args.hours}h)")
    print(f"  Presets:  {presets}")
    print(f"  Edge≥{args.min_edge:.0%} Conf≥{args.min_confidence:.0%} Pos=${args.position_size}")
    print()

    # ── Phase 1: BTC history (shared across presets) ──────────
    t0 = time.time()
    btc = BTCHistory()
    if Path(args.btc_csv).exists():
        added = btc.load_csv(args.btc_csv)
        print(f"  Loaded {added} BTC ticks from {args.btc_csv}")
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    if (
        btc.n_ticks == 0
        or btc.first_timestamp() > start_ms
        or btc.last_timestamp() < end_ms
    ):
        fetch_start = start - timedelta(hours=2)
        fetch_end = end + timedelta(hours=1)
        added = btc.load_from_binance(fetch_start, fetch_end, interval="1m")
        print(f"  Fetched {added} klines from Binance")
    print(f"  BTC ticks: {btc.n_ticks}, span {btc.time_span_seconds/3600:.1f}h ({time.time()-t0:.1f}s)")

    # ── Phase 2: Candle registry (shared) ─────────────────────
    t0 = time.time()
    registry = CandleRegistry()
    registry.fetch_range(start - timedelta(hours=1), end + timedelta(hours=1))
    print(f"  Registry: {registry.n_contracts} contracts, "
          f"{registry.n_tokens} tokens ({time.time()-t0:.1f}s)")
    if registry.n_contracts == 0:
        print("ERROR: no candle contracts in range — Gamma may not have data this old", file=sys.stderr)
        return 1

    # ── Phase 3: PMXT loader (shared cache) ───────────────────
    loader = PMXTLoader(cache_dir=args.cache_dir)

    # ── Run each preset ──
    config = StrategyConfig(
        min_confidence=args.min_confidence,
        min_edge=args.min_edge,
        position_size_usd=args.position_size,
        fee_rate=args.fee_rate,
        realized_vol=args.vol,
        use_implied_vol=args.use_implied_vol,
    )

    runs = []
    for name in presets:
        run = run_one_preset(
            name, PRESETS[name],
            start=start, end=end,
            config=config,
            registry=registry, btc=btc, loader=loader,
        )
        runs.append(run)

    # ── Side-by-side report ──
    print(f"\n{'='*70}")
    print(f"  COMPARISON")
    print(f"{'='*70}")
    header = f"{'preset':<10}{'lat ms':>10}{'trades':>8}{'wr':>8}{'pnl $':>10}{'sharpe':>10}{'fills ok':>10}{'avg slip':>10}"
    print(header)
    print("-" * len(header))
    for run in runs:
        r = run["results"]
        e = run["engine"]
        print(
            f"{run['preset']:<10}"
            f"{run['latency']['total_insert_ms']:>10.1f}"
            f"{r['n_trades']:>8d}"
            f"{r['win_rate']*100:>7.1f}%"
            f"{r['total_pnl']:>10.2f}"
            f"{r['sharpe']:>10.2f}"
            f"{e['fills_success']:>10d}"
            f"{e['avg_slippage']:>10.4f}"
        )

    out = {
        "config": {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "hours": args.hours,
            "min_confidence": args.min_confidence,
            "min_edge": args.min_edge,
            "position_size": args.position_size,
            "fee_rate": args.fee_rate,
            "vol": args.vol,
            "use_implied_vol": args.use_implied_vol,
            "presets": presets,
        },
        "runs": runs,
    }
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(out, indent=2, default=str))
    print(f"\n  saved to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
