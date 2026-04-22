#!/usr/bin/env python3
"""Strategy harness: run N candle strategies against the same L2 replay data.

Goals:
  1. Quick A/B of new strategies against the baseline (decide_candle_trade)
  2. Per-regime stratification so we see WHERE each strategy wins/loses
  3. Shared BTC/registry/loader setup → amortized cost across variants

Usage:
    uv run python scripts/run_strategy_harness.py \
        --start 2026-04-12T00 --hours 24 \
        --output logs/harness_apr12.json
"""
from __future__ import annotations

import argparse
import functools
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "src")

# Flush print immediately so progress is visible when stdout is redirected
print = functools.partial(print, flush=True)

from polymomentum.monitoring.logging_config import configure_logging

configure_logging()

from polymomentum.backtest.btc_history import BTCHistory
from polymomentum.backtest.candle_registry import CandleRegistry
from polymomentum.backtest.candle_resolver import (
    BacktestResults,
    format_report,
    resolve_backtest,
)
from polymomentum.backtest.candle_strategy import CandleStrategyAdapter, StrategyConfig
from polymomentum.backtest.l2_replay import L2BacktestEngine
from polymomentum.backtest.latency_model import preset_dublin_vps
from polymomentum.backtest.pmxt_loader import PMXTLoader
from polymomentum.backtest.strategies import (
    BaselineStrategy,
    EwmaVolStrategy,
    RegimeConditionalStrategy,
)
from polymomentum.crypto.decision import ZoneConfig
from polymomentum.crypto.momentum import VolatilityRegime


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


# ── Regime oracle: classify each trade's entry time ──────────────


class RegimeOracle:
    """Replays BTC ticks through an EwmaVolStrategy to get a regime
    timeline, then answers regime_at(ts) queries for trade classification.

    Samples the current regime every `sample_interval_s` seconds to keep
    memory bounded. Between samples, returns the most recent sampled regime.
    """

    def __init__(
        self,
        btc: BTCHistory,
        start_ts: float,
        end_ts: float,
        sample_interval_s: float = 10.0,
    ):
        self._vol = EwmaVolStrategy(fast_half_life_min=15.0, slow_half_life_min=240.0)
        self._samples: list[tuple[float, VolatilityRegime, float, float]] = []
        self._warm_up_and_sample(btc, start_ts, end_ts, sample_interval_s)

    def _warm_up_and_sample(
        self, btc: BTCHistory, start_ts: float, end_ts: float, interval_s: float
    ) -> None:
        from polymomentum.crypto.momentum import classify_vol_regime

        # Extract ticks within [start - 1h, end]
        warm_start_ms = int((start_ts - 3600) * 1000)
        end_ms = int(end_ts * 1000)
        # BTCHistory.price_at is a lookup — use raw buffer if available
        # Fallback: sample at interval
        last_sample_ts = 0.0
        ticks_fed = 0
        # Walk through seconds from warm_start to end, querying BTC price
        t = warm_start_ms / 1000
        dt = 1.0  # 1 second step
        while t < end_ts:
            price = btc.price_at(int(t * 1000))
            if price and price > 0:
                self._vol.on_tick(t, price)
                ticks_fed += 1
                if t - last_sample_ts >= interval_s and t >= start_ts:
                    fast = self._vol.fast_vol_annualized
                    slow = self._vol.slow_vol_annualized
                    regime = classify_vol_regime(fast, slow)
                    self._samples.append((t, regime, fast, slow))
                    last_sample_ts = t
            t += dt

    def regime_at(self, ts_s: float) -> VolatilityRegime:
        if not self._samples:
            return VolatilityRegime.NORMAL
        # Binary search would be nicer, but N samples is small (< 10k)
        best = self._samples[0][1]
        for sample_ts, regime, _, _ in self._samples:
            if sample_ts > ts_s:
                return best
            best = regime
        return best

    def vol_at(self, ts_s: float) -> tuple[float, float]:
        if not self._samples:
            return 0.0, 0.0
        best_fast = 0.0
        best_slow = 0.0
        for sample_ts, _, fast, slow in self._samples:
            if sample_ts > ts_s:
                return best_fast, best_slow
            best_fast, best_slow = fast, slow
        return best_fast, best_slow


# ── Per-regime stratification ────────────────────────────────────


def stratify_by_regime(
    results: BacktestResults, oracle: RegimeOracle
) -> dict[str, dict]:
    """Group resolved trades by regime at entry time. Returns a dict
    keyed by regime name with per-regime stats.
    """
    buckets: dict[str, list] = {r.name: [] for r in VolatilityRegime}
    for trade in results.trades:
        regime = oracle.regime_at(trade.fill.fill_timestamp)
        buckets[regime.name].append(trade)

    summary = {}
    for regime_name, trades in buckets.items():
        n = len(trades)
        wins = sum(1 for t in trades if t.won)
        pnl = sum(t.pnl_after_fee for t in trades)
        summary[regime_name] = {
            "trades": n,
            "wins": wins,
            "losses": n - wins,
            "win_rate": (wins / n) if n else 0.0,
            "pnl": round(pnl, 2),
            "avg_pnl": round(pnl / n, 3) if n else 0.0,
        }
    return summary


# ── Run a single strategy against the replay ─────────────────────


def run_strategy(
    strategy,
    *,
    start: datetime,
    end: datetime,
    registry: CandleRegistry,
    btc: BTCHistory,
    loader: PMXTLoader,
    oracle: RegimeOracle,
    position_size: float = 1.0,
    fee_rate: float = 0.02,
    use_implied_vol: bool = False,
    asset_histories: dict[str, BTCHistory] | None = None,
) -> dict:
    print(f"\n{'='*70}")
    print(f"  STRATEGY: {strategy.name}")
    print(f"{'='*70}")

    config = StrategyConfig(
        min_confidence=0.60,
        min_edge=0.07,
        realized_vol=0.50,
        position_size_usd=position_size,
        fee_rate=fee_rate,
        use_implied_vol=use_implied_vol,
    )
    adapter = CandleStrategyAdapter(
        registry=registry, btc_history=btc, config=config,
        strategy=strategy,
        asset_histories=asset_histories or {},
    )
    engine = L2BacktestEngine(loader=loader, latency=preset_dublin_vps())

    t0 = time.time()
    engine.replay(
        start=start, end=end,
        token_ids=registry.all_token_ids(),
        strategy=adapter.on_event,
        fee_rate=config.fee_rate,
    )
    replay_s = time.time() - t0

    results = resolve_backtest(engine, adapter, btc)
    by_regime = stratify_by_regime(results, oracle)

    print(format_report(results))
    print(f"\n  per-regime:")
    for regime, stats in by_regime.items():
        if stats["trades"] > 0:
            print(
                f"    {regime:10} trades={stats['trades']:4d} "
                f"WR={stats['win_rate']*100:5.1f}% "
                f"pnl=${stats['pnl']:+7.2f} "
                f"avg=${stats['avg_pnl']:+.3f}"
            )

    strat_snapshot = strategy.snapshot() if hasattr(strategy, "snapshot") else {"name": strategy.name}
    print(f"\n  strategy state: {strat_snapshot}")
    print(f"  replay: {replay_s:.1f}s")

    trades_dump = [
        {
            "asset": t.contract.asset,
            "condition_id": t.contract.condition_id,
            "window_min": t.contract.window_minutes,
            "entry_ts": t.fill.fill_timestamp,
            "close_ts": t.contract.end_time_s,
            "hold_s": t.contract.end_time_s - t.fill.fill_timestamp,
            "direction": t.decision.direction,
            "zone": t.decision.zone,
            "confidence": round(t.decision.confidence, 3),
            "edge": round(t.decision.edge, 4),
            "fill_price": round(t.fill.fill_price, 4),
            "filled_size": round(t.fill.filled_size, 3),
            "cost": round(t.fill.fill_price * t.fill.filled_size, 4),
            "fee": round(t.fill.fee, 5),
            "won": t.won,
            "pnl": round(t.pnl, 5),
            "pnl_after_fee": round(t.pnl_after_fee, 5),
            "regime_at_entry": oracle.regime_at(t.fill.fill_timestamp).name,
        }
        for t in results.trades
    ]

    return {
        "strategy": strategy.name,
        "snapshot": strat_snapshot,
        "replay_s": round(replay_s, 2),
        "total": {
            "trades": results.n_trades,
            "wins": results.n_wins,
            "losses": results.n_losses,
            "win_rate": results.win_rate,
            "pnl": results.total_pnl,
            "sharpe": results.sharpe,
            "fees": results.total_fees,
        },
        "by_regime": by_regime,
        "skip_counts": dict(sorted(
            adapter.skip_counts.items(), key=lambda x: -x[1]
        )[:10]),
        "trades": trades_dump,
    }


# ── Main entry point ─────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--hours", type=int, default=4)
    parser.add_argument("--position-size", type=float, default=1.0)
    parser.add_argument("--fee-rate", type=float, default=0.02)
    parser.add_argument("--asset", default="ALL",
                        help="Filter contracts by asset: BTC, ETH, SOL, or ALL")
    parser.add_argument("--btc-csv", default="data/btcusdt_1s_7d.csv")
    parser.add_argument("--cache-dir", default="data/pmxt_cache")
    parser.add_argument("--output", default="logs/strategy_harness.json")
    parser.add_argument("--strategies", default="baseline,ewma_15min",
                        help="Comma-separated subset: baseline,ewma_15min,regime")
    parser.add_argument("--kline-interval", default="1s",
                        help="Binance kline interval for BTC/ETH/SOL fetch (1s, 1m, ...)")
    parser.add_argument("--load-alt-csvs", action="store_true", default=True,
                        help="Load per-asset histories for ETH/SOL so altcoin contracts use their own prices")
    args = parser.parse_args()

    start = parse_dt(args.start)
    end = start + timedelta(hours=args.hours)

    print(f"\n  STRATEGY HARNESS")
    print(f"  Range:    {start} → {end} ({args.hours}h)")
    print(f"  Position: ${args.position_size}  Fee: {args.fee_rate*100:.1f}%")

    # Shared setup (once, amortized across all strategies)
    t0 = time.time()
    btc = BTCHistory()
    if Path(args.btc_csv).exists():
        btc.load_csv(args.btc_csv)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    if btc.n_ticks == 0 or btc.first_timestamp() > start_ms or btc.last_timestamp() < end_ms:
        fetch_start = start - timedelta(hours=2)
        fetch_end = end + timedelta(hours=1)
        btc.load_from_binance(fetch_start, fetch_end, interval=args.kline_interval)
    print(f"  BTC ticks: {btc.n_ticks} ({time.time()-t0:.1f}s)")

    # Per-asset histories so altcoin contracts don't fall back to BTC prices
    asset_histories: dict[str, BTCHistory] = {}
    if args.load_alt_csvs:
        alt_csv_paths = {
            "ETH": "data/ethusdt_1s_7d.csv",
            "SOL": "data/solusdt_1s_7d.csv",
        }
        for asset, csv_path in alt_csv_paths.items():
            h = BTCHistory()
            if Path(csv_path).exists():
                h.load_csv(csv_path)
            # Also fetch from Binance for the window so we have recent data
            if (h.n_ticks == 0
                    or h.first_timestamp() > start_ms
                    or h.last_timestamp() < end_ms):
                fetch_start = start - timedelta(hours=2)
                fetch_end = end + timedelta(hours=1)
                h.load_from_binance(
                    fetch_start, fetch_end,
                    symbol=f"{asset}USDT",
                    interval=args.kline_interval,
                )
            if h.n_ticks > 0:
                asset_histories[asset] = h
                print(f"  {asset} ticks: {h.n_ticks}")

    t0 = time.time()
    registry = CandleRegistry()
    registry.fetch_range(start - timedelta(hours=1), end + timedelta(hours=1))
    print(f"  Registry:  {registry.n_contracts} contracts ({time.time()-t0:.1f}s)")
    if registry.n_contracts == 0:
        print("ERROR: no candle contracts in range", file=sys.stderr)
        return 1

    loader = PMXTLoader(cache_dir=args.cache_dir)

    # Regime oracle — replay BTC ticks through EWMA vol tracker once,
    # answers regime_at(ts) for trade stratification
    t0 = time.time()
    oracle = RegimeOracle(btc, start.timestamp(), end.timestamp())
    print(f"  Oracle:    {len(oracle._samples)} regime samples ({time.time()-t0:.1f}s)")

    # Terminal-only gate: block non-terminal zones by making their bars
    # unreachable. Keeps terminal thresholds at their defaults so we're
    # running *only* the zone that was profitable in the post-fix audit.
    terminal_only_cfg = ZoneConfig(
        early_min_confidence=0.99,
        early_min_z=999.0,
        early_min_edge=0.99,
        primary_min_z=999.0,
        late_min_confidence=0.99,
        late_min_z=999.0,
        late_min_edge=0.99,
        # terminal_* all left at defaults
    )

    wanted = {s.strip() for s in args.strategies.split(",") if s.strip()}
    strategy_catalog = {
        "baseline": lambda: BaselineStrategy(
            name="baseline",
            zone_config=ZoneConfig(),
        ),
        "ewma_15min": lambda: EwmaVolStrategy(
            name="ewma_15min",
            fast_half_life_min=15.0,
            slow_half_life_min=240.0,
        ),
        "regime": lambda: RegimeConditionalStrategy(name="regime"),
        "terminal_only": lambda: BaselineStrategy(
            name="terminal_only",
            zone_config=terminal_only_cfg,
        ),
        "ewma_terminal_only": lambda: EwmaVolStrategy(
            name="ewma_terminal_only",
            fast_half_life_min=15.0,
            slow_half_life_min=240.0,
            zone_config=terminal_only_cfg,
        ),
    }
    strategies = [strategy_catalog[name]() for name in wanted if name in strategy_catalog]

    runs = []
    for strat in strategies:
        run = run_strategy(
            strat,
            start=start, end=end,
            registry=registry, btc=btc, loader=loader,
            oracle=oracle,
            position_size=args.position_size,
            fee_rate=args.fee_rate,
            asset_histories=asset_histories,
        )
        runs.append(run)

    # Side-by-side summary
    print(f"\n{'='*80}")
    print(f"  HARNESS SUMMARY")
    print(f"{'='*80}")
    hdr = f"{'strategy':<25}{'trades':>8}{'WR':>8}{'pnl $':>10}{'sharpe':>10}{'fees $':>10}"
    print(hdr)
    print("-" * len(hdr))
    for run in runs:
        t = run["total"]
        print(
            f"{run['strategy']:<25}"
            f"{t['trades']:>8d}"
            f"{t['win_rate']*100:>7.1f}%"
            f"{t['pnl']:>10.2f}"
            f"{t['sharpe']:>10.2f}"
            f"{t['fees']:>10.2f}"
        )

    print(f"\n  Per-regime breakdown (trades / win rate / pnl):")
    print(f"  {'strategy':<25}{'LOW':>20}{'NORMAL':>20}{'HIGH':>20}{'EXTREME':>20}")
    for run in runs:
        cells = []
        for regime in ("LOW", "NORMAL", "HIGH", "EXTREME"):
            s = run["by_regime"].get(regime, {"trades": 0, "win_rate": 0, "pnl": 0})
            if s["trades"] > 0:
                cells.append(f"{s['trades']:3d}/{s['win_rate']*100:3.0f}%/${s['pnl']:+.1f}")
            else:
                cells.append("—")
        print(f"  {run['strategy']:<25}" + "".join(f"{c:>20}" for c in cells))

    out = {
        "config": {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "hours": args.hours,
            "position_size": args.position_size,
            "fee_rate": args.fee_rate,
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
