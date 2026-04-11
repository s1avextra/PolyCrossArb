#!/usr/bin/env python3
"""Parameter sweep over min_confidence × min_edge × vol on the same L2
data range. Loads BTC + registry + PMXT cache once, then runs the
strategy for each grid point.

Picks a *stable* point, not the best one — favours config combinations
where adjacent grid neighbours also produce a positive Sharpe.

Usage:
    uv run python scripts/run_l2_sweep.py \
        --start 2026-04-10T12 --hours 4 --vps dublin \
        --confidences 0.55,0.60,0.65 \
        --edges 0.05,0.07,0.10 \
        --vols 0.40,0.50,0.60 \
        --output logs/l2_sweep.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "src")

from polycrossarb.monitoring.logging_config import configure_logging

configure_logging()

from polycrossarb.backtest.btc_history import BTCHistory
from polycrossarb.backtest.candle_registry import CandleRegistry
from polycrossarb.backtest.candle_resolver import resolve_backtest
from polycrossarb.backtest.candle_strategy import CandleStrategyAdapter, StrategyConfig
from polycrossarb.backtest.l2_replay import L2BacktestEngine
from polycrossarb.backtest.latency_model import (
    preset_dublin_vps,
    preset_local,
    preset_macbook_us,
    preset_swiss_vps,
)
from polycrossarb.backtest.pmxt_loader import PMXTLoader
from polycrossarb.crypto.decision import ZoneConfig, zone_config_from_settings

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


def parse_floats(s: str) -> list[float]:
    return [float(x.strip()) for x in s.split(",") if x.strip()]


def neighbours(idx: int, length: int) -> list[int]:
    return [i for i in (idx - 1, idx + 1) if 0 <= i < length]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--hours", type=int, default=2)
    parser.add_argument("--vps", choices=list(PRESETS), default="dublin")
    # Primary-zone gates (passed via StrategyConfig)
    parser.add_argument("--confidences", default="0.60",
                        help="StrategyConfig.min_confidence values (primary-zone gate)")
    parser.add_argument("--edges", default="0.07",
                        help="StrategyConfig.min_edge values (primary-zone gate)")
    parser.add_argument("--vols", default="0.50")
    # Late-zone gates (passed via ZoneConfig overrides — these are the
    # binding ones in observed runs, since most signals fire late)
    parser.add_argument("--late-confidences", default="0.65",
                        help="ZoneConfig.late_min_confidence values")
    parser.add_argument("--late-zs", default="0.5",
                        help="ZoneConfig.late_min_z values")
    parser.add_argument("--late-edges", default="0.08",
                        help="ZoneConfig.late_min_edge values")
    parser.add_argument("--primary-zs", default="1.0",
                        help="ZoneConfig.primary_min_z values")
    parser.add_argument("--position-size", type=float, default=5.0)
    parser.add_argument("--fee-rate", type=float, default=0.072)
    parser.add_argument("--btc-csv", default="data/btcusdt_1s_7d.csv")
    parser.add_argument("--cache-dir", default="data/pmxt_cache")
    parser.add_argument("--output", default="logs/l2_sweep.json")
    args = parser.parse_args()

    start = parse_dt(args.start)
    end = start + timedelta(hours=args.hours)

    confidences = parse_floats(args.confidences)
    edges = parse_floats(args.edges)
    vols = parse_floats(args.vols)
    late_confs = parse_floats(args.late_confidences)
    late_zs = parse_floats(args.late_zs)
    late_edges = parse_floats(args.late_edges)
    primary_zs = parse_floats(args.primary_zs)

    n_runs = (
        len(confidences) * len(edges) * len(vols)
        * len(late_confs) * len(late_zs) * len(late_edges) * len(primary_zs)
    )

    print(f"\n  L2 SWEEP")
    print(f"  Range:        {start} → {end} ({args.hours}h)")
    print(f"  Preset:       {args.vps}")
    print(f"  Grid:         {n_runs} runs total")
    print(f"  Primary conf: {confidences}")
    print(f"  Primary edge: {edges}")
    print(f"  Primary z:    {primary_zs}")
    print(f"  Late conf:    {late_confs}")
    print(f"  Late z:       {late_zs}")
    print(f"  Late edge:    {late_edges}")
    print(f"  Vol:          {vols}\n")

    # ── Shared setup ──
    t0 = time.time()
    btc = BTCHistory()
    if Path(args.btc_csv).exists():
        btc.load_csv(args.btc_csv)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    if (
        btc.n_ticks == 0
        or btc.first_timestamp() > start_ms
        or btc.last_timestamp() < end_ms
    ):
        btc.load_from_binance(
            start - timedelta(hours=2),
            end + timedelta(hours=1),
            interval="1m",
        )
    print(f"  BTC ticks: {btc.n_ticks} ({time.time()-t0:.1f}s)")

    t1 = time.time()
    registry = CandleRegistry()
    registry.fetch_range(start - timedelta(hours=1), end + timedelta(hours=1))
    print(f"  Registry: {registry.n_contracts} contracts ({time.time()-t1:.1f}s)")
    if registry.n_contracts == 0:
        print("ERROR: no candle contracts in range", file=sys.stderr)
        return 1

    loader = PMXTLoader(cache_dir=args.cache_dir)
    latency = PRESETS[args.vps]

    # ── Build the parameter grid as a flat list of dicts ──
    # Each cell is one strategy run.
    base_zone = zone_config_from_settings()
    cells: list[dict] = []
    for conf in confidences:
        for edge in edges:
            for vol in vols:
                for lc in late_confs:
                    for lz in late_zs:
                        for le in late_edges:
                            for pz in primary_zs:
                                cells.append({
                                    "min_confidence": conf,
                                    "min_edge": edge,
                                    "vol": vol,
                                    "late_min_confidence": lc,
                                    "late_min_z": lz,
                                    "late_min_edge": le,
                                    "primary_min_z": pz,
                                })

    # ── Sweep ──
    runs: list[dict] = []
    grid_t0 = time.time()
    for run_idx, cell in enumerate(cells, 1):
        zone_cfg = ZoneConfig(
            early_min_confidence=base_zone.early_min_confidence,
            early_min_z=base_zone.early_min_z,
            early_min_edge=base_zone.early_min_edge,
            primary_min_z=cell["primary_min_z"],
            late_min_confidence=cell["late_min_confidence"],
            late_min_z=cell["late_min_z"],
            late_min_edge=cell["late_min_edge"],
            dead_zone_lo=base_zone.dead_zone_lo,
            dead_zone_hi=base_zone.dead_zone_hi,
            min_price=base_zone.min_price,
            max_price=base_zone.max_price,
            edge_cap=base_zone.edge_cap,
        )
        cfg = StrategyConfig(
            min_confidence=cell["min_confidence"],
            min_edge=cell["min_edge"],
            realized_vol=cell["vol"],
            position_size_usd=args.position_size,
            fee_rate=args.fee_rate,
            zone_config=zone_cfg,
        )
        adapter = CandleStrategyAdapter(
            registry=registry, btc_history=btc, config=cfg,
        )
        engine = L2BacktestEngine(loader=loader, latency=latency)

        t = time.time()
        engine.replay(
            start=start, end=end,
            token_ids=registry.all_token_ids(),
            strategy=adapter.on_event,
            fee_rate=cfg.fee_rate,
        )
        results = resolve_backtest(engine, adapter, btc)
        dt = time.time() - t

        # Pull adapter zone-mix and skip stats so we can see WHY a cell
        # produced few trades — invaluable for tuning the next sweep.
        a = adapter.summary()
        rec = {
            **cell,
            "trades": results.n_trades,
            "wins": results.n_wins,
            "losses": results.n_losses,
            "win_rate": results.win_rate,
            "pnl": results.total_pnl,
            "sharpe": results.sharpe,
            "fees": results.total_fees,
            "slippage": results.total_slippage,
            "by_zone": dict(a.get("by_zone", {})),
            "top_skip_reasons": a.get("top_skip_reasons", [])[:5],
            "duration_s": dt,
        }
        runs.append(rec)
        print(
            f"  [{run_idx:>2d}/{n_runs}] "
            f"L_conf={cell['late_min_confidence']:.2f} L_z={cell['late_min_z']:.2f} "
            f"L_edge={cell['late_min_edge']:.2f} P_z={cell['primary_min_z']:.2f} "
            f"→ n={results.n_trades:<3d} wr={results.win_rate*100:.0f}% "
            f"pnl=${results.total_pnl:+.2f} sharpe={results.sharpe:.2f} "
            f"zones={dict(a.get('by_zone', {}))} ({dt:.0f}s)"
        )

    grid_elapsed = time.time() - grid_t0

    # ── Ranking ──
    # With many param axes the simple grid-neighbour stability score
    # from before doesn't apply, so rank by (a) PnL and (b) raw Sharpe
    # — operator can pick. We deliberately do NOT pick "the best" cell
    # for them, since on small samples that's noise-fitting.
    by_pnl = sorted(runs, key=lambda r: (r["pnl"], r["trades"]), reverse=True)
    by_sharpe = sorted(runs, key=lambda r: r["sharpe"], reverse=True)
    by_trades = sorted(runs, key=lambda r: (r["trades"], r["pnl"]), reverse=True)

    print(f"\n{'='*78}")
    print("  TOP 5 BY PnL")
    print(f"{'='*78}")
    for i, rec in enumerate(by_pnl[:5], 1):
        print(
            f"  {i}. L_conf={rec['late_min_confidence']:.2f} L_z={rec['late_min_z']:.2f} "
            f"L_edge={rec['late_min_edge']:.2f} P_z={rec['primary_min_z']:.2f} "
            f"| pnl=${rec['pnl']:+.2f} n={rec['trades']} wr={rec['win_rate']*100:.0f}% "
            f"sharpe={rec['sharpe']:.2f}"
        )

    print(f"\n{'='*78}")
    print("  TOP 5 BY TRADE COUNT")
    print(f"{'='*78}")
    for i, rec in enumerate(by_trades[:5], 1):
        print(
            f"  {i}. L_conf={rec['late_min_confidence']:.2f} L_z={rec['late_min_z']:.2f} "
            f"L_edge={rec['late_min_edge']:.2f} P_z={rec['primary_min_z']:.2f} "
            f"| n={rec['trades']} pnl=${rec['pnl']:+.2f} wr={rec['win_rate']*100:.0f}% "
            f"sharpe={rec['sharpe']:.2f}"
        )

    print(f"\n{'='*78}")
    print("  TOP 5 BY SHARPE (warning: noise-fits on small n)")
    print(f"{'='*78}")
    for i, rec in enumerate(by_sharpe[:5], 1):
        print(
            f"  {i}. L_conf={rec['late_min_confidence']:.2f} L_z={rec['late_min_z']:.2f} "
            f"L_edge={rec['late_min_edge']:.2f} P_z={rec['primary_min_z']:.2f} "
            f"| sharpe={rec['sharpe']:.2f} n={rec['trades']} pnl=${rec['pnl']:+.2f}"
        )

    out = {
        "config": {
            "start": start.isoformat(), "end": end.isoformat(), "hours": args.hours,
            "vps": args.vps, "position_size": args.position_size, "fee_rate": args.fee_rate,
            "confidences": confidences, "edges": edges, "vols": vols,
            "late_confidences": late_confs, "late_zs": late_zs, "late_edges": late_edges,
            "primary_zs": primary_zs,
        },
        "runs": runs,
        "best_by_pnl": by_pnl[:5],
        "best_by_trades": by_trades[:5],
        "best_by_sharpe": by_sharpe[:5],
        "grid_elapsed_s": grid_elapsed,
    }
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(out, indent=2))
    print(f"\n  Saved to {args.output} ({grid_elapsed:.0f}s total)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
