#!/usr/bin/env python3
"""Parallel parameter sweep over the L2 backtest engine.

Loads BTC + registry + PMXT cache once in the main process, then forks
workers that inherit the shared data via copy-on-write. Each worker runs
one grid cell independently — zero serialization overhead.

Usage:
    uv run python scripts/run_l2_sweep.py \
        --start 2026-04-10T00 --hours 48 --vps dublin \
        --workers 0 \
        --late-zs 0.3,0.5 \
        --terminal-confidences 0.45,0.50,0.55,0.60 \
        --terminal-zs 0.2,0.3,0.5 \
        --terminal-edges 0.02,0.03,0.05 \
        --output logs/l2_megasweep.json

    # --workers 0 = all cores (default)
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
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

# ── Module globals — set in main, inherited by forked workers via COW ──
_btc: BTCHistory | None = None
_registry: CandleRegistry | None = None
_loader: PMXTLoader | None = None
_latency = None
_base_zone: ZoneConfig | None = None
_start: datetime | None = None
_end: datetime | None = None
_prefer_maker: bool = False
_position_size: float = 5.0
_fee_rate: float = 0.072
_token_ids: list[str] | None = None  # filtered token_ids for the sweep


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


def _run_cell(cell: dict) -> dict:
    """Run a single grid cell. Executed in a forked worker process.

    Uses module-level globals (_btc, _registry, etc.) that were set in
    the parent process before forking — inherited via COW, zero copy cost.
    """
    zone_cfg = ZoneConfig(
        early_min_confidence=_base_zone.early_min_confidence,
        early_min_z=_base_zone.early_min_z,
        early_min_edge=_base_zone.early_min_edge,
        primary_min_z=cell["primary_min_z"],
        late_min_confidence=cell["late_min_confidence"],
        late_min_z=cell["late_min_z"],
        late_min_edge=cell["late_min_edge"],
        terminal_min_confidence=cell["terminal_min_confidence"],
        terminal_min_z=cell["terminal_min_z"],
        terminal_min_edge=cell["terminal_min_edge"],
        dead_zone_lo=_base_zone.dead_zone_lo,
        dead_zone_hi=_base_zone.dead_zone_hi,
        min_price=_base_zone.min_price,
        max_price=_base_zone.max_price,
        edge_cap=_base_zone.edge_cap,
    )
    cfg = StrategyConfig(
        min_confidence=cell["min_confidence"],
        min_edge=cell["min_edge"],
        realized_vol=cell["vol"],
        position_size_usd=_position_size,
        fee_rate=_fee_rate,
        prefer_maker=_prefer_maker,
        maker_fee_rate=0.0 if _prefer_maker else _fee_rate,
        zone_config=zone_cfg,
    )
    adapter = CandleStrategyAdapter(
        registry=_registry, btc_history=_btc, config=cfg,
    )
    engine = L2BacktestEngine(loader=_loader, latency=_latency)

    t = time.time()
    engine.replay(
        start=_start, end=_end,
        token_ids=_token_ids,
        strategy=adapter.on_event,
        fee_rate=cfg.fee_rate,
    )
    results = resolve_backtest(engine, adapter, _btc)
    dt = time.time() - t

    a = adapter.summary()
    return {
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


def main() -> int:
    global _btc, _registry, _loader, _latency, _base_zone
    global _start, _end, _prefer_maker, _position_size, _fee_rate, _token_ids

    parser = argparse.ArgumentParser(
        description="Parallel L2 parameter sweep across all CPU cores",
    )
    parser.add_argument("--start", required=True)
    parser.add_argument("--hours", type=int, default=2)
    parser.add_argument("--vps", choices=list(PRESETS), default="dublin")
    parser.add_argument("--asset", default="BTC",
                        help="Filter contracts by asset (BTC, ETH, SOL, or ALL)")
    parser.add_argument("--workers", type=int, default=0,
                        help="Number of parallel workers (0 = all cores)")
    # Primary-zone gates
    parser.add_argument("--confidences", default="0.60")
    parser.add_argument("--edges", default="0.07")
    parser.add_argument("--vols", default="0.50")
    # Late-zone gates
    parser.add_argument("--late-confidences", default="0.65")
    parser.add_argument("--late-zs", default="0.5")
    parser.add_argument("--late-edges", default="0.08")
    parser.add_argument("--primary-zs", default="1.0")
    # Terminal-zone gates
    parser.add_argument("--terminal-confidences", default="0.55")
    parser.add_argument("--terminal-zs", default="0.3")
    parser.add_argument("--terminal-edges", default="0.03")
    # Execution
    parser.add_argument("--prefer-maker", action="store_true")
    parser.add_argument("--position-size", type=float, default=5.0)
    parser.add_argument("--fee-rate", type=float, default=0.072)
    parser.add_argument("--btc-csv", default="data/btcusdt_1s_7d.csv")
    parser.add_argument("--cache-dir", default="data/pmxt_cache")
    parser.add_argument("--output", default="logs/l2_sweep.json")
    args = parser.parse_args()

    n_workers = args.workers if args.workers > 0 else os.cpu_count() or 1
    _start = parse_dt(args.start)
    _end = _start + timedelta(hours=args.hours)
    _prefer_maker = args.prefer_maker
    _position_size = args.position_size
    _fee_rate = args.fee_rate

    confidences = parse_floats(args.confidences)
    edges = parse_floats(args.edges)
    vols = parse_floats(args.vols)
    late_confs = parse_floats(args.late_confidences)
    late_zs = parse_floats(args.late_zs)
    late_edges = parse_floats(args.late_edges)
    primary_zs = parse_floats(args.primary_zs)
    terminal_confs = parse_floats(args.terminal_confidences)
    terminal_zs = parse_floats(args.terminal_zs)
    terminal_edges = parse_floats(args.terminal_edges)

    # ── Build grid ──
    cells: list[dict] = []
    for conf in confidences:
        for edge in edges:
            for vol in vols:
                for lc in late_confs:
                    for lz in late_zs:
                        for le in late_edges:
                            for pz in primary_zs:
                                for tc in terminal_confs:
                                    for tz in terminal_zs:
                                        for te in terminal_edges:
                                            cells.append({
                                                "min_confidence": conf,
                                                "min_edge": edge,
                                                "vol": vol,
                                                "late_min_confidence": lc,
                                                "late_min_z": lz,
                                                "late_min_edge": le,
                                                "primary_min_z": pz,
                                                "terminal_min_confidence": tc,
                                                "terminal_min_z": tz,
                                                "terminal_min_edge": te,
                                            })
    n_runs = len(cells)

    print(f"\n  L2 MEGASWEEP (parallel)")
    print(f"  Range:          {_start} → {_end} ({args.hours}h)")
    print(f"  Asset:          {args.asset.upper()}")
    print(f"  Preset:         {args.vps}")
    print(f"  Workers:        {n_workers} / {os.cpu_count()} cores")
    print(f"  Grid:           {n_runs} runs")
    print(f"  Late z:         {late_zs}")
    print(f"  Late edge:      {late_edges}")
    print(f"  Terminal conf:  {terminal_confs}")
    print(f"  Terminal z:     {terminal_zs}")
    print(f"  Terminal edge:  {terminal_edges}")
    print(f"  Prefer maker:   {_prefer_maker}")

    # ── Load shared data (inherited by workers via fork COW) ──
    t0 = time.time()
    _btc = BTCHistory()
    if Path(args.btc_csv).exists():
        _btc.load_csv(args.btc_csv)
    start_ms = int(_start.timestamp() * 1000)
    end_ms = int(_end.timestamp() * 1000)
    if (
        _btc.n_ticks == 0
        or _btc.first_timestamp() > start_ms
        or _btc.last_timestamp() < end_ms
    ):
        _btc.load_from_binance(
            _start - timedelta(hours=2),
            _end + timedelta(hours=1),
            interval="1m",
        )
    print(f"  BTC ticks:      {_btc.n_ticks} ({time.time()-t0:.1f}s)")

    t1 = time.time()
    _registry = CandleRegistry()
    _registry.fetch_range(_start - timedelta(hours=1), _end + timedelta(hours=1))
    print(f"  Registry:       {_registry.n_contracts} contracts ({time.time()-t1:.1f}s)")
    if _registry.n_contracts == 0:
        print("ERROR: no candle contracts in range", file=sys.stderr)
        return 1

    # Filter token_ids by asset
    asset_filter = args.asset.upper()
    if asset_filter == "ALL":
        _token_ids = _registry.all_token_ids()
        asset_label = "ALL"
    else:
        filtered = _registry.filter_by_asset(asset_filter)
        _token_ids = []
        for c in filtered:
            _token_ids.append(c.up_token_id)
            _token_ids.append(c.down_token_id)
        asset_label = asset_filter
    print(f"  Asset filter:   {asset_label} ({len(_token_ids)} token_ids from {len(_token_ids)//2} contracts)")

    _loader = PMXTLoader(cache_dir=args.cache_dir)
    _latency = PRESETS[args.vps]
    _base_zone = zone_config_from_settings()

    est_per_run = 300 / max(48 / args.hours, 1)  # rough: ~300s per 48h replay
    est_total = est_per_run * n_runs / n_workers
    print(f"  Est. time:      ~{est_total/60:.0f} min ({est_per_run:.0f}s/run × {n_runs} / {n_workers} workers)")
    print()

    # ── Parallel sweep ──
    # Use fork context so workers inherit the loaded data via COW.
    # On macOS, default is 'spawn' since 3.8 — force 'fork'.
    ctx = mp.get_context("fork")
    grid_t0 = time.time()

    completed = 0
    runs: list[dict] = []

    with ctx.Pool(processes=n_workers) as pool:
        for rec in pool.imap_unordered(_run_cell, cells):
            completed += 1
            runs.append(rec)
            # Progress line
            pct = completed / n_runs * 100
            print(
                f"  [{completed:>3d}/{n_runs}] {pct:5.1f}%  "
                f"L_z={rec['late_min_z']:.2f} T_z={rec['terminal_min_z']:.2f} "
                f"T_conf={rec['terminal_min_confidence']:.2f} "
                f"→ n={rec['trades']:<3d} wr={rec['win_rate']*100:.0f}% "
                f"pnl=${rec['pnl']:+.2f} sharpe={rec['sharpe']:.2f} "
                f"zones={rec.get('by_zone', {})} ({rec['duration_s']:.0f}s)"
            )

    grid_elapsed = time.time() - grid_t0
    speedup = sum(r["duration_s"] for r in runs) / max(grid_elapsed, 1)

    # ── Ranking ──
    by_pnl = sorted(runs, key=lambda r: (r["pnl"], r["trades"]), reverse=True)
    by_sharpe = sorted(runs, key=lambda r: r["sharpe"], reverse=True)
    by_trades = sorted(runs, key=lambda r: (r["trades"], r["pnl"]), reverse=True)

    def _fmt(rec: dict) -> str:
        return (
            f"L_z={rec['late_min_z']:.2f} L_e={rec['late_min_edge']:.2f} "
            f"T_conf={rec['terminal_min_confidence']:.2f} "
            f"T_z={rec['terminal_min_z']:.2f} T_e={rec['terminal_min_edge']:.2f}"
        )

    print(f"\n{'='*80}")
    print(f"  RESULTS — {n_runs} cells, {grid_elapsed:.0f}s wall, {speedup:.1f}x speedup on {n_workers} cores")
    print(f"{'='*80}")

    print(f"\n  TOP 5 BY PnL")
    for i, rec in enumerate(by_pnl[:5], 1):
        print(
            f"  {i}. {_fmt(rec)} "
            f"| pnl=${rec['pnl']:+.2f} n={rec['trades']} "
            f"wr={rec['win_rate']*100:.0f}% sharpe={rec['sharpe']:.2f} "
            f"zones={rec.get('by_zone', {})}"
        )

    print(f"\n  TOP 5 BY TRADE COUNT")
    for i, rec in enumerate(by_trades[:5], 1):
        print(
            f"  {i}. {_fmt(rec)} "
            f"| n={rec['trades']} pnl=${rec['pnl']:+.2f} "
            f"wr={rec['win_rate']*100:.0f}% sharpe={rec['sharpe']:.2f}"
        )

    print(f"\n  TOP 5 BY SHARPE (caution: noise on small n)")
    for i, rec in enumerate(by_sharpe[:5], 1):
        print(
            f"  {i}. {_fmt(rec)} "
            f"| sharpe={rec['sharpe']:.2f} n={rec['trades']} "
            f"pnl=${rec['pnl']:+.2f} wr={rec['win_rate']*100:.0f}%"
        )

    # ── Terminal zone breakdown ──
    terminal_cells = [r for r in runs if r.get("by_zone", {}).get("terminal", 0) > 0]
    if terminal_cells:
        total_terminal = sum(r["by_zone"].get("terminal", 0) for r in terminal_cells)
        print(f"\n  TERMINAL ZONE: {len(terminal_cells)}/{n_runs} cells had terminal trades, "
              f"{total_terminal} total terminal entries")
        best_terminal = max(terminal_cells, key=lambda r: r["by_zone"].get("terminal", 0))
        print(f"  Most terminal trades: {_fmt(best_terminal)} "
              f"→ {best_terminal['by_zone'].get('terminal', 0)} terminal, "
              f"wr={best_terminal['win_rate']*100:.0f}%")
    else:
        print(f"\n  WARNING: No terminal zone trades in any cell!")

    # ── Save ──
    out = {
        "config": {
            "start": _start.isoformat(), "end": _end.isoformat(), "hours": args.hours,
            "vps": args.vps, "workers": n_workers,
            "position_size": _position_size, "fee_rate": _fee_rate,
            "prefer_maker": _prefer_maker,
            "confidences": confidences, "edges": edges, "vols": vols,
            "late_confidences": late_confs, "late_zs": late_zs, "late_edges": late_edges,
            "primary_zs": primary_zs,
            "terminal_confidences": terminal_confs, "terminal_zs": terminal_zs,
            "terminal_edges": terminal_edges,
        },
        "runs": runs,
        "best_by_pnl": by_pnl[:10],
        "best_by_trades": by_trades[:10],
        "best_by_sharpe": by_sharpe[:10],
        "grid_elapsed_s": grid_elapsed,
        "speedup": speedup,
    }
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(out, indent=2))
    print(f"\n  Saved to {args.output}")
    print(f"  Wall time: {grid_elapsed:.0f}s ({speedup:.1f}x parallel speedup)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
