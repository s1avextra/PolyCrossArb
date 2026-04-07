#!/usr/bin/env python3
"""Run backtest on historical BTC data.

Usage:
  python scripts/run_backtest.py [--data data/btcusdt_1s_7d.csv] [--window 5] [--grid]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time

sys.path.insert(0, "src")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

from polycrossarb.backtest.window_builder import load_ticks, build_windows, build_all_window_sizes
from polycrossarb.backtest.replay_engine import run_backtest, grid_search, BacktestConfig
from polycrossarb.backtest.evaluator import evaluate, print_report, save_results


def main(data_path: str, window: int | None, do_grid: bool):
    t0 = time.time()

    # Load data
    print(f"Loading ticks from {data_path}...")
    ticks = load_ticks(data_path)
    print(f"  {len(ticks):,} ticks loaded")
    print()

    # Build windows
    if window:
        windows_map = {window: build_windows(ticks, window)}
    else:
        windows_map = build_all_window_sizes(ticks)

    for w, wins in windows_map.items():
        print(f"  {w}-min windows: {len(wins):,}")

    if do_grid:
        # Grid search across all window sizes
        print("\n" + "=" * 60)
        print("  GRID SEARCH")
        print("=" * 60)

        for w, wins in windows_map.items():
            if not wins:
                continue
            print(f"\n--- {w}-minute windows ---")
            results = grid_search(wins)

            # Print top 5
            for i, (cfg, res) in enumerate(results[:5]):
                print(
                    f"  #{i+1}: conf≥{cfg.min_confidence:.0%} "
                    f"edge≥{cfg.min_edge:.0%} "
                    f"elapsed≥{cfg.min_minutes_elapsed:.1f}m → "
                    f"{res.n_trades} trades, {res.win_rate:.0%} WR, "
                    f"${res.total_pnl:+.2f}"
                )

    else:
        # Single run with default config
        all_windows = []
        for wins in windows_map.values():
            all_windows.extend(wins)
        all_windows.sort(key=lambda w: w.start_ms)

        print(f"\nRunning backtest on {len(all_windows):,} windows...")
        result = run_backtest(all_windows)
        metrics = evaluate(result)

        report = print_report(metrics)
        print(report)

        # Save
        save_results(metrics, result.trades)
        print(f"\nResults saved to logs/backtest_results.json and logs/backtest_trades.csv")

    elapsed = time.time() - t0
    print(f"\nTotal time: {elapsed:.1f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/btcusdt_1s_7d.csv")
    parser.add_argument("--window", type=int, default=None,
                        help="Window size in minutes (5, 15, 60). Default: all")
    parser.add_argument("--grid", action="store_true",
                        help="Run grid search for optimal parameters")
    args = parser.parse_args()
    main(args.data, args.window, args.grid)
