#!/usr/bin/env python3
"""Concurrency and position-cap analysis for harness trade dumps.

Reads `logs/harness_*.json` that contains per-trade data (entry_ts, close_ts,
cost, pnl_after_fee, ...) and reports:

  1. Peak concurrent positions over time
  2. Peak $ exposure over time
  3. Cap-conditional PnL for a sweep of position-count caps
  4. Cap-conditional PnL for a sweep of $ exposure caps
  5. "Golden middle" — best PnL/$exposure (capital efficiency) trade-off

Usage:
    uv run python scripts/analyze_concurrency.py logs/harness_apr12_trades.json

Both the position-count and the dollar-exposure simulations do the SAME thing
conceptually: walk the trades in chronological fill order, evict positions
that have resolved, and reject new signals that would exceed the cap. Each
rejected trade contributes $0 to pnl (as if the live risk manager blocked it).
"""
from __future__ import annotations

import argparse
import heapq
import json
import math
import sys
from pathlib import Path


def load_trades(path: Path, strategy: str | None = None) -> tuple[list[dict], str]:
    data = json.loads(path.read_text())
    runs = data["runs"]
    if strategy:
        matches = [r for r in runs if r["strategy"] == strategy]
        if not matches:
            print(f"ERROR: no run named '{strategy}' in {path}", file=sys.stderr)
            sys.exit(1)
        run = matches[0]
    else:
        run = runs[0]
    trades = run.get("trades", [])
    if not trades:
        print(f"ERROR: run '{run['strategy']}' has no `trades` field — re-run harness with instrumented version", file=sys.stderr)
        sys.exit(1)
    trades = sorted(trades, key=lambda t: t["entry_ts"])
    return trades, run["strategy"]


def concurrency_profile(trades: list[dict]) -> dict:
    """Walk trades chronologically and compute peak/time-average concurrency.

    We track both the count of concurrently-open positions and the $ exposure
    (sum of cost of open positions) over the span of the backtest.
    """
    events: list[tuple[float, int, float]] = []
    for t in trades:
        events.append((t["entry_ts"], +1, +t["cost"]))
        events.append((t["close_ts"], -1, -t["cost"]))
    events.sort(key=lambda e: (e[0], e[1]))

    peak_positions = 0
    peak_dollars = 0.0
    open_positions = 0
    open_dollars = 0.0
    cumulative_position_seconds = 0.0
    cumulative_dollar_seconds = 0.0
    prev_ts: float | None = None

    for ts, dpos, ddol in events:
        if prev_ts is not None and ts > prev_ts:
            cumulative_position_seconds += open_positions * (ts - prev_ts)
            cumulative_dollar_seconds += open_dollars * (ts - prev_ts)
        open_positions += dpos
        open_dollars += ddol
        peak_positions = max(peak_positions, open_positions)
        peak_dollars = max(peak_dollars, open_dollars)
        prev_ts = ts

    if not events:
        return {}
    span_s = events[-1][0] - events[0][0]
    span_s = max(span_s, 1.0)
    return {
        "peak_positions": peak_positions,
        "peak_dollars": round(peak_dollars, 4),
        "avg_positions_time_weighted": round(cumulative_position_seconds / span_s, 3),
        "avg_dollars_time_weighted": round(cumulative_dollar_seconds / span_s, 4),
        "span_hours": round(span_s / 3600, 2),
    }


def simulate_count_cap(trades: list[dict], cap: int) -> dict:
    """Walk trades chronologically; reject new trades when open count >= cap."""
    open_heap: list[tuple[float, int]] = []  # (close_ts, idx)
    accepted: list[dict] = []
    rejected = 0

    for i, t in enumerate(trades):
        entry = t["entry_ts"]
        while open_heap and open_heap[0][0] <= entry:
            heapq.heappop(open_heap)
        if len(open_heap) >= cap:
            rejected += 1
            continue
        accepted.append(t)
        heapq.heappush(open_heap, (t["close_ts"], i))

    return _summarize(accepted, rejected, trades)


def simulate_dollar_cap(trades: list[dict], cap: float) -> dict:
    """Walk trades chronologically; reject when open cost sum + new cost > cap."""
    open_positions: list[dict] = []
    accepted: list[dict] = []
    rejected = 0

    for t in trades:
        entry = t["entry_ts"]
        open_positions = [p for p in open_positions if p["close_ts"] > entry]
        current_exposure = sum(p["cost"] for p in open_positions)
        if current_exposure + t["cost"] > cap + 1e-9:
            rejected += 1
            continue
        accepted.append(t)
        open_positions.append(t)

    return _summarize(accepted, rejected, trades)


def _summarize(accepted: list[dict], rejected: int, all_trades: list[dict]) -> dict:
    n = len(accepted)
    wins = sum(1 for t in accepted if t["won"])
    pnl = sum(t["pnl_after_fee"] for t in accepted)
    pnls = [t["pnl_after_fee"] for t in accepted]
    mean = sum(pnls) / n if n else 0.0
    std = math.sqrt(sum((p - mean) ** 2 for p in pnls) / n) if n >= 2 else 0.0
    sharpe = mean / std if std > 0 else 0.0

    # Running drawdown: settle accepted trades in close_ts order and walk
    # the equity curve, tracking max peak-to-trough.
    max_dd = 0.0
    if accepted:
        settled = sorted(accepted, key=lambda t: t["close_ts"])
        equity = 0.0
        peak = 0.0
        for t in settled:
            equity += t["pnl_after_fee"]
            peak = max(peak, equity)
            max_dd = min(max_dd, equity - peak)

    peak_prof = concurrency_profile(accepted) if accepted else {"peak_dollars": 0.0}
    return {
        "accepted": n,
        "rejected": rejected,
        "reject_rate": round(rejected / max(len(all_trades), 1), 3),
        "win_rate": round(wins / n, 4) if n else 0.0,
        "pnl": round(pnl, 4),
        "avg_pnl": round(mean, 4),
        "sharpe": round(sharpe, 3),
        "peak_dollars_accepted": peak_prof.get("peak_dollars", 0.0),
        "max_drawdown": round(max_dd, 4),
    }


def print_count_cap_table(trades: list[dict], caps: list[int]) -> None:
    # Use the largest cap as the "uncapped" reference so the % column is honest.
    uncapped = simulate_count_cap(trades, max(caps))
    base_pnl = uncapped["pnl"]

    print()
    print("  POSITION-COUNT CAP SWEEP  (baseline = cap infinity)")
    print(f"  {'cap':>4} {'trades':>7} {'WR':>7} {'pnl $':>10} {'avg $':>8} {'sharpe':>7} "
          f"{'peak$':>7} {'maxDD$':>8} {'rej%':>6} {'pnl%':>7} {'$pnl/pk$':>10}")
    print("  " + "-" * 90)
    for cap in caps:
        r = simulate_count_cap(trades, cap)
        pnl_frac = (r["pnl"] / base_pnl) if base_pnl else 0
        pnl_per_pk = r["pnl"] / r["peak_dollars_accepted"] if r["peak_dollars_accepted"] else 0
        print(
            f"  {cap:>4} {r['accepted']:>7} {r['win_rate']*100:>6.1f}% "
            f"{r['pnl']:>+10.2f} {r['avg_pnl']:>+8.3f} {r['sharpe']:>7.3f} "
            f"{r['peak_dollars_accepted']:>7.2f} {r['max_drawdown']:>+8.3f} "
            f"{r['reject_rate']*100:>5.1f}% {pnl_frac*100:>6.1f}% {pnl_per_pk:>+9.2f}"
        )


def print_dollar_cap_table(trades: list[dict], caps: list[float]) -> None:
    uncapped = simulate_dollar_cap(trades, max(caps))
    base_pnl = uncapped["pnl"]

    print()
    print("  DOLLAR-EXPOSURE CAP SWEEP  (baseline = cap infinity)")
    print(f"  {'cap$':>7} {'trades':>7} {'WR':>7} {'pnl $':>10} {'avg $':>8} {'sharpe':>7} "
          f"{'peak$':>7} {'maxDD$':>8} {'rej%':>6} {'pnl%':>7} {'$pnl/cap':>10}")
    print("  " + "-" * 90)
    for cap in caps:
        r = simulate_dollar_cap(trades, cap)
        pnl_frac = (r["pnl"] / base_pnl) if base_pnl else 0
        pnl_per_cap = r["pnl"] / cap if cap else 0
        print(
            f"  {cap:>7.0f} {r['accepted']:>7} {r['win_rate']*100:>6.1f}% "
            f"{r['pnl']:>+10.2f} {r['avg_pnl']:>+8.3f} {r['sharpe']:>7.3f} "
            f"{r['peak_dollars_accepted']:>7.2f} {r['max_drawdown']:>+8.3f} "
            f"{r['reject_rate']*100:>5.1f}% {pnl_frac*100:>6.1f}% {pnl_per_cap:>+9.2f}"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--strategy", default=None,
                        help="Which run's trades to analyze (default: first)")
    parser.add_argument("--count-caps", default="1,2,3,4,5,6,8,10,15,20,30,50,1000")
    parser.add_argument("--dollar-caps", default="3,5,10,15,20,25,50,100,250,1000,10000")
    args = parser.parse_args()

    trades, strategy = load_trades(Path(args.path), args.strategy)
    print(f"\n  CONCURRENCY ANALYSIS — {strategy}  ({len(trades)} trades)")

    profile = concurrency_profile(trades)
    print()
    print("  UNCAPPED PROFILE")
    for k, v in profile.items():
        print(f"    {k:<35} {v}")

    count_caps = [int(x) for x in args.count_caps.split(",")]
    dollar_caps = [float(x) for x in args.dollar_caps.split(",")]

    print_count_cap_table(trades, count_caps)
    print_dollar_cap_table(trades, dollar_caps)

    print()
    print("  NOTES")
    print("  - All numbers are $1-per-trade position sizing from the harness run")
    print("  - 'pnl $' is total $ PnL across accepted trades (after fees)")
    print("  - 'pk$' is peak concurrent $ exposure from accepted trades")
    print("  - 'rej%' is the fraction of signals a live risk manager would drop at that cap")

    return 0


if __name__ == "__main__":
    sys.exit(main())
