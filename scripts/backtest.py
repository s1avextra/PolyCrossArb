#!/usr/bin/env python3
"""Backtester: replay historical snapshots through the arb pipeline.

Since Polymarket doesn't provide historical price APIs directly,
this backtester takes periodic snapshots and replays them.

Usage:
  python -m scripts.backtest --snapshot-dir snapshots/ [--min-margin 0.02]
  python -m scripts.backtest --collect --interval 300  # collect snapshots every 5 min
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, "src")

from polycrossarb.arb.detector import detect_cross_market_arbs
from polycrossarb.data.client import PolymarketClient
from polycrossarb.data.models import Market, Outcome
from polycrossarb.graph.screener import find_event_partitions
from polycrossarb.solver.linear import solve_all_partitions

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
log = logging.getLogger("backtest")


async def collect_snapshots(output_dir: str, interval: float, max_snapshots: int):
    """Periodically snapshot market data for later backtesting."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    client = PolymarketClient()
    count = 0

    try:
        while count < max_snapshots:
            markets = await client.fetch_all_active_markets()
            ts = int(time.time())

            snapshot = []
            for m in markets:
                if not any(o.price > 0 for o in m.outcomes):
                    continue
                snapshot.append({
                    "condition_id": m.condition_id,
                    "question": m.question,
                    "event_id": m.event_id,
                    "event_title": m.event_title,
                    "neg_risk": m.neg_risk,
                    "outcomes": [{"name": o.name, "price": o.price, "token_id": o.token_id} for o in m.outcomes],
                    "volume": m.volume,
                    "liquidity": m.liquidity,
                })

            filename = out / f"snapshot_{ts}.json"
            with open(filename, "w") as f:
                json.dump({"timestamp": ts, "markets": snapshot}, f)

            log.info("Snapshot %d: %d markets -> %s", count + 1, len(snapshot), filename)
            count += 1

            if count < max_snapshots:
                await asyncio.sleep(interval)
    finally:
        await client.close()


def load_snapshot(path: Path) -> tuple[float, list[Market]]:
    """Load a snapshot file into Market objects."""
    with open(path) as f:
        data = json.load(f)

    ts = data["timestamp"]
    markets: list[Market] = []

    for raw in data["markets"]:
        outcomes = [
            Outcome(token_id=o["token_id"], name=o["name"], price=o["price"])
            for o in raw["outcomes"]
        ]
        markets.append(Market(
            condition_id=raw["condition_id"],
            question=raw["question"],
            slug="",
            outcomes=outcomes,
            event_id=raw.get("event_id", ""),
            event_title=raw.get("event_title", ""),
            neg_risk=raw.get("neg_risk", False),
            volume=raw.get("volume", 0),
            liquidity=raw.get("liquidity", 0),
        ))

    return ts, markets


def replay_snapshots(snapshot_dir: str, min_margin: float, max_position: float):
    """Replay all snapshots and compute theoretical P&L."""
    snap_dir = Path(snapshot_dir)
    files = sorted(snap_dir.glob("snapshot_*.json"))

    if not files:
        log.error("No snapshot files found in %s", snapshot_dir)
        return

    log.info("Replaying %d snapshots from %s", len(files), snapshot_dir)

    total_profit = 0.0
    total_trades = 0
    opportunities_seen = 0

    total_fees = 0.0
    total_exits = 0
    total_exit_pnl = 0.0
    bankroll = 100.0  # simulate dynamic bankroll

    # Track positions across snapshots for early exit simulation
    from polycrossarb.risk.manager import RiskManager
    risk = RiskManager(initial_bankroll=100.0, state_dir="/tmp/backtest_state")

    for snap_file in files:
        ts, markets = load_snapshot(snap_file)
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

        # Check early exits on existing positions before new trades
        current_prices = {
            f"{m.condition_id}:0": m.outcomes[0].price
            for m in markets if m.outcomes and m.outcomes[0].price > 0
        }
        exits = risk.check_early_exits(current_prices)
        for key, exit_price in exits:
            pnl = risk.close_position(key, exit_price)
            total_exit_pnl += pnl
            total_exits += 1
            bankroll = risk.effective_bankroll

        # Detect arbs
        opps = detect_cross_market_arbs(markets, min_margin=min_margin, exclusive_only=True)
        opportunities_seen += len(opps)

        if not opps:
            continue

        # Only solve top 50 arb partitions (same as live pipeline)
        top_opps = sorted(opps, key=lambda o: o.margin, reverse=True)[:50]
        arb_event_ids = {o.markets[0].event_id for o in top_opps if o.markets}

        partitions = find_event_partitions(markets)
        neg_risk = [p for p in partitions if p.is_neg_risk and p.event_id in arb_event_ids]

        available = risk.available_capital
        results = solve_all_partitions(
            partitions=neg_risk,
            max_position_per_event=min(max_position, risk.effective_bankroll * 0.2),
            max_total_exposure=available,
            min_profit=0.10,
        )

        # Record trades in risk manager
        for r in results:
            allowed, _ = risk.check_trade(r, "")
            if allowed:
                risk.record_trade(r.orders, paper=True)
                risk.record_fees(r.total_fees)

        snap_profit = sum(r.guaranteed_profit for r in results)
        snap_fees = sum(r.total_fees for r in results)
        snap_trades = len(results)
        total_profit += snap_profit
        total_fees += snap_fees
        total_trades += snap_trades
        bankroll = risk.effective_bankroll

        if snap_trades > 0 or total_exits > 0:
            log.info(
                "[%s] %d opps, %d trades, %d exits, profit $%.2f, bankroll $%.2f",
                time_str, len(opps), snap_trades, len(exits), snap_profit, bankroll,
            )

    # Clean up temp state
    import os
    try:
        os.remove("/tmp/backtest_state/state.json")
    except FileNotFoundError:
        pass

    print(f"\n{'='*60}")
    print(f"  Backtest Results")
    print(f"  Snapshots: {len(files)}")
    print(f"  Opportunities seen: {opportunities_seen}")
    print(f"  Trades executed: {total_trades}")
    print(f"  Early exits: {total_exits} (P&L: ${total_exit_pnl:.2f})")
    print(f"  Starting bankroll: $100.00")
    print(f"  Final bankroll: ${bankroll:.2f}")
    print(f"  Net profit: ${total_profit:.2f}")
    print(f"  Total fees: ${total_fees:.4f}")
    print(f"  Return: {(bankroll/100-1)*100:.1f}%")
    if total_trades:
        print(f"  Avg profit/trade: ${total_profit/total_trades:.2f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PolyCrossArb backtester")
    sub = parser.add_subparsers(dest="command")

    collect_p = sub.add_parser("collect", help="Collect market snapshots")
    collect_p.add_argument("--output-dir", default="snapshots")
    collect_p.add_argument("--interval", type=float, default=300, help="Seconds between snapshots")
    collect_p.add_argument("--max", type=int, default=288, help="Max snapshots (288 = 24h at 5min)")

    replay_p = sub.add_parser("replay", help="Replay snapshots for backtesting")
    replay_p.add_argument("--snapshot-dir", default="snapshots")
    replay_p.add_argument("--min-margin", type=float, default=0.02)
    replay_p.add_argument("--max-position", type=float, default=100)

    args = parser.parse_args()

    if args.command == "collect":
        asyncio.run(collect_snapshots(args.output_dir, args.interval, args.max))
    elif args.command == "replay":
        replay_snapshots(args.snapshot_dir, args.min_margin, args.max_position)
    else:
        parser.print_help()
