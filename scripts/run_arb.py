#!/usr/bin/env python3
"""Standalone split/merge arbitrage pipeline.

Scans Polymarket for cross-market and single-market arbitrage using
real order book prices. Executes via buy_then_merge (underpriced)
or split_then_sell (overpriced).

Completely independent — own bankroll, own risk limits.

Usage:
  python scripts/run_arb.py [--mode paper|live] [--bankroll N] [--duration N]

  # Tune parameters:
  python scripts/run_arb.py --mode paper --min-margin 0.01 --scan-interval 15
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, "src")

from polycrossarb.config import settings
from polycrossarb.execution.executor import ExecutionMode
from polycrossarb.monitoring.logging_config import configure_logging
from polycrossarb.risk.manager import RiskManager
from polycrossarb.pipeline import Pipeline
from polycrossarb.data.client import PolymarketClient
from polycrossarb.crypto.candle_scanner import scan_candle_markets
from polycrossarb.arb.detector import detect_single_market_orderbook_arbs

configure_logging()


async def main(args):
    exec_mode = ExecutionMode.LIVE if args.mode == "live" else ExecutionMode.PAPER

    # Enable on-chain execution for split/merge
    os.environ["ENABLE_ONCHAIN_EXECUTION"] = "true"
    # Reload settings
    from polycrossarb import config
    config.settings = config.Settings()

    risk = RiskManager(
        initial_bankroll=args.bankroll,
        max_per_market=min(args.bankroll * 0.30, 20.0),
        state_dir="logs/arb",
    )

    pipeline = Pipeline(mode=exec_mode)
    # Inject shared risk manager
    pipeline._risk = risk
    pipeline._paper_executor.risk_manager = risk
    pipeline._live_executor.risk_manager = risk
    pipeline._hybrid_executor._risk = risk
    pipeline.scan_interval = args.scan_interval

    loop = asyncio.get_event_loop()
    for sig_name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_name, lambda: setattr(pipeline, '_running', False))

    if args.duration:
        async def stop_after():
            await asyncio.sleep(args.duration)
            pipeline._running = False
        asyncio.create_task(stop_after())

    print(f"{'='*55}")
    print(f"  ARB PIPELINE — {'LIVE' if args.mode == 'live' else 'PAPER'}")
    print(f"{'='*55}")
    print(f"  Bankroll:        ${args.bankroll:.2f}")
    print(f"  On-chain exec:   {config.settings.enable_onchain_execution}")
    print(f"  Min margin:      {args.min_margin:.1%}")
    print(f"  Min profit:      ${args.min_profit:.2f}")
    print(f"  Scan interval:   {args.scan_interval}s")
    print(f"  Max per market:  ${risk.max_per_market:.2f}")
    print(f"{'='*55}")
    print()

    start = time.time()

    # Run both: slow cross-market pipeline + fast candle book scanner
    async def fast_candle_scan():
        """Fast loop scanning candle contracts for merge/split (vidarx pattern).

        Historical data shows 9 opportunities/hour at 1% margin, but they're
        transient — need to scan every 5-10 seconds, not every 30.
        """
        client = PolymarketClient()
        trade_count = 0
        scan_count = 0

        while pipeline._running:
            try:
                scan_count += 1
                markets = await client.fetch_all_active_markets(min_liquidity=0)
                candle_contracts = scan_candle_markets(markets, max_hours=1.0, min_liquidity=50)

                # Get the underlying markets for candle contracts
                candle_market_ids = {c.market.condition_id for c in candle_contracts}
                candle_markets = [m for m in markets if m.condition_id in candle_market_ids
                                  and m.num_outcomes == 2]

                if candle_markets:
                    # Fetch order books (fast, parallel)
                    await client.enrich_with_order_books(candle_markets[:50], concurrency=20)

                    # Detect book-level arbs
                    arbs = detect_single_market_orderbook_arbs(candle_markets, min_margin=0.01)

                    if arbs:
                        for opp in arbs[:3]:  # max 3 per scan
                            if risk.available_capital < 1.0:
                                break

                            trade_count += 1
                            m = opp.markets[0]

                            # Log the opportunity
                            entry = {
                                "timestamp": time.time(),
                                "type": "arb_detected",
                                "strategy": opp.execution_strategy,
                                "arb_type": opp.arb_type,
                                "margin": round(opp.margin, 4),
                                "market": m.question[:50],
                                "condition_id": m.condition_id[:20],
                                "details": opp.details[:100],
                                "bankroll": round(risk.effective_bankroll, 2),
                                "mode": "paper",
                            }
                            Path("logs/arb").mkdir(parents=True, exist_ok=True)
                            with open("logs/arb/arb_trades.jsonl", "a") as f:
                                f.write(json.dumps(entry) + "\n")

                            print(f"  ARB: {opp.execution_strategy} margin={opp.margin:.1%} "
                                  f"{m.question[:40]}...")

                if scan_count % 10 == 0:
                    print(f"  [arb.fast_scan] cycle={scan_count} contracts={len(candle_markets)} "
                          f"trades={trade_count}")

            except Exception as e:
                if "Rate limited" not in str(e):
                    logging.debug("fast_scan error: %s", str(e)[:50])

            await asyncio.sleep(args.scan_interval)

        await client.close()

    await asyncio.gather(
        pipeline.run(max_cycles=args.max_cycles),
        fast_candle_scan(),
        return_exceptions=True,
    )
    elapsed = time.time() - start

    print(f"\n{'='*55}")
    print(f"  ARB RESULTS ({elapsed/60:.0f} min)")
    print(f"{'='*55}")
    print(f"  Cycles:          {pipeline._cycle_count}")
    print(f"  Bankroll:        ${risk.effective_bankroll:.2f}")
    print(f"  P&L:             ${risk.total_pnl:+.2f}")
    print(f"{'='*55}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Arb Split/Merge Pipeline")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--bankroll", type=float, default=6.03)
    parser.add_argument("--duration", type=int, default=None, help="Seconds to run")
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--min-margin", type=float, default=0.01)
    parser.add_argument("--min-profit", type=float, default=0.05)
    parser.add_argument("--scan-interval", type=float, default=30)
    args = parser.parse_args()
    asyncio.run(main(args))
