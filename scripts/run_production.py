#!/usr/bin/env python3
"""Production candle trading with safety circuit breaker.

Runs LIVE candle trading with:
  - Backtest-optimized parameters (edge≥7%, conf≥60%)
  - Circuit breaker: auto-stop if WR<65% over 20 trades or drawdown>30%
  - Live data collector running in parallel for ongoing validation
  - Full trade logging to logs/candle_trades.jsonl

Usage:
  # Paper mode first (validate):
  python scripts/run_production.py --mode paper

  # Go live:
  python scripts/run_production.py --mode live

  # With custom bankroll:
  python scripts/run_production.py --mode live --bankroll 20
"""
from __future__ import annotations

import argparse
import asyncio
import signal
import sys
import time

sys.path.insert(0, "src")

from polycrossarb.config import settings
from polycrossarb.execution.executor import ExecutionMode
from polycrossarb.crypto.candle_pipeline import CandlePipeline
from polycrossarb.monitoring.logging_config import configure_logging
from polycrossarb.risk.manager import RiskManager
from polycrossarb.backtest.collector import DataCollector

configure_logging()


async def main(mode: str, bankroll: float | None, duration: int | None = None):
    exec_mode = ExecutionMode.LIVE if mode == "live" else ExecutionMode.PAPER

    # Read on-chain balance
    from polycrossarb.execution.wallet import get_wallet_balances, detect_bankroll

    balances = get_wallet_balances()
    on_chain = balances["usdc_e"]

    # Determine starting bankroll:
    #   --bankroll flag > on-chain balance > config fallback
    if bankroll is not None:
        starting = bankroll
        source = "cli flag"
    elif on_chain > 0:
        starting = on_chain
        source = "on-chain USDC.e"
    else:
        starting = detect_bankroll()
        source = "config fallback"

    # Risk manager starts from live balance, compounds from there
    risk = RiskManager(
        initial_bankroll=starting,
        max_per_market=settings.max_position_per_market_usd,
        state_dir="logs/candle",
    )

    print(f"{'='*55}")
    print(f"  CANDLE STRATEGY — {'LIVE' if mode == 'live' else 'PAPER'} MODE")
    print(f"{'='*55}")
    print(f"  Wallet:       {balances['address'][:10]}...")
    print(f"  On-chain:     ${on_chain:.2f} USDC.e")
    if balances['usdc_native'] > 0:
        print(f"                ${balances['usdc_native']:.2f} native USDC (not usable)")
    print(f"  Bankroll:     ${starting:.2f} ({source})")
    print(f"  Max/trade:    ${settings.max_position_per_market_usd:.2f}")
    print(f"  Min edge:     {settings.min_crypto_edge:.0%}")
    print(f"  Min conf:     60%")
    print(f"  Circuit:      stop if WR<65% (20+ trades) or DD>30%")
    print(f"  Compounding:  profits grow bankroll, losses shrink it")
    print(f"{'='*55}")
    print()

    if on_chain < 1.5 and mode == "live":
        print(f"  *** WARNING: ${on_chain:.2f} USDC.e — need $1.50+ for live ***")
        print(f"  Fund wallet or use --mode paper")
        return

    if mode == "live":
        print("  *** LIVE MODE — REAL MONEY ***")
        print("  Starting in 5 seconds... Ctrl+C to abort")
        await asyncio.sleep(5)

    pipeline = CandlePipeline(
        mode=exec_mode,
        min_confidence=0.60,
        risk_manager=risk,
    )

    # Data collector runs alongside
    collector = DataCollector()

    loop = asyncio.get_event_loop()

    def _handle_signal(sig_name: str) -> None:
        # Spawn the async shutdown so we can drain paper-position state
        # and send the shutdown alert before the process exits.
        async def _do():
            try:
                await pipeline.shutdown(reason=f"signal {sig_name}")
            finally:
                try:
                    collector.stop()
                except Exception:
                    pass
        asyncio.create_task(_do())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal, sig.name)

    if duration:
        async def stop_after():
            await asyncio.sleep(duration)
            pipeline.stop()
            collector.stop()
        asyncio.create_task(stop_after())

    start_time = time.time()

    try:
        await asyncio.gather(
            pipeline.run(),
            collector.run(),
            return_exceptions=True,
        )
    except asyncio.CancelledError:
        pass

    elapsed = time.time() - start_time

    s = pipeline.status()
    print(f"\n{'='*55}")
    print(f"  SESSION RESULTS ({elapsed/60:.1f} minutes)")
    print(f"{'='*55}")
    print(f"  BTC:          ${s['btc_price']:,.2f}")
    print(f"  Sources:      {s['sources']}")
    print(f"  Contracts:    {s['contracts']}")
    print(f"  Trades:       {s['trades']}")
    print(f"  Wins:         {s['wins']}")
    print(f"  Losses:       {s['losses']}")
    print(f"  Win Rate:     {s['win_rate']}")
    print(f"  Expected P&L: ${s['expected_profit']:+.2f}")
    print(f"  Realized P&L: ${s['realized_profit']:+.2f}")
    print(f"  Bankroll:     ${s['bankroll']:.2f}")
    if s['circuit_breaker']:
        print(f"  *** CIRCUIT BREAKER TRIPPED ***")
    print(f"{'='*55}")

    c = collector.status()
    print(f"  Data: {c['ticks_collected']} ticks, {c['contracts_collected']} contracts saved")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--bankroll", type=float, default=None,
                        help="Override bankroll (default: from .env)")
    parser.add_argument("--duration", type=int, default=None,
                        help="Run for N seconds then stop")
    args = parser.parse_args()
    asyncio.run(main(args.mode, args.bankroll, args.duration))
