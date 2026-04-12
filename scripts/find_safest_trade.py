#!/usr/bin/env python3
"""Find the safest possible $1 diagnostic trade on Polymarket.

Looks for candle contracts closest to resolution where BTC has already
moved strongly — the outcome is nearly determined, minimizing loss risk.

Usage:
    python scripts/find_safest_trade.py
"""
from __future__ import annotations

import asyncio
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "src")

from polycrossarb.data.client import PolymarketClient
from polycrossarb.crypto.candle_scanner import scan_candle_markets
from polycrossarb.crypto.price_feed import CryptoPriceFeed


async def main():
    client = PolymarketClient()

    # Fetch contracts resolving within 1 hour
    markets = await client.fetch_markets_by_end_date(max_hours=1.0)
    contracts = scan_candle_markets(markets, max_hours=1.0, min_liquidity=10)

    # Get live BTC price
    feed = CryptoPriceFeed()
    task = asyncio.create_task(feed.start())
    await asyncio.sleep(6)
    btc = feed.btc_price
    feed.stop()
    task.cancel()

    now = datetime.now(timezone.utc)
    time_str = now.strftime("%H:%M:%S")
    print(f"BTC: ${btc:,.2f}")
    print(f"Time: {time_str} UTC")
    print(f"Contracts found: {len(contracts)}")
    print()

    candidates = []
    for c in contracts:
        try:
            end = datetime.fromisoformat(c.end_date.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        minutes_left = (end - now).total_seconds() / 60
        if minutes_left <= 0.5 or minutes_left > 30:
            continue

        # Estimate window length from description
        desc = c.window_description.lower()
        if "am-" in desc or "pm-" in desc:
            window_minutes = 5.0
        elif any(f"{h}am" in desc or f"{h}pm" in desc for h in range(1, 13)):
            window_minutes = 60.0
        else:
            window_minutes = 15.0

        elapsed = max(0, window_minutes - minutes_left)
        elapsed_pct = elapsed / window_minutes if window_minutes > 0 else 1.0

        # The SAFEST trade: buy the side that's already winning (price > 0.70)
        # at the latest possible moment (highest elapsed_pct)
        # A contract at 0.85 means market thinks 85% chance of that outcome
        # Buying at $0.85 means risking $0.15 to win $0.15 — but 85% likely
        safe_side = None
        safe_price = 0
        if c.up_price >= 0.70:
            safe_side = "UP"
            safe_price = c.up_price
        elif c.down_price >= 0.70:
            safe_side = "DOWN"
            safe_price = c.down_price

        if safe_side and elapsed_pct > 0.50:
            # Expected P&L: $1 * (1 - safe_price) * win_prob - $1 * safe_price * (1 - win_prob)
            # Where win_prob ≈ safe_price (market-implied)
            expected_profit = 1.0 * (1.0 - safe_price) * safe_price - 1.0 * safe_price * (1.0 - safe_price)
            # = 0 in expectation (no edge) — but we have BTC momentum info

            candidates.append({
                "question": c.market.question[:65],
                "asset": c.asset,
                "minutes_left": minutes_left,
                "elapsed_pct": elapsed_pct,
                "safe_side": safe_side,
                "safe_price": safe_price,
                "up_price": c.up_price,
                "down_price": c.down_price,
                "spread": c.spread,
                "liquidity": c.liquidity,
                "up_token": c.up_token_id,
                "down_token": c.down_token_id,
                "condition_id": c.market.condition_id,
                "window_min": window_minutes,
            })

    # Sort: most elapsed first, then cheapest (highest probability)
    candidates.sort(key=lambda x: (-x["elapsed_pct"], -x["safe_price"]))

    if not candidates:
        print("No safe candidates found. Wait for candles to approach resolution.")
        await client.close()
        return

    print("SAFEST $1 TRADE CANDIDATES")
    print(f"{'#':>2} {'Min':>5} {'Elpsd':>6} {'Side':>4} {'Price':>6} {'Risk$':>6} {'Asset':>4}  Question")
    print("-" * 100)
    for i, c in enumerate(candidates[:10], 1):
        risk = 1.0 * c["safe_price"]  # max loss if wrong
        reward = 1.0 * (1.0 - c["safe_price"])  # profit if right
        print(
            f"{i:2d} {c['minutes_left']:5.1f} {c['elapsed_pct']*100:5.1f}% "
            f"{c['safe_side']:>4} ${c['safe_price']:.3f} ${risk:.3f} "
            f"{c['asset']:>4}  {c['question']}"
        )

    best = candidates[0]
    print()
    print("=" * 80)
    print("  RECOMMENDED DIAGNOSTIC TRADE")
    print("=" * 80)
    print(f"  Contract: {best['question']}")
    print(f"  Side:     BUY {best['safe_side']} @ ${best['safe_price']:.3f}")
    print(f"  Size:     $1.00 ({1.0/best['safe_price']:.1f} shares)")
    print(f"  Risk:     ${best['safe_price']:.3f} (if wrong)")
    print(f"  Reward:   ${1.0 - best['safe_price']:.3f} (if right)")
    print(f"  Win prob: ~{best['safe_price']*100:.0f}% (market-implied)")
    print(f"  Time left: {best['minutes_left']:.1f} min ({best['elapsed_pct']*100:.0f}% elapsed)")
    print(f"  Liquidity: ${best['liquidity']:,.0f}")
    if best["safe_side"] == "UP":
        print(f"  Token ID: {best['up_token']}")
    else:
        print(f"  Token ID: {best['down_token']}")
    print(f"  Condition: {best['condition_id']}")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
