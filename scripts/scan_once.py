#!/usr/bin/env python3
"""One-shot arbitrage scanner.

Fetches all active Polymarket markets and scans for single-market arbitrage.
Usage: python -m scripts.scan_once [--min-margin 0.01] [--with-books]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys

# Allow running from project root
sys.path.insert(0, "src")

from polycrossarb.arb.detector import (
    detect_cross_market_arbs,
    detect_single_market_arbs,
    detect_single_market_orderbook_arbs,
)
from polycrossarb.data.client import PolymarketClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
)
log = logging.getLogger("scan_once")


async def main(min_margin: float, with_books: bool, top_n: int):
    client = PolymarketClient()
    try:
        log.info("Fetching active markets from Polymarket...")
        markets = await client.fetch_all_active_markets()
        log.info("Loaded %d markets", len(markets))

        # Filter to markets with actual price data
        priced = [m for m in markets if any(o.price > 0 for o in m.outcomes)]
        log.info("%d markets have price data", len(priced))

        # ── Mid-price scan ────────────────────────────────────────
        opps = detect_single_market_arbs(priced, min_margin=min_margin)

        print(f"\n{'='*80}")
        print(f"  SINGLE-MARKET ARB SCAN (mid-price) — {len(opps)} opportunities")
        print(f"{'='*80}\n")

        for i, opp in enumerate(opps[:top_n], 1):
            m = opp.markets[0]
            prices = ", ".join(f"{o.name}={o.price:.4f}" for o in m.outcomes)
            print(f"  #{i}  [{opp.arb_type}]  margin={opp.margin:.4f}  "
                  f"profit/$ = {opp.profit_per_dollar:.4f}")
            print(f"      Q: {m.question[:90]}")
            print(f"      Prices: {prices}")
            print(f"      Volume: ${m.volume:,.0f}  Liquidity: ${m.liquidity:,.0f}")
            print(f"      {opp.details}")
            print()

        if not opps:
            print("  No single-market arbitrage found above threshold.\n")

        # ── Cross-market scan (event-grouped) ─────────────────────
        cross_opps = detect_cross_market_arbs(priced, min_margin=min_margin)

        print(f"\n{'='*80}")
        print(f"  CROSS-MARKET ARB SCAN (event groups) — {len(cross_opps)} opportunities")
        print(f"{'='*80}\n")

        for i, opp in enumerate(cross_opps[:top_n], 1):
            print(f"  #{i}  [{opp.arb_type}]  margin={opp.margin:.4f}  "
                  f"profit/$ = {opp.profit_per_dollar:.4f}  "
                  f"markets={len(opp.markets)}")
            print(f"      {opp.details}")
            for m in opp.markets:
                yes_p = m.outcomes[0].price if m.outcomes else 0
                print(f"        YES={yes_p:.4f}  {m.question[:70]}")
            print()

        if not cross_opps:
            print("  No cross-market arbitrage found above threshold.\n")

        # ── Order book scan (optional) ────────────────────────────
        if with_books and opps:
            # Only fetch books for markets with mid-price arb signals
            arb_markets = [opp.markets[0] for opp in opps[:20]]
            log.info("Fetching order books for %d candidate markets...", len(arb_markets))
            await client.enrich_with_order_books(arb_markets)

            book_opps = detect_single_market_orderbook_arbs(arb_markets, min_margin=min_margin)

            print(f"\n{'='*80}")
            print(f"  ORDER BOOK CONFIRMED ARBS — {len(book_opps)} executable")
            print(f"{'='*80}\n")

            for i, opp in enumerate(book_opps[:top_n], 1):
                m = opp.markets[0]
                print(f"  #{i}  [{opp.arb_type}]  margin={opp.margin:.4f}  "
                      f"profit/$ = {opp.profit_per_dollar:.4f}")
                print(f"      Q: {m.question[:90]}")
                print(f"      {opp.details}")
                print()

            if not book_opps:
                print("  No order-book-confirmed arbs found.\n")

    finally:
        await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket single-market arb scanner")
    parser.add_argument("--min-margin", type=float, default=0.01,
                        help="Minimum arb margin (default 0.01 = 1%%)")
    parser.add_argument("--with-books", action="store_true",
                        help="Also check order book prices for executable arbs")
    parser.add_argument("--top", type=int, default=25,
                        help="Show top N results (default 25)")
    args = parser.parse_args()

    asyncio.run(main(args.min_margin, args.with_books, args.top))
