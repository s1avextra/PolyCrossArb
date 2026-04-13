"""Live data collector for future backtests.

Runs 24/7, persisting:
  - BTC ticks (1-second from all WebSocket feeds)
  - Candle contract states (every 30s from Gamma API)
  - Candle contract order book snapshots (every 5s — bid/ask for backtest realism)
  - Resolution outcomes when contracts close
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from polymomentum.crypto.price_feed import CryptoPriceFeed
from polymomentum.data.client import PolymarketClient
from polymomentum.crypto.candle_scanner import scan_candle_markets

log = logging.getLogger(__name__)


class DataCollector:
    """Collects and persists live data for future backtesting."""

    def __init__(self, data_dir: str = "data/live"):
        self._dir = Path(data_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._price_feed = CryptoPriceFeed()
        self._client = PolymarketClient()
        self._running = False
        self._tick_count = 0
        self._contract_count = 0
        self._book_count = 0
        self._contracts: list = []  # shared with book writer

    async def run(self):
        """Run all collectors concurrently."""
        self._running = True
        log.info("Data collector started, output: %s", self._dir)

        try:
            await asyncio.gather(
                self._price_feed.start(),
                self._tick_writer(),
                self._contract_writer(),
                self._book_snapshot_writer(),
                return_exceptions=True,
            )
        finally:
            self._price_feed.stop()
            await self._client.close()
            self._running = False

    def stop(self):
        self._running = False
        self._price_feed.stop()

    async def _tick_writer(self):
        """Write BTC ticks to CSV every second."""
        while self._running and self._price_feed.btc_price == 0:
            await asyncio.sleep(0.5)

        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        csv_path = self._dir / f"btc_ticks_{date_str}.csv"
        is_new = not csv_path.exists()

        with open(csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            if is_new:
                writer.writerow(["timestamp_ms", "price", "sources", "spread"])

            while self._running:
                agg = self._price_feed.get_aggregated()
                if agg.mid > 0:
                    writer.writerow([
                        int(time.time() * 1000),
                        round(agg.mid, 2),
                        agg.n_sources,
                        round(agg.spread, 2),
                    ])
                    f.flush()
                    self._tick_count += 1

                    new_date = datetime.now(timezone.utc).strftime("%Y%m%d")
                    if new_date != date_str:
                        break

                await asyncio.sleep(1)

        if self._running:
            await self._tick_writer()

    async def _contract_writer(self):
        """Snapshot candle contracts every 30s."""
        csv_path = self._dir / "candle_contracts.csv"
        is_new = not csv_path.exists()

        with open(csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            if is_new:
                writer.writerow([
                    "timestamp_ms", "condition_id", "question",
                    "up_price", "down_price", "end_date",
                    "volume", "liquidity", "window_desc",
                ])

            while self._running:
                try:
                    markets = await self._client.fetch_all_active_markets(min_liquidity=0)
                    contracts = scan_candle_markets(markets, max_hours=2.0, min_liquidity=0)
                    self._contracts = contracts  # share with book writer
                    now_ms = int(time.time() * 1000)

                    for c in contracts:
                        writer.writerow([
                            now_ms,
                            c.market.condition_id,
                            c.market.question[:100],
                            round(c.up_price, 4),
                            round(c.down_price, 4),
                            c.end_date,
                            round(c.volume, 2),
                            round(c.liquidity, 2),
                            c.window_description[:30],
                        ])
                    f.flush()
                    self._contract_count += len(contracts)

                except Exception:
                    log.exception("contract_writer error")

                await asyncio.sleep(30)

    async def _book_snapshot_writer(self):
        """Record order book bid/ask for candle contracts every 5 seconds.

        This data is critical for realistic backtesting — it captures
        the actual prices available to trade at each point in time.
        """
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        csv_path = self._dir / f"candle_books_{date_str}.csv"
        is_new = not csv_path.exists()

        # Wait for contracts to be populated by _contract_writer
        wait_count = 0
        while self._running and not self._contracts:
            await asyncio.sleep(2)
            wait_count += 1
            if wait_count % 15 == 0:
                log.info("Book writer waiting for contracts... (%ds)", wait_count * 2)

        with open(csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            if is_new:
                writer.writerow([
                    "timestamp_ms", "condition_id", "end_date",
                    "up_best_bid", "up_best_ask", "up_bid_depth",
                    "down_best_bid", "down_best_ask", "down_bid_depth",
                    "btc_price",
                ])

            while self._running:
                try:
                    now_ms = int(time.time() * 1000)
                    btc = round(self._price_feed.btc_price, 2)

                    for c in self._contracts:
                        # Fetch order book for up and down tokens
                        up_book = None
                        down_book = None
                        try:
                            if c.up_token_id:
                                up_book = await self._client.fetch_order_book(c.up_token_id)
                            if c.down_token_id:
                                down_book = await self._client.fetch_order_book(c.down_token_id)
                        except Exception:
                            continue

                        up_bid = up_book.best_bid or 0 if up_book and up_book.bids else 0
                        up_ask = up_book.best_ask or 0 if up_book and up_book.asks else 0
                        up_depth = sum(b.size * b.price for b in (up_book.bids if up_book else []))
                        down_bid = down_book.best_bid or 0 if down_book and down_book.bids else 0
                        down_ask = down_book.best_ask or 0 if down_book and down_book.asks else 0
                        down_depth = sum(b.size * b.price for b in (down_book.bids if down_book else []))

                        writer.writerow([
                            now_ms,
                            c.market.condition_id,
                            c.end_date,
                            round(up_bid, 4),
                            round(up_ask, 4),
                            round(up_depth, 2),
                            round(down_bid, 4),
                            round(down_ask, 4),
                            round(down_depth, 2),
                            btc,
                        ])
                        self._book_count += 1

                    f.flush()

                except Exception as e:
                    log.warning("book_snapshot_writer error: %s", e)

                # Rotate at midnight
                new_date = datetime.now(timezone.utc).strftime("%Y%m%d")
                if new_date != date_str:
                    break

                await asyncio.sleep(5)

        if self._running:
            await self._book_snapshot_writer()

    def status(self) -> dict:
        return {
            "ticks_collected": self._tick_count,
            "contracts_collected": self._contract_count,
            "book_snapshots": self._book_count,
            "btc_price": self._price_feed.btc_price,
            "sources": self._price_feed.n_live_sources,
        }
