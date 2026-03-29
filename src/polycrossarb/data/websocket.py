"""Polymarket WebSocket client for real-time market data.

Subscribes to the market channel for order book updates, price changes,
best bid/ask, and market resolution events. Event-driven — callbacks fire
immediately when prices change, enabling sub-second arb detection.

Endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
Auth: None required for market data.
Keepalive: Send PING every 10s.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable

import contextlib

import websockets
import websockets.exceptions

from polycrossarb.data.models import Market, OrderBook, OrderBookLevel

log = logging.getLogger(__name__)

WSS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL = 10  # seconds
RECONNECT_DELAY = 5  # seconds


@dataclass
class PriceUpdate:
    """A price change event from the WebSocket."""
    asset_id: str
    market_condition_id: str
    best_bid: float | None = None
    best_ask: float | None = None
    last_price: float | None = None
    timestamp: float = 0.0


@dataclass
class MarketResolution:
    """A market resolution event."""
    market_condition_id: str
    winning_asset_id: str
    winning_outcome: str
    timestamp: float = 0.0


# Callback types
PriceCallback = Callable[[PriceUpdate], None]
BookCallback = Callable[[str, OrderBook], None]  # (asset_id, book)
ResolutionCallback = Callable[[MarketResolution], None]


class PolymarketWebSocket:
    """Real-time WebSocket client for Polymarket market data.

    Usage:
        ws = PolymarketWebSocket()
        ws.on_price_change = my_callback
        ws.subscribe(token_ids)
        await ws.run()
    """

    def __init__(self):
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._subscribed_ids: set[str] = set()
        self._running = False

        # In-memory state
        self._books: dict[str, OrderBook] = {}  # asset_id -> OrderBook
        self._best_prices: dict[str, tuple[float | None, float | None]] = {}  # asset_id -> (bid, ask)
        self._last_prices: dict[str, float] = {}  # asset_id -> last trade price
        self._asset_to_market: dict[str, str] = {}  # asset_id -> condition_id

        # Callbacks
        self.on_price_change: PriceCallback | None = None
        self.on_book_update: BookCallback | None = None
        self.on_resolution: ResolutionCallback | None = None

        # Stats
        self._msg_count = 0
        self._connect_time = 0.0
        self._last_msg_time = 0.0

    def set_asset_market_map(self, mapping: dict[str, str]) -> None:
        """Set the asset_id -> condition_id mapping for price updates."""
        self._asset_to_market = mapping

    def subscribe(self, asset_ids: list[str]) -> None:
        """Queue asset IDs for subscription. Call before or during run()."""
        self._subscribed_ids.update(aid for aid in asset_ids if aid)

    def unsubscribe(self, asset_ids: list[str]) -> None:
        """Remove asset IDs from subscription."""
        self._subscribed_ids -= set(asset_ids)

    def get_book(self, asset_id: str) -> OrderBook | None:
        """Get cached order book for an asset."""
        return self._books.get(asset_id)

    def get_best_bid_ask(self, asset_id: str) -> tuple[float | None, float | None]:
        """Get cached best bid/ask for an asset."""
        return self._best_prices.get(asset_id, (None, None))

    def get_last_price(self, asset_id: str) -> float | None:
        """Get last traded price for an asset."""
        return self._last_prices.get(asset_id)

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and not self._ws.close_code

    @property
    def stats(self) -> dict:
        return {
            "connected": self.is_connected,
            "subscriptions": len(self._subscribed_ids),
            "messages_received": self._msg_count,
            "books_cached": len(self._books),
            "uptime_s": round(time.time() - self._connect_time, 1) if self._connect_time else 0,
        }

    async def run(self) -> None:
        """Connect and process messages. Reconnects automatically."""
        self._running = True
        while self._running:
            try:
                await self._connect_and_listen()
            except (websockets.exceptions.ConnectionClosed, OSError) as e:
                if self._running:
                    log.warning("WebSocket disconnected: %s. Reconnecting in %ds...", e, RECONNECT_DELAY)
                    await asyncio.sleep(RECONNECT_DELAY)
            except Exception:
                log.exception("WebSocket error")
                if self._running:
                    await asyncio.sleep(RECONNECT_DELAY)

    def stop(self) -> None:
        """Stop the WebSocket client."""
        self._running = False

    async def close(self) -> None:
        """Close the connection."""
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _connect_and_listen(self) -> None:
        """Connect, subscribe, and process messages."""
        async with websockets.connect(WSS_MARKET_URL, ping_interval=None) as ws:
            self._ws = ws
            self._connect_time = time.time()
            log.info("WebSocket connected to %s", WSS_MARKET_URL)

            # Send initial subscription
            if self._subscribed_ids:
                await self._send_subscribe(list(self._subscribed_ids))

            # Start ping task
            ping_task = asyncio.create_task(self._ping_loop())

            try:
                async for raw_msg in ws:
                    if not self._running:
                        break
                    self._last_msg_time = time.time()
                    self._msg_count += 1

                    if raw_msg == "PONG":
                        continue

                    try:
                        msg = json.loads(raw_msg)
                        await self._handle_message(msg)
                    except json.JSONDecodeError:
                        pass
            finally:
                ping_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ping_task

    async def _send_subscribe(self, asset_ids: list[str]) -> None:
        """Send subscription message."""
        if not self._ws or not asset_ids:
            return

        # Subscribe in batches of 100 to avoid message size limits
        for i in range(0, len(asset_ids), 100):
            batch = asset_ids[i:i + 100]
            msg = {
                "assets_ids": batch,
                "type": "market",
                "initial_dump": True,
                "level": 2,
                "custom_feature_enabled": True,
            }
            if i > 0:
                msg["operation"] = "subscribe"
                del msg["type"]

            await self._ws.send(json.dumps(msg))
            log.info("Subscribed to %d assets (batch %d)", len(batch), i // 100 + 1)

    async def _ping_loop(self) -> None:
        """Send PING every 10 seconds to keep connection alive."""
        while self._running and self._ws and not self._ws.close_code:
            try:
                await self._ws.send("PING")
                await asyncio.sleep(PING_INTERVAL)
            except Exception:
                break

    async def _handle_message(self, msg: dict | list) -> None:
        """Route incoming message to the appropriate handler."""
        if isinstance(msg, list):
            for m in msg:
                await self._handle_message(m)
            return

        event_type = msg.get("event_type", "")

        if event_type == "book":
            self._handle_book(msg)
        elif event_type == "price_change":
            self._handle_price_change(msg)
        elif event_type == "best_bid_ask":
            self._handle_best_bid_ask(msg)
        elif event_type == "last_trade_price":
            self._handle_last_trade(msg)
        elif event_type == "market_resolved":
            self._handle_resolution(msg)

    def _handle_book(self, msg: dict) -> None:
        """Handle full order book snapshot."""
        asset_id = msg.get("asset_id", "")
        if not asset_id:
            return

        bids = [
            OrderBookLevel(price=float(b["price"]), size=float(b["size"]))
            for b in msg.get("bids", [])
        ]
        asks = [
            OrderBookLevel(price=float(a["price"]), size=float(a["size"]))
            for a in msg.get("asks", [])
        ]
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)

        book = OrderBook(bids=bids, asks=asks)
        self._books[asset_id] = book

        if book.best_bid is not None or book.best_ask is not None:
            self._best_prices[asset_id] = (book.best_bid, book.best_ask)

        if self.on_book_update:
            self.on_book_update(asset_id, book)

    def _handle_price_change(self, msg: dict) -> None:
        """Handle incremental price change."""
        changes = msg.get("price_changes", [])
        market_id = msg.get("market", "")

        for change in changes:
            asset_id = change.get("asset_id", "")
            price = float(change.get("price", 0))
            side = change.get("side", "")
            size = float(change.get("size", 0))

            # Update cached book incrementally
            if asset_id in self._books:
                book = self._books[asset_id]
                if side == "BUY":
                    bids = [b for b in book.bids if abs(b.price - price) > 1e-8]
                    if size > 0:
                        bids.append(OrderBookLevel(price=price, size=size))
                    bids.sort(key=lambda x: x.price, reverse=True)
                    self._books[asset_id] = OrderBook(bids=bids, asks=book.asks)
                elif side == "SELL":
                    asks = [a for a in book.asks if abs(a.price - price) > 1e-8]
                    if size > 0:
                        asks.append(OrderBookLevel(price=price, size=size))
                    asks.sort(key=lambda x: x.price)
                    self._books[asset_id] = OrderBook(bids=book.bids, asks=asks)

                updated = self._books[asset_id]
                self._best_prices[asset_id] = (updated.best_bid, updated.best_ask)

            if self.on_price_change:
                cid = self._asset_to_market.get(asset_id, market_id)
                self.on_price_change(PriceUpdate(
                    asset_id=asset_id,
                    market_condition_id=cid,
                    best_bid=self._best_prices.get(asset_id, (None, None))[0],
                    best_ask=self._best_prices.get(asset_id, (None, None))[1],
                    timestamp=time.time(),
                ))

    def _handle_best_bid_ask(self, msg: dict) -> None:
        """Handle best bid/ask update (custom feature)."""
        asset_id = msg.get("asset_id", "")
        bid = float(msg["best_bid"]) if msg.get("best_bid") else None
        ask = float(msg["best_ask"]) if msg.get("best_ask") else None

        self._best_prices[asset_id] = (bid, ask)

        if self.on_price_change:
            cid = self._asset_to_market.get(asset_id, msg.get("market", ""))
            self.on_price_change(PriceUpdate(
                asset_id=asset_id,
                market_condition_id=cid,
                best_bid=bid,
                best_ask=ask,
                timestamp=time.time(),
            ))

    def _handle_last_trade(self, msg: dict) -> None:
        """Handle last trade price update."""
        asset_id = msg.get("asset_id", "")
        price = float(msg.get("price", 0))
        self._last_prices[asset_id] = price

    def _handle_resolution(self, msg: dict) -> None:
        """Handle market resolution event."""
        if self.on_resolution:
            self.on_resolution(MarketResolution(
                market_condition_id=msg.get("market", msg.get("id", "")),
                winning_asset_id=msg.get("winning_asset_id", ""),
                winning_outcome=msg.get("winning_outcome", ""),
                timestamp=time.time(),
            ))
