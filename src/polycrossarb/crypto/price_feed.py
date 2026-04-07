"""Real-time BTC price feed from 4 exchanges via WebSocket.

Aggregates spot prices from:
  - Binance  (wss://stream.binance.com, ~100ms updates)
  - Bybit    (wss://stream.bybit.com, ~100ms updates)
  - OKX      (wss://ws.okx.com, ~100ms updates)
  - MEXC     (wss://wbs.mexc.com, ~200ms updates)

All WebSocket feeds are free and require no API keys for public market data.
API keys are only needed for trading — not for price feeds.

The aggregator computes:
  - Weighted mid-price across all sources
  - Cross-exchange spread (max price - min price)
  - Rolling volatility for fair-value calculations
  - Price staleness detection
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import deque
from dataclasses import dataclass

import websockets

log = logging.getLogger(__name__)


@dataclass
class PriceSnapshot:
    """A price reading from an exchange."""
    price: float
    source: str
    timestamp: float
    bid: float = 0.0
    ask: float = 0.0


@dataclass
class AggregatedPrice:
    """Cross-exchange aggregated price."""
    mid: float                  # weighted average across exchanges
    spread: float               # max - min across exchanges (cross-exchange spread)
    n_sources: int              # how many exchanges are live
    staleness_ms: float         # ms since most recent update
    sources: dict[str, float]   # exchange -> price


class CryptoPriceFeed:
    """Aggregated real-time BTC price from 4 exchanges.

    All feeds are free public WebSocket streams — no API keys needed.
    """

    STALE_THRESHOLD = 10.0  # seconds — mark source as stale after this
    MIN_SOURCES = 2  # minimum live sources required for reliable price

    def __init__(self):
        self._prices: dict[str, PriceSnapshot] = {}
        self._price_history: deque[tuple[float, float]] = deque(maxlen=2000)
        self._running = False
        self._mid_price: float = 0.0
        self._volatility_24h: float = 0.50
        self._implied_vol: float | None = None
        self._last_update: float = 0.0
        self._update_count: int = 0

        # Multi-asset price tracking
        self._asset_prices: dict[str, dict[str, float]] = {}  # asset -> {source: price}
        self._asset_mid: dict[str, float] = {}  # asset -> mid price

    @property
    def btc_price(self) -> float:
        return self._mid_price

    def get_price(self, asset: str) -> float:
        """Get mid price for any tracked asset (BTC, ETH, SOL)."""
        if asset.upper() == "BTC":
            return self._mid_price
        return self._asset_mid.get(asset.upper(), 0.0)

    def _update_asset_price(self, asset: str, source: str, price: float):
        """Update price for any tracked asset."""
        if price <= 0:
            return
        asset = asset.upper()
        if asset not in self._asset_prices:
            self._asset_prices[asset] = {}
        self._asset_prices[asset][source] = price
        prices = list(self._asset_prices[asset].values())
        self._asset_mid[asset] = sum(prices) / len(prices)

    @property
    def volatility(self) -> float:
        return self._volatility_24h

    @property
    def implied_volatility(self) -> float:
        """Deribit implied vol when available, else realized vol."""
        return self._implied_vol if self._implied_vol else self._volatility_24h

    @property
    def age_ms(self) -> float:
        return (time.time() - self._last_update) * 1000 if self._last_update else 99999

    @property
    def sources(self) -> dict[str, float]:
        now = time.time()
        return {
            s: p.price for s, p in self._prices.items()
            if now - p.timestamp < self.STALE_THRESHOLD
        }

    @property
    def n_live_sources(self) -> int:
        return len(self.sources)

    @property
    def is_reliable(self) -> bool:
        """True if enough sources are live for reliable price aggregation."""
        return self.n_live_sources >= self.MIN_SOURCES

    @property
    def cross_exchange_spread(self) -> float:
        """Price difference between highest and lowest exchange."""
        prices = list(self.sources.values())
        if len(prices) < 2:
            return 0.0
        return max(prices) - min(prices)

    def get_aggregated(self) -> AggregatedPrice:
        """Get current aggregated price snapshot."""
        src = self.sources
        return AggregatedPrice(
            mid=self._mid_price,
            spread=self.cross_exchange_spread,
            n_sources=len(src),
            staleness_ms=self.age_ms,
            sources=src,
        )

    async def start(self) -> None:
        """Start all 4 exchange feeds + Deribit IV concurrently."""
        self._running = True
        await asyncio.gather(
            self._binance_ws(),
            self._bybit_ws(),
            self._okx_ws(),
            self._mexc_ws(),
            self._deribit_iv_loop(),
            return_exceptions=True,
        )

    def stop(self):
        self._running = False

    def _update_price(self, source: str, price: float, bid: float = 0, ask: float = 0):
        """Update price from a source and recalculate aggregate."""
        if price <= 0:
            return

        now = time.time()
        self._prices[source] = PriceSnapshot(
            price=price, source=source, timestamp=now, bid=bid, ask=ask,
        )
        self._last_update = now
        self._update_count += 1
        self._price_history.append((now, price))

        # Weighted mid: average across all live sources
        live = {s: p.price for s, p in self._prices.items() if now - p.timestamp < self.STALE_THRESHOLD}
        if live:
            self._mid_price = sum(live.values()) / len(live)

        # Recalculate volatility every 200 ticks
        if self._update_count % 200 == 0:
            self._recalc_volatility()

    def _recalc_volatility(self):
        """Estimate annualized volatility from recent price history."""
        if len(self._price_history) < 50:
            return

        items = list(self._price_history)
        returns = []
        for i in range(1, len(items)):
            dt = items[i][0] - items[i - 1][0]
            if dt > 0 and items[i - 1][1] > 0:
                log_return = math.log(items[i][1] / items[i - 1][1])
                returns.append((log_return, dt))

        if len(returns) < 20:
            return

        avg_dt = sum(dt for _, dt in returns) / len(returns)
        mean_r = sum(r for r, _ in returns) / len(returns)
        var_r = sum((r - mean_r) ** 2 for r, _ in returns) / len(returns)

        if avg_dt > 0:
            var_per_second = var_r / avg_dt
            self._volatility_24h = min(5.0, math.sqrt(var_per_second * 365.25 * 86400))

    async def _deribit_iv_loop(self):
        """Fetch Deribit implied volatility every 60s."""
        from polycrossarb.crypto.deribit_vol import fetch_btc_implied_vol
        while self._running:
            try:
                iv = await fetch_btc_implied_vol()
                if iv and iv > 0:
                    self._implied_vol = iv
            except Exception:
                pass
            await asyncio.sleep(60)

    # ── Exchange WebSocket Feeds ──────────────────────────────────

    async def _binance_ws(self):
        """Binance BTC+ETH+SOL/USDT tickers (free, ~100ms)."""
        url = "wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/ethusdt@ticker/solusdt@ticker"
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    log.info("Binance WS connected")
                    async for msg in ws:
                        if not self._running:
                            break
                        try:
                            d = json.loads(msg)
                            data = d.get("data", d)
                            symbol = data.get("s", "").upper()
                            price = float(data.get("c", 0))
                            bid = float(data.get("b", 0))
                            ask = float(data.get("a", 0))
                            if symbol == "BTCUSDT":
                                self._update_price("binance", price, bid, ask)
                            elif symbol == "ETHUSDT":
                                self._update_asset_price("ETH", "binance", price)
                            elif symbol == "SOLUSDT":
                                self._update_asset_price("SOL", "binance", price)
                        except (json.JSONDecodeError, ValueError, TypeError):
                            pass
            except Exception as e:
                if self._running:
                    log.debug("Binance WS: %s", e)
                    await asyncio.sleep(3)

    async def _bybit_ws(self):
        """Bybit BTC/USDT ticker (free, ~100ms)."""
        url = "wss://stream.bybit.com/v5/public/spot"
        sub = {"op": "subscribe", "args": ["tickers.BTCUSDT"]}
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    await ws.send(json.dumps(sub))
                    log.info("Bybit WS connected")
                    async for msg in ws:
                        if not self._running:
                            break
                        try:
                            d = json.loads(msg)
                            data = d.get("data", {})
                            price = float(data.get("lastPrice", 0))
                            bid = float(data.get("bid1Price", 0))
                            ask = float(data.get("ask1Price", 0))
                            if price > 0:
                                self._update_price("bybit", price, bid, ask)
                        except (json.JSONDecodeError, ValueError, TypeError):
                            pass
            except Exception as e:
                if self._running:
                    log.debug("Bybit WS: %s", e)
                    await asyncio.sleep(3)

    async def _okx_ws(self):
        """OKX BTC/USDT ticker (free, ~100ms)."""
        url = "wss://ws.okx.com:8443/ws/v5/public"
        sub = {"op": "subscribe", "args": [{"channel": "tickers", "instId": "BTC-USDT"}]}
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    await ws.send(json.dumps(sub))
                    log.info("OKX WS connected")
                    async for msg in ws:
                        if not self._running:
                            break
                        try:
                            d = json.loads(msg)
                            data_list = d.get("data", [])
                            if data_list and isinstance(data_list, list):
                                data = data_list[0]
                                price = float(data.get("last", 0))
                                bid = float(data.get("bidPx", 0))
                                ask = float(data.get("askPx", 0))
                                if price > 0:
                                    self._update_price("okx", price, bid, ask)
                        except (json.JSONDecodeError, ValueError, TypeError):
                            pass
            except Exception as e:
                if self._running:
                    log.debug("OKX WS: %s", e)
                    await asyncio.sleep(3)

    async def _mexc_ws(self):
        """MEXC BTC/USDT ticker (free, ~200ms)."""
        url = "wss://wbs.mexc.com/ws"
        sub = {"method": "SUBSCRIPTION", "params": ["spot@public.miniTicker.v3.api@BTCUSDT@UTC+8"]}
        reconnect_delay = 3
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
                    await ws.send(json.dumps(sub))
                    log.info("MEXC WS connected")
                    reconnect_delay = 3  # reset on successful connect
                    async for msg in ws:
                        if not self._running:
                            break
                        try:
                            d = json.loads(msg)
                            data = d.get("d", {})
                            price = float(data.get("c", 0))  # close/last price
                            if price > 0:
                                self._update_price("mexc", price)
                        except (json.JSONDecodeError, ValueError, TypeError):
                            pass
            except Exception as e:
                if self._running:
                    log.debug("MEXC WS: %s", e)
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 1.5, 30)  # backoff up to 30s
