"""Real-time BTC price feed from multiple exchanges.

Aggregates spot prices from free WebSocket/REST APIs:
  - Binance (primary, ~100ms updates via WebSocket)
  - CoinGecko (backup, REST, rate-limited)
  - Kraken (secondary, WebSocket)

Provides a weighted mid-price and tracks volatility for
fair-value calculations.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field

import httpx
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


class CryptoPriceFeed:
    """Aggregated real-time BTC price feed.

    Connects to Binance WebSocket for ~100ms updates.
    Falls back to REST polling if WebSocket fails.
    Tracks rolling volatility for options pricing.
    """

    def __init__(self):
        self._prices: dict[str, PriceSnapshot] = {}  # source -> latest
        self._price_history: deque[tuple[float, float]] = deque(maxlen=1000)  # (timestamp, price)
        self._running = False
        self._mid_price: float = 0.0
        self._volatility_24h: float = 0.50  # annualized, default 50%
        self._last_update: float = 0.0

    @property
    def btc_price(self) -> float:
        """Current best BTC/USD price."""
        return self._mid_price

    @property
    def volatility(self) -> float:
        """Annualized volatility estimate."""
        return self._volatility_24h

    @property
    def age_ms(self) -> float:
        """Milliseconds since last price update."""
        return (time.time() - self._last_update) * 1000

    @property
    def sources(self) -> dict[str, float]:
        """Current price from each source."""
        return {s: p.price for s, p in self._prices.items()}

    async def start(self) -> None:
        """Start all price feeds concurrently."""
        self._running = True
        await asyncio.gather(
            self._binance_ws(),
            self._rest_poller(),
            return_exceptions=True,
        )

    def stop(self):
        self._running = False

    def _update_price(self, source: str, price: float, bid: float = 0, ask: float = 0):
        """Update price from a source and recalculate aggregate."""
        now = time.time()
        self._prices[source] = PriceSnapshot(
            price=price, source=source, timestamp=now, bid=bid, ask=ask,
        )
        self._last_update = now
        self._price_history.append((now, price))

        # Weighted mid: average across all sources (simple for now)
        prices = [p.price for p in self._prices.values() if now - p.timestamp < 30]
        if prices:
            self._mid_price = sum(prices) / len(prices)

        # Update volatility estimate every 100 ticks
        if len(self._price_history) % 100 == 0:
            self._recalc_volatility()

    def _recalc_volatility(self):
        """Estimate annualized volatility from recent price history."""
        if len(self._price_history) < 20:
            return

        returns = []
        items = list(self._price_history)
        for i in range(1, len(items)):
            dt = items[i][0] - items[i - 1][0]
            if dt > 0 and items[i - 1][1] > 0:
                log_return = math.log(items[i][1] / items[i - 1][1])
                returns.append((log_return, dt))

        if len(returns) < 10:
            return

        # Variance of returns, annualized
        avg_dt = sum(dt for _, dt in returns) / len(returns)
        mean_r = sum(r for r, _ in returns) / len(returns)
        var_r = sum((r - mean_r) ** 2 for r, _ in returns) / len(returns)

        # Annualize: sqrt(var_per_second * seconds_per_year)
        if avg_dt > 0:
            var_per_second = var_r / avg_dt
            self._volatility_24h = math.sqrt(var_per_second * 365.25 * 86400)

    async def _binance_ws(self):
        """Binance BTC/USDT WebSocket feed (free, ~100ms updates)."""
        url = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    log.info("Binance WS connected")
                    async for msg in ws:
                        if not self._running:
                            break
                        try:
                            data = json.loads(msg)
                            price = float(data.get("c", 0))  # last price
                            bid = float(data.get("b", 0))
                            ask = float(data.get("a", 0))
                            if price > 0:
                                self._update_price("binance", price, bid, ask)
                        except (json.JSONDecodeError, ValueError):
                            pass
            except Exception as e:
                if self._running:
                    log.warning("Binance WS error: %s, reconnecting...", e)
                    await asyncio.sleep(5)

    async def _rest_poller(self):
        """REST fallback: poll CoinGecko every 10s."""
        async with httpx.AsyncClient(timeout=10) as client:
            while self._running:
                try:
                    resp = await client.get(
                        "https://api.coingecko.com/api/v3/simple/price",
                        params={"ids": "bitcoin", "vs_currencies": "usd"},
                    )
                    if resp.status_code == 200:
                        price = resp.json().get("bitcoin", {}).get("usd", 0)
                        if price > 0:
                            self._update_price("coingecko", price)
                except Exception:
                    pass
                await asyncio.sleep(10)
