"""Kalshi REST API client for cross-platform arbitrage.

Kalshi accepts USDC deposits (no US bank account needed).
API docs: https://trading-api.readme.io/reference

Markets that overlap with Polymarket:
  - BTC/ETH/SOL candle contracts (Up/Down)
  - Weather (temperature brackets)
  - Politics, sports

Cross-arb opportunity: when Polymarket YES + Kalshi NO < $1.00
(or vice versa), there's a risk-free profit.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)

KALSHI_BASE = "https://trading-api.kalshi.com/trade-api/v2"


@dataclass
class KalshiMarket:
    """A Kalshi event market."""
    ticker: str             # e.g. "KXBTC-25APR07-T67000"
    title: str
    yes_price: float        # cents → dollars
    no_price: float
    volume: int
    open_time: str
    close_time: str
    category: str
    status: str


@dataclass
class CrossArbOpportunity:
    """Cross-platform arbitrage opportunity."""
    polymarket_cid: str
    kalshi_ticker: str
    poly_yes: float         # Polymarket YES price
    kalshi_no: float        # Kalshi NO price (= 1 - Kalshi YES)
    combined_cost: float    # poly_yes + kalshi_no (< $1 = arb)
    profit: float           # $1 - combined_cost
    direction: str          # what we're betting
    market_title: str


class KalshiClient:
    """Kalshi API client for market data and (future) trading.

    Currently read-only for cross-arb detection.
    Trading requires API key + KYC verification.
    """

    def __init__(self, api_key: str = "", api_secret: str = ""):
        self._api_key = api_key
        self._api_secret = api_secret
        self._client = httpx.AsyncClient(
            base_url=KALSHI_BASE,
            timeout=15,
            headers={"Accept": "application/json"},
        )
        self._token: str = ""
        self._token_expiry: float = 0

    async def _ensure_auth(self) -> None:
        """Authenticate if we have credentials and token is expired."""
        if not self._api_key:
            return
        if time.time() < self._token_expiry:
            return

        try:
            resp = await self._client.post(
                "/login",
                json={"email": self._api_key, "password": self._api_secret},
            )
            resp.raise_for_status()
            data = resp.json()
            self._token = data.get("token", "")
            self._token_expiry = time.time() + 3500  # ~1 hour
            self._client.headers["Authorization"] = f"Bearer {self._token}"
        except Exception:
            log.debug("Kalshi auth failed — running in read-only mode")

    async def fetch_markets(
        self,
        series_ticker: str = "",
        status: str = "open",
        limit: int = 100,
    ) -> list[KalshiMarket]:
        """Fetch markets from Kalshi.

        Args:
            series_ticker: Filter by series (e.g. "KXBTC" for BTC markets)
            status: "open", "closed", "settled"
            limit: Max results
        """
        await self._ensure_auth()

        params: dict = {"limit": limit, "status": status}
        if series_ticker:
            params["series_ticker"] = series_ticker

        try:
            resp = await self._client.get("/markets", params=params)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.debug("Kalshi fetch failed: %s", str(e)[:60])
            return []

        markets = []
        for m in data.get("markets", []):
            yes_price = m.get("yes_bid", 0) / 100  # cents to dollars
            no_price = m.get("no_bid", 0) / 100
            markets.append(KalshiMarket(
                ticker=m.get("ticker", ""),
                title=m.get("title", ""),
                yes_price=yes_price,
                no_price=no_price,
                volume=m.get("volume", 0),
                open_time=m.get("open_time", ""),
                close_time=m.get("close_time", ""),
                category=m.get("category", ""),
                status=m.get("status", ""),
            ))

        return markets

    async def fetch_btc_candle_markets(self) -> list[KalshiMarket]:
        """Fetch BTC candle/binary markets specifically."""
        return await self.fetch_markets(series_ticker="KXBTC", limit=200)

    async def close(self):
        await self._client.aclose()


def detect_cross_arbs(
    poly_markets: list[dict],
    kalshi_markets: list[KalshiMarket],
    min_profit: float = 0.01,
) -> list[CrossArbOpportunity]:
    """Detect cross-platform arb between Polymarket and Kalshi.

    Strategy: buy YES on one platform + buy NO on the other.
    If combined cost < $1.00, guaranteed profit at resolution.

    Args:
        poly_markets: Polymarket markets as dicts with keys:
            condition_id, question, up_price, down_price
        kalshi_markets: Kalshi markets from fetch_markets()
        min_profit: Minimum profit threshold

    Returns:
        List of arbitrage opportunities
    """
    opps: list[CrossArbOpportunity] = []

    # Build lookup by approximate matching
    # Kalshi tickers like "KXBTC-25APR07-T67000" → extract date + strike
    # Polymarket questions like "Bitcoin Up or Down - April 7, 3:45AM-4:00AM ET"
    # Match by date and direction

    for km in kalshi_markets:
        if km.yes_price <= 0 or km.no_price <= 0:
            continue

        for pm in poly_markets:
            poly_yes = pm.get("up_price", 0)
            poly_no = pm.get("down_price", 0)

            if poly_yes <= 0 or poly_no <= 0:
                continue

            # Strategy 1: Buy Poly YES + Buy Kalshi NO
            cost1 = poly_yes + (1 - km.yes_price)
            if cost1 < (1.0 - min_profit):
                opps.append(CrossArbOpportunity(
                    polymarket_cid=pm.get("condition_id", ""),
                    kalshi_ticker=km.ticker,
                    poly_yes=poly_yes,
                    kalshi_no=1 - km.yes_price,
                    combined_cost=cost1,
                    profit=1.0 - cost1,
                    direction="poly_yes_kalshi_no",
                    market_title=km.title,
                ))

            # Strategy 2: Buy Poly NO + Buy Kalshi YES
            cost2 = poly_no + (1 - km.no_price)
            if cost2 < (1.0 - min_profit):
                opps.append(CrossArbOpportunity(
                    polymarket_cid=pm.get("condition_id", ""),
                    kalshi_ticker=km.ticker,
                    poly_yes=poly_no,
                    kalshi_no=1 - km.no_price,
                    combined_cost=cost2,
                    profit=1.0 - cost2,
                    direction="poly_no_kalshi_yes",
                    market_title=km.title,
                ))

    return sorted(opps, key=lambda o: o.profit, reverse=True)
