"""Polymarket CLOB + Gamma API client (REST)."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx

from polycrossarb.config import settings
from polycrossarb.data.models import Market, OrderBook, OrderBookLevel, Outcome

log = logging.getLogger(__name__)

# Polymarket uses two APIs:
#   - Gamma API: market metadata (questions, outcomes, tags, event grouping)
#   - CLOB API:  order books, prices, trading
#
# Gamma is the primary source for market discovery.
# CLOB provides real-time prices and order book depth.

def _parse_json_or_csv(val: str | list) -> list[str]:
    """Parse a field that may be a JSON array string, CSV string, or already a list."""
    if isinstance(val, list):
        return [str(v) for v in val]
    if not isinstance(val, str) or not val.strip():
        return []
    val = val.strip()
    if val.startswith("["):
        try:
            parsed = json.loads(val)
            return [str(v) for v in parsed]
        except json.JSONDecodeError:
            pass
    return [s.strip().strip('"') for s in val.split(",") if s.strip()]


_GAMMA_MARKETS = "/markets"
_CLOB_BOOK = "/book"
_CLOB_PRICE = "/price"
_CLOB_MIDPOINT = "/midpoint"


class PolymarketClient:
    def __init__(
        self,
        gamma_url: str = settings.poly_gamma_url,
        clob_url: str = settings.poly_base_url,
        timeout: float = 15.0,
        max_retries: int = 3,
    ):
        self._gamma_url = gamma_url.rstrip("/")
        self._clob_url = clob_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _request(self, base_url: str, path: str, params: dict | None = None) -> Any:
        client = await self._get_client()
        url = f"{base_url}{path}"
        last_exc = None
        for attempt in range(self._max_retries):
            try:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                last_exc = exc
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429:
                    wait = 2 ** attempt
                    log.warning("Rate limited, waiting %ds", wait)
                    await asyncio.sleep(wait)
                elif attempt < self._max_retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    raise
        raise last_exc  # type: ignore[misc]

    # ── Gamma API (market discovery) ──────────────────────────────────

    async def fetch_markets_page(
        self,
        limit: int = 100,
        offset: int = 0,
        active: bool = True,
        closed: bool = False,
    ) -> list[dict]:
        """Fetch a page of markets from Gamma API."""
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "active": str(active).lower(),
            "closed": str(closed).lower(),
        }
        data = await self._request(self._gamma_url, _GAMMA_MARKETS, params)
        if isinstance(data, list):
            return data
        return data.get("data", data.get("markets", []))

    async def fetch_all_active_markets(
        self,
        min_volume: float = 0.0,
        min_liquidity: float = 0.0,
    ) -> list[Market]:
        """Fetch ALL active markets, paginating until exhausted.

        No artificial cap — fetches every page the API returns.
        Filters out markets with zero prices, no outcomes, or below
        minimum volume/liquidity thresholds.
        """
        all_markets: list[Market] = []
        offset = 0
        page_size = 100

        while True:
            raw = await self.fetch_markets_page(limit=page_size, offset=offset)
            if not raw:
                break
            for item in raw:
                market = self._parse_gamma_market(item)
                if market is None:
                    continue
                # Filter: must have outcomes with valid prices and token IDs
                if not market.outcomes or all(o.price == 0 for o in market.outcomes):
                    continue
                if any(not o.token_id for o in market.outcomes):
                    continue
                if market.volume < min_volume:
                    continue
                if market.liquidity < min_liquidity:
                    continue
                all_markets.append(market)
            if len(raw) < page_size:
                break
            offset += page_size

        log.info("Fetched %d active markets (filtered)", len(all_markets))
        return all_markets

    def _parse_gamma_market(self, raw: dict) -> Market | None:
        """Parse a Gamma API market response into our Market model."""
        try:
            condition_id = raw.get("conditionId") or raw.get("condition_id", "")
            question = raw.get("question", "")
            if not condition_id or not question:
                return None

            # Parse outcomes — Gamma returns JSON-encoded strings like '["Yes","No"]'
            outcome_names = _parse_json_or_csv(raw.get("outcomes", ""))
            outcome_prices = _parse_json_or_csv(raw.get("outcomePrices") or raw.get("outcome_prices", ""))
            outcome_prices = [float(p) for p in outcome_prices]
            clob_token_ids = _parse_json_or_csv(raw.get("clobTokenIds") or raw.get("clob_token_ids", ""))

            outcomes: list[Outcome] = []
            for i, name in enumerate(outcome_names):
                price = outcome_prices[i] if i < len(outcome_prices) else 0.0
                token_id = clob_token_ids[i] if i < len(clob_token_ids) else ""
                outcomes.append(Outcome(token_id=token_id, name=name, price=price))

            tags_raw = raw.get("tags") or []
            if isinstance(tags_raw, str):
                tags_raw = [t.strip() for t in tags_raw.split(",") if t.strip()]

            # Extract event grouping from the events array
            events_list = raw.get("events") or []
            event_slug = ""
            event_id = ""
            event_title = ""
            if isinstance(events_list, list) and events_list:
                ev = events_list[0]
                event_slug = ev.get("slug", "")
                event_id = str(ev.get("id", ""))
                event_title = ev.get("title", "")

            return Market(
                condition_id=condition_id,
                question=question,
                slug=raw.get("slug", ""),
                outcomes=outcomes,
                tags=tags_raw,
                category=raw.get("category", ""),
                active=raw.get("active", True),
                closed=raw.get("closed", False),
                volume=float(raw.get("volume", 0) or 0),
                liquidity=float(raw.get("liquidity", 0) or 0),
                end_date=raw.get("endDate") or raw.get("end_date", ""),
                event_slug=event_slug,
                event_id=event_id,
                event_title=event_title,
                group_slug=raw.get("groupSlug") or raw.get("group_slug", ""),
                neg_risk=bool(raw.get("negRisk", False)),
            )
        except Exception:
            log.debug("Failed to parse market: %s", raw.get("question", "?"), exc_info=True)
            return None

    # ── CLOB API (prices & order books) ───────────────────────────────

    async def fetch_order_book(self, token_id: str) -> OrderBook:
        """Fetch order book for a specific outcome token."""
        data = await self._request(self._clob_url, _CLOB_BOOK, {"token_id": token_id})
        bids = [
            OrderBookLevel(price=float(b["price"]), size=float(b["size"]))
            for b in data.get("bids", [])
        ]
        asks = [
            OrderBookLevel(price=float(a["price"]), size=float(a["size"]))
            for a in data.get("asks", [])
        ]
        # Sort: bids descending, asks ascending
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)
        return OrderBook(bids=bids, asks=asks)

    async def fetch_midpoint(self, token_id: str) -> float | None:
        """Fetch midpoint price for a token."""
        try:
            data = await self._request(self._clob_url, _CLOB_MIDPOINT, {"token_id": token_id})
            return float(data.get("mid", 0))
        except Exception:
            return None

    async def enrich_with_order_books(self, markets: list[Market], concurrency: int = 10) -> None:
        """Fetch order books for all outcomes in the given markets."""
        sem = asyncio.Semaphore(concurrency)

        async def _fetch(outcome: Outcome):
            if not outcome.token_id:
                return
            async with sem:
                try:
                    outcome.order_book = await self.fetch_order_book(outcome.token_id)
                except Exception:
                    log.debug("Failed to fetch book for %s", outcome.token_id)

        tasks = []
        for market in markets:
            for outcome in market.outcomes:
                tasks.append(_fetch(outcome))
        await asyncio.gather(*tasks)
