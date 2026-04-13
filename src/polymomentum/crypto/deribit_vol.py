"""Fetch BTC implied volatility from Deribit options API.

Deribit provides free public market data (no API key needed).
We fetch ATM implied volatility from near-term BTC options.
"""
from __future__ import annotations

import logging
import math
import time

import httpx

log = logging.getLogger(__name__)

DERIBIT_API = "https://www.deribit.com/api/v2/public"
_cache: tuple[float, float] | None = None  # (iv, timestamp)
_CACHE_TTL = 60  # seconds


async def fetch_btc_implied_vol() -> float | None:
    """Fetch BTC ATM implied volatility from Deribit.

    Returns annualized IV as a fraction (e.g. 0.65 = 65%).
    Cached for 60 seconds.
    """
    global _cache
    now = time.time()
    if _cache and now - _cache[1] < _CACHE_TTL:
        return _cache[0]

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{DERIBIT_API}/get_book_summary_by_currency",
                params={"currency": "BTC", "kind": "option"},
            )
            resp.raise_for_status()
            data = resp.json()

            results = data.get("result", [])
            if not results:
                return None

            # Get current BTC index price
            idx_resp = await client.get(
                f"{DERIBIT_API}/get_index_price",
                params={"index_name": "btc_usd"},
            )
            idx_data = idx_resp.json()
            btc_price = idx_data.get("result", {}).get("index_price", 0)
            if btc_price <= 0:
                return None

            # Filter: ATM options (strike within 5% of spot), expiring 1-14 days
            atm_ivs = []
            for opt in results:
                iv = opt.get("mark_iv", 0)
                if iv <= 0:
                    continue

                # Parse instrument name for strike and expiry
                # Format: BTC-28MAR26-67000-C
                name = opt.get("instrument_name", "")
                parts = name.split("-")
                if len(parts) < 4:
                    continue

                try:
                    strike = float(parts[2])
                except ValueError:
                    continue

                # Check strike is near ATM (within 5%)
                ratio = strike / btc_price
                if ratio < 0.95 or ratio > 1.05:
                    continue

                # IV from Deribit is in percentage (e.g. 65.5 = 65.5%)
                atm_ivs.append(iv / 100.0)

            if not atm_ivs:
                return None

            avg_iv = sum(atm_ivs) / len(atm_ivs)
            _cache = (avg_iv, now)

            log.info("Deribit BTC IV: %.1f%% (from %d ATM options)", avg_iv * 100, len(atm_ivs))
            return avg_iv

    except Exception:
        log.debug("Deribit IV fetch failed", exc_info=True)
        return None
