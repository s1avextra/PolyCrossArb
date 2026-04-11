"""Polymarket fee calculation — live API + fallback.

Fee sources (in priority order):
  1. Live API: GET /fee-rate?token_id={token_id} — always reflects current rules
  2. Hardcoded fallback: used only if the API is unreachable

The live fee cache refreshes every 5 minutes so the bot automatically adapts
when Polymarket changes fee schedules, adds categories, or adjusts rates.

Polygon gas costs are estimated from recent block data when possible.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import httpx

log = logging.getLogger(__name__)

# ── Live fee cache ────────────────────────────────────────────────────

_fee_cache: dict[str, tuple[float, float]] = {}  # token_id -> (base_fee, timestamp)
_CACHE_TTL = 300  # 5 minutes
_CLOB_BASE = "https://clob.polymarket.com"


def _parse_fee_response(data: dict) -> float | None:
    """Parse fee from API response, handling current and future field names."""
    fee = data.get("base_fee")
    if fee is None:
        for key in ("fee_rate", "fee_rate_bps", "taker_fee", "fee"):
            if key in data and isinstance(data[key], (int, float)):
                val = data[key]
                if "bps" in key and val > 1:
                    val = val / 10000
                fee = val
                break
    if fee is not None:
        return float(fee)
    return None


def _fetch_fee_rate_sync(token_id: str) -> float | None:
    """Fetch live fee rate from Polymarket CLOB API (synchronous)."""
    try:
        resp = httpx.get(
            f"{_CLOB_BASE}/fee-rate",
            params={"token_id": token_id},
            timeout=3.0,
        )
        resp.raise_for_status()
        return _parse_fee_response(resp.json())
    except Exception:
        log.debug("Fee API unavailable for token %s", token_id[:16])
    return None


def get_live_fee_rate(token_id: str) -> float | None:
    """Get fee rate for a token, using cache with TTL."""
    now = time.time()
    if token_id in _fee_cache:
        cached_fee, cached_at = _fee_cache[token_id]
        if now - cached_at < _CACHE_TTL:
            return cached_fee

    fee = _fetch_fee_rate_sync(token_id)
    if fee is not None:
        _fee_cache[token_id] = (fee, now)
    return fee


def prefetch_fee_rates(token_ids: list[str]) -> int:
    """Batch-prefetch fee rates for tokens using connection pooling.

    Strategy:
      1. Sample 5 tokens first — if all return 0, cache all as 0 (fast path)
      2. Otherwise fetch remaining uncached tokens with connection reuse

    Returns number of tokens cached.
    """
    now = time.time()
    # Deduplicate and filter already-cached
    to_fetch = list({
        tid for tid in token_ids
        if tid and (tid not in _fee_cache or now - _fee_cache[tid][1] >= _CACHE_TTL)
    })

    if not to_fetch:
        return 0

    cached = 0

    with httpx.Client(timeout=3.0) as client:
        # Step 1: Sample a few tokens to check if fees are active
        sample = to_fetch[:5]
        sample_fees = []
        for tid in sample:
            try:
                resp = client.get(f"{_CLOB_BASE}/fee-rate", params={"token_id": tid})
                resp.raise_for_status()
                fee = _parse_fee_response(resp.json())
                if fee is not None:
                    sample_fees.append(fee)
                    _fee_cache[tid] = (fee, now)
                    cached += 1
            except Exception:
                sample_fees.append(None)

        # Fast path: if all sampled fees are 0, assume all are 0
        valid_fees = [f for f in sample_fees if f is not None]
        if valid_fees and all(f == 0 for f in valid_fees):
            for tid in to_fetch:
                if tid not in _fee_cache or now - _fee_cache[tid][1] >= _CACHE_TTL:
                    _fee_cache[tid] = (0.0, now)
                    cached += 1
            log.info("Fees inactive (all 0) — cached %d tokens", cached)
            return cached

        # Step 2: Fetch remaining (fees are non-zero, need per-token rates)
        remaining = [tid for tid in to_fetch[5:] if tid not in _fee_cache or now - _fee_cache[tid][1] >= _CACHE_TTL]
        for tid in remaining:
            try:
                resp = client.get(f"{_CLOB_BASE}/fee-rate", params={"token_id": tid})
                resp.raise_for_status()
                fee = _parse_fee_response(resp.json())
                if fee is not None:
                    _fee_cache[tid] = (fee, now)
                    cached += 1
            except Exception:
                pass

    log.info("Prefetched fees for %d/%d tokens", cached, len(to_fetch))
    return cached


def clear_fee_cache() -> None:
    """Clear the fee cache (useful for testing or after fee schedule changes)."""
    _fee_cache.clear()


# ── Fallback fee schedule ─────────────────────────────────────────────
# Used ONLY when the live API is unreachable.
# Last updated: March 30, 2026 (docs.polymarket.com/trading/fees)

# Category -> (fee_rate, exponent, maker_rebate_pct)
_FALLBACK_SCHEDULE: dict[str, tuple[float, float, float]] = {
    "crypto":       (0.072, 1.0, 0.20),
    "sports":       (0.030, 1.0, 0.25),
    "finance":      (0.040, 1.0, 0.50),
    "politics":     (0.040, 1.0, 0.25),
    "economics":    (0.030, 0.5, 0.25),
    "culture":      (0.050, 1.0, 0.25),
    "weather":      (0.025, 0.5, 0.25),
    "other":        (0.200, 2.0, 0.25),
    "mentions":     (0.250, 2.0, 0.25),
    "tech":         (0.040, 1.0, 0.25),
    "geopolitics":  (0.000, 1.0, 0.00),
}

_FALLBACK_DEFAULT = "other"


def polymarket_fee(shares: float, price: float, fee_rate: float) -> float:
    """Polymarket binary-option fee formula (verified against NautilusTrader adapter).

        fee = qty * fee_rate * p * (1 - p)

    Maximum at p = 0.50. Symmetric around 0.5.
    Reference: prediction_market_extensions/adapters/polymarket/parsing.py
    in evan-kolberg/prediction-market-backtesting (NautilusTrader fork).

    This replaces the prior `qty * p * rate * (p*(1-p))^exponent` formula
    which under-counted fees by ~2x at p=0.5.
    """
    if fee_rate <= 0 or shares <= 0:
        return 0.0
    p = max(0.0, min(1.0, price))
    fee = shares * fee_rate * p * (1.0 - p)
    fee = round(fee, 5)
    if 0 < fee < 0.00001:
        fee = 0.00001
    return fee


def _fallback_fee(shares: float, price: float, category: str) -> float:
    """Calculate fee using hardcoded fallback schedule.

    Uses the verified Polymarket binary fee formula. The `exponent` field
    in the fallback schedule is retained for backward compatibility but
    is no longer applied — Polymarket's actual formula has no exponent.
    """
    cat = category.lower().strip()
    fee_rate, _exponent, _ = _FALLBACK_SCHEDULE.get(cat, _FALLBACK_SCHEDULE[_FALLBACK_DEFAULT])
    return polymarket_fee(shares, price, fee_rate)


# ── Gas cost estimation ───────────────────────────────────────────────

_gas_cache: tuple[float, float] | None = None  # (cost_usd, timestamp)
_GAS_CACHE_TTL = 600  # 10 minutes
_DEFAULT_GAS_USD = 0.005


def _fetch_polygon_gas_price() -> float | None:
    """Fetch current Polygon gas price and estimate tx cost in USD."""
    try:
        # Use Polygon's gas station API
        resp = httpx.get("https://gasstation.polygon.technology/v2", timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        # standard speed gas price in Gwei
        standard = data.get("standard", {})
        max_fee = standard.get("maxFee", 30)  # Gwei

        # Typical CLOB order ≈ 150k gas
        # POL price: fetch from a simple source
        gas_cost_pol = (150_000 * max_fee) / 1e9  # in POL

        # Get POL/USD price (rough estimate from Polygon RPC is complex,
        # use a simple heuristic: POL ≈ $0.40-0.60, use $0.50 as default)
        pol_resp = httpx.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "polygon-ecosystem-token", "vs_currencies": "usd"},
            timeout=5.0,
        )
        pol_price = 0.50  # default
        if pol_resp.status_code == 200:
            cg_data = pol_resp.json()
            pol_price = cg_data.get("polygon-ecosystem-token", {}).get("usd", 0.50)

        return gas_cost_pol * pol_price
    except Exception:
        log.debug("Gas price fetch failed", exc_info=True)
        return None


def estimate_gas_cost(n_transactions: int) -> float:
    """Estimate Polygon gas cost for N transactions, with live price."""
    global _gas_cache
    now = time.time()

    if _gas_cache and now - _gas_cache[1] < _GAS_CACHE_TTL:
        per_tx = _gas_cache[0]
    else:
        live = _fetch_polygon_gas_price()
        if live is not None:
            per_tx = live
            _gas_cache = (per_tx, now)
        else:
            per_tx = _DEFAULT_GAS_USD

    return round(n_transactions * per_tx, 6)


# ── Public API ────────────────────────────────────────────────────────

def calculate_taker_fee(
    shares: float,
    price: float,
    category: str = _FALLBACK_DEFAULT,
    token_id: str = "",
) -> float:
    """Calculate taker fee for a single order.

    Uses cached live rate if available (no API calls from here).
    Falls back to hardcoded schedule if cache miss.
    Call prefetch_fee_rates() before solving to warm the cache.

    Args:
        shares: Number of shares traded
        price: Share price (0 to 1)
        category: Market category (for fallback)
        token_id: Outcome token ID (for cache lookup)

    Returns:
        Fee in USD, rounded to 4 decimal places.
    """
    if shares <= 0:
        return 0.0

    # Check cache only — never make API calls from inside the solver
    if token_id and token_id in _fee_cache:
        cached_fee, cached_at = _fee_cache[token_id]
        if time.time() - cached_at < _CACHE_TTL:
            return polymarket_fee(shares, price, cached_fee)

    # Fallback to hardcoded schedule (no network call)
    return _fallback_fee(shares, price, category)


def calculate_trade_fees(
    legs: list[tuple[float, float, str, str]],
    is_maker: bool = False,
) -> float:
    """Calculate total fees for a multi-leg trade.

    Args:
        legs: List of (shares, price, category, token_id) per leg.
        is_maker: If True, apply maker rebate.

    Returns:
        Total fees in USD.
    """
    total = 0.0
    for shares, price, category, token_id in legs:
        fee = calculate_taker_fee(shares, price, category, token_id)
        if is_maker:
            cat = category.lower().strip()
            _, _, rebate_pct = _FALLBACK_SCHEDULE.get(cat, _FALLBACK_SCHEDULE[_FALLBACK_DEFAULT])
            fee *= (1 - rebate_pct)
        total += fee
    return total


def calculate_total_costs(
    legs: list[tuple[float, float, str, str]],
    is_maker: bool = False,
) -> dict[str, float]:
    """Calculate all costs: trading fees + gas."""
    trading_fees = calculate_trade_fees(legs, is_maker)
    gas = estimate_gas_cost(len(legs))
    return {
        "trading_fees": round(trading_fees, 4),
        "gas_fees": round(gas, 4),
        "total_costs": round(trading_fees + gas, 4),
    }
