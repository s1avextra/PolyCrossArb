"""Fair value calculator for BTC binary option contracts.

Given: current BTC price, strike price, time to expiry, volatility
Compute: probability that BTC exceeds strike by expiry

Uses Black-Scholes formula for digital (binary) options:
  P(BTC > K at T) = N(d2)
  where d2 = [ln(S/K) + (r - σ²/2)T] / (σ√T)

This is the fair value of a Polymarket "Will BTC exceed $X?" contract.
If the market price diverges from this, there's an arb opportunity.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class FairValueResult:
    """Fair value calculation for a binary contract."""
    fair_price: float       # probability [0, 1]
    market_price: float     # current Polymarket price
    edge: float             # fair_price - market_price (positive = buy signal)
    edge_pct: float         # edge as % of market price
    d2: float               # Black-Scholes d2 parameter
    btc_price: float
    strike: float
    days_to_expiry: float
    volatility: float


def _norm_cdf(x: float) -> float:
    """Standard normal CDF using math.erf (ULP-accurate).

    Previously used a hand-rolled Abramowitz & Stegun approximation that had
    systematic errors up to 3.7 percentage points at moderate |x| because the
    polynomial approximates erf(x), not N(x), and the argument must be
    scaled by 1/sqrt(2). We now use Python's stdlib erf which is exact to
    machine precision.

    N(x) = (1 + erf(x / sqrt(2))) / 2
    """
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def compute_fair_value(
    btc_price: float,
    strike: float,
    days_to_expiry: float,
    volatility: float,
    market_price: float = 0.0,
    risk_free_rate: float = 0.05,
) -> FairValueResult:
    """Compute fair value of a binary "BTC > strike" contract.

    Uses Black-Scholes for digital options:
      P(S > K) = N(d2)
      d2 = [ln(S/K) + (r - σ²/2)T] / (σ√T)

    Args:
        btc_price: Current BTC spot price
        strike: Strike price (e.g. 155000)
        days_to_expiry: Days until contract resolution
        volatility: Annualized volatility (e.g. 0.50 = 50%)
        market_price: Current Polymarket price for edge calculation
        risk_free_rate: Annual risk-free rate (default 5%)

    Returns:
        FairValueResult with fair price and edge
    """
    if btc_price <= 0 or strike <= 0 or days_to_expiry <= 0 or volatility < 0.01:
        return FairValueResult(
            fair_price=0.5, market_price=market_price, edge=0,
            edge_pct=0, d2=0, btc_price=btc_price, strike=strike,
            days_to_expiry=days_to_expiry, volatility=volatility,
        )

    T = days_to_expiry / 365.25  # years
    S = btc_price
    K = strike
    r = risk_free_rate
    sigma = volatility

    d2 = (math.log(S / K) + (r - 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    fair_price = _norm_cdf(d2)

    # Clamp to [0.01, 0.99] — log when clamping masks extreme values
    raw_fair_price = fair_price
    fair_price = max(0.01, min(0.99, fair_price))
    if raw_fair_price != fair_price:
        log.debug("fair_value.clamped raw=%.4f clamped=%.4f d2=%.2f vol=%.4f T=%.6f",
                  raw_fair_price, fair_price, d2, sigma, T)

    edge = fair_price - market_price
    edge_pct = edge / max(market_price, 0.01)

    return FairValueResult(
        fair_price=fair_price,
        market_price=market_price,
        edge=edge,
        edge_pct=edge_pct,
        d2=d2,
        btc_price=btc_price,
        strike=strike,
        days_to_expiry=days_to_expiry,
        volatility=volatility,
    )
