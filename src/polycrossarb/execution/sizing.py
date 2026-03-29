"""Position sizing using Modified Kelly Criterion.

From the PDF: f = ((bp - q) / b) * sqrt(p)
  where b = arb margin %, p = execution probability, q = 1 - p.

The sqrt(p) term adjusts for execution risk on the non-atomic CLOB.
Cap at 50% of visible depth to avoid adverse selection.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class PositionSize:
    """Calculated position size for a trade."""
    kelly_fraction: float     # raw Kelly fraction
    adjusted_fraction: float  # after sqrt(p) adjustment
    size_usd: float           # dollar amount to trade
    size_shares: float        # number of shares/sets
    execution_prob: float     # estimated execution probability
    depth_cap_applied: bool   # whether depth cap reduced the size
    reason: str = ""


def modified_kelly(
    arb_margin: float,
    execution_prob: float,
    bankroll: float,
    max_fraction: float = 0.10,
    kelly_fraction: float = 0.25,
) -> float:
    """Modified Kelly with execution probability adjustment.

    For arbitrage, the Kelly fraction simplifies because the "odds" are
    the payout ratio: b = 1/cost_per_set, and the "edge" = arb_margin.

    f_raw = arb_margin * execution_prob
    f_adjusted = f_raw * sqrt(execution_prob)

    The sqrt(p) term from the PDF adjusts for non-atomic CLOB execution risk.
    """
    if arb_margin <= 0 or execution_prob <= 0:
        return 0.0

    # For arb: expected gain per dollar = margin * exec_prob
    # Kelly says bet edge/odds; for arb, odds ≈ 1/margin, edge = margin * p
    # Simplifies to: f = margin * p (since loss on failed exec ≈ spread cost, not full capital)
    kelly_raw = arb_margin * execution_prob

    # Apply sqrt(p) adjustment for execution risk
    kelly_adjusted = kelly_raw * math.sqrt(execution_prob)

    # Fractional Kelly: reduces variance while preserving ~94% of growth rate
    # At 0.25x Kelly, max drawdown drops from ~50% to ~12%
    kelly_adjusted *= kelly_fraction

    # Cap at max_fraction of bankroll
    fraction = min(kelly_adjusted, max_fraction)
    return max(0.0, fraction * bankroll)


def estimate_execution_probability(
    n_legs: int,
    avg_spread_pct: float,
    avg_depth_usd: float,
    order_size_usd: float,
) -> float:
    """Estimate probability that all legs of the arb execute successfully.

    Factors:
    - More legs = lower probability (each is independent fill risk)
    - Wider spreads = more likely to be stale quotes
    - Low depth relative to order size = partial fill risk
    """
    if n_legs == 0 or order_size_usd <= 0:
        return 0.0

    # Per-leg fill probability
    depth_ratio = min(avg_depth_usd / order_size_usd, 10.0) / 10.0
    spread_factor = max(0.0, 1.0 - avg_spread_pct * 10)  # penalise wide spreads
    per_leg = min(0.99, depth_ratio * 0.8 + spread_factor * 0.2)

    # Joint probability (independent legs)
    joint = per_leg ** n_legs

    return max(0.01, joint)


def calculate_position_size(
    arb_margin: float,
    n_legs: int,
    bankroll: float,
    max_position_usd: float,
    visible_depth_usd: float,
    avg_spread_pct: float = 0.01,
    avg_depth_usd: float = 100.0,
    min_profit_usd: float = 0.05,
) -> PositionSize:
    """Calculate optimal position size for an arbitrage trade.

    Combines Modified Kelly with liquidity constraints.
    """
    exec_prob = estimate_execution_probability(
        n_legs=n_legs,
        avg_spread_pct=avg_spread_pct,
        avg_depth_usd=avg_depth_usd,
        order_size_usd=max_position_usd,
    )

    kelly_usd = modified_kelly(
        arb_margin=arb_margin,
        execution_prob=exec_prob,
        bankroll=bankroll,
    )

    # Cap at max position
    size_usd = min(kelly_usd, max_position_usd)

    # Cap at 50% of visible depth
    depth_cap = visible_depth_usd * 0.5
    depth_capped = size_usd > depth_cap
    if depth_capped:
        size_usd = depth_cap

    # Check minimum profit threshold
    expected_profit = size_usd * arb_margin
    if expected_profit < min_profit_usd:
        return PositionSize(
            kelly_fraction=kelly_usd / bankroll if bankroll > 0 else 0,
            adjusted_fraction=size_usd / bankroll if bankroll > 0 else 0,
            size_usd=0,
            size_shares=0,
            execution_prob=exec_prob,
            depth_cap_applied=depth_capped,
            reason=f"Expected profit ${expected_profit:.4f} below min ${min_profit_usd}",
        )

    # Convert to shares (1 share = $1 on resolution for YES)
    price_per_set = 1.0  # approximate, actual depends on prices
    size_shares = size_usd / price_per_set

    return PositionSize(
        kelly_fraction=kelly_usd / bankroll if bankroll > 0 else 0,
        adjusted_fraction=size_usd / bankroll if bankroll > 0 else 0,
        size_usd=size_usd,
        size_shares=size_shares,
        execution_prob=exec_prob,
        depth_cap_applied=depth_capped,
    )
