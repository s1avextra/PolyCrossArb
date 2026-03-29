"""Tier 1 LP solver: maximise guaranteed profit subject to polytope constraints.

Uses PuLP + CBC (free) to solve the integer-linear program:
  - Variables: position sizes for each outcome (how many shares to buy/sell)
  - Objective: maximise worst-case profit across all possible resolutions
  - Constraints: polytope feasibility + liquidity bounds
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
import pulp

from polycrossarb.arb.polytope import PolytopeConstraints
from polycrossarb.execution.fees import (
    calculate_taker_fee,
    estimate_gas_cost,
)
from polycrossarb.graph.screener import EventGroup

log = logging.getLogger(__name__)


@dataclass
class TradeOrder:
    """A single order to be placed."""
    market_condition_id: str
    outcome_idx: int  # 0=YES, 1=NO
    side: str  # "buy" or "sell"
    size: float  # number of shares/contracts
    price: float  # limit price
    expected_cost: float  # price * size (negative if selling)

    @property
    def var_key(self) -> str:
        return f"{self.market_condition_id}:{self.outcome_idx}"


@dataclass
class SolverResult:
    """Result of the LP solver."""
    status: str  # "optimal", "infeasible", "unbounded", etc.
    guaranteed_profit: float  # profit AFTER fees
    gross_profit: float = 0.0  # profit before fees
    orders: list[TradeOrder] = field(default_factory=list)
    total_cost: float = 0.0
    total_revenue: float = 0.0
    trading_fees: float = 0.0
    gas_fees: float = 0.0

    @property
    def is_optimal(self) -> bool:
        return self.status == "optimal"

    @property
    def total_fees(self) -> float:
        return self.trading_fees + self.gas_fees


def _get_executable_prices(group: EventGroup) -> tuple[list[float], list[float], float]:
    """Get executable prices from order books, falling back to mid-prices.

    Returns (exec_prices, spreads, estimated_spread_cost_per_set).
    For overpriced: exec_price = best_bid (what we can sell at).
    For underpriced: exec_price = best_ask (what we can buy at).
    """
    mid_prices = group.yes_prices
    price_sum = sum(mid_prices)
    is_over = price_sum > 1.0

    exec_prices: list[float] = []
    spreads: list[float] = []

    for i, market in enumerate(group.markets):
        mid = mid_prices[i]
        book = market.outcomes[0].order_book if market.outcomes else None

        if book is not None:
            if is_over:
                # Selling: use best bid
                ep = book.best_bid if book.best_bid is not None else mid
            else:
                # Buying: use best ask
                ep = book.best_ask if book.best_ask is not None else mid
            spread = abs(ep - mid)
        else:
            # No order book — use mid and estimate spread as 1% of price
            ep = mid
            spread = mid * 0.01

        exec_prices.append(ep)
        spreads.append(spread)

    spread_cost = sum(spreads)
    return exec_prices, spreads, spread_cost


def solve_partition_arb(
    group: EventGroup,
    max_position_usd: float = 100.0,
    liquidity_caps: dict[str, float] | None = None,
) -> SolverResult:
    """Solve for optimal position in a single partition (event group).

    Uses executable prices (best bid for selling, best ask for buying)
    instead of mid-prices. Correctly models neg_risk collateral:
      - Selling YES requires (1 - price) collateral per share per outcome
      - Buying YES costs price per share per outcome

    Accounts for: trading fees, gas fees, spread costs.
    """
    POLYMARKET_MIN_ORDER_USD = 1.0  # Polymarket rejects orders below $1

    if not group.markets:
        return SolverResult(status="empty", guaranteed_profit=0.0)

    mid_prices = group.yes_prices
    if not mid_prices or any(p <= 0 for p in mid_prices):
        return SolverResult(status="no_prices", guaranteed_profit=0.0)

    mid_sum = sum(mid_prices)
    n = len(mid_prices)

    # Get executable prices from order books
    exec_prices, spreads, spread_cost = _get_executable_prices(group)
    exec_sum = sum(exec_prices)

    # Check bid-side liquidity: every leg must have bids (sellable/exitable)
    for i, market in enumerate(group.markets):
        book = market.outcomes[0].order_book if market.outcomes else None
        if book is not None and not book.bids:
            return SolverResult(status="no_exit_liquidity", guaranteed_profit=0.0)

    # Check minimum order size: price * size must be >= $1 per leg
    # Calculate the minimum size that satisfies $1 on the cheapest leg
    min_price = min(exec_prices)
    if min_price > 0:
        min_size_for_cheapest = POLYMARKET_MIN_ORDER_USD / min_price
    else:
        return SolverResult(status="zero_price_leg", guaranteed_profit=0.0)

    prob = pulp.LpProblem("partition_arb", pulp.LpMaximize)
    # Enforce minimum size so every leg meets the $1 order minimum
    size = pulp.LpVariable("set_size", lowBound=min_size_for_cheapest)

    if mid_sum > 1.0:
        # OVERPRICED: sell YES on all → collect exec_sum per set, pay out 1.0
        # Use executable bid prices (what we actually receive)
        profit_per_set = exec_sum - 1.0
        if profit_per_set <= 0:
            return SolverResult(status="spread_kills_arb", guaranteed_profit=0.0)

        prob += size * profit_per_set, "maximize_profit"

        # Neg_risk collateral: selling YES requires (1 - price) per share per outcome.
        # The binding constraint is the outcome with the HIGHEST collateral requirement.
        # Total collateral needed = size * max(1 - price_i for all i)
        max_collateral_per_share = max(1 - p for p in exec_prices)
        prob += size * max_collateral_per_share <= max_position_usd, "collateral_limit"

    else:
        # UNDERPRICED: buy YES on all → pay exec_sum per set, receive 1.0
        # Use executable ask prices (what we actually pay)
        profit_per_set = 1.0 - exec_sum
        if profit_per_set <= 0:
            return SolverResult(status="spread_kills_arb", guaranteed_profit=0.0)

        prob += size * profit_per_set, "maximize_profit"

        # Buying costs exec_sum per set
        prob += size * exec_sum <= max_position_usd, "capital_limit"

    # Liquidity constraints: cap at 50% of visible bid depth per leg
    for i, market in enumerate(group.markets):
        book = market.outcomes[0].order_book if market.outcomes else None
        if book is not None and book.bids:
            total_bid_depth = sum(float(b.size) for b in book.bids)
            depth_cap = total_bid_depth * 0.5  # don't take more than 50% of depth
            if depth_cap > 0:
                prob += size <= depth_cap, f"depth_{i}"

    # Additional liquidity constraints if provided
    if liquidity_caps:
        for i, market in enumerate(group.markets):
            cap = liquidity_caps.get(market.condition_id, float("inf"))
            if cap < float("inf"):
                prob += size <= cap, f"liquidity_{i}"

    # Solve
    prob.solve(pulp.PULP_CBC_CMD(msg=0))

    status = pulp.LpStatus[prob.status]
    if status != "Optimal":
        return SolverResult(status=status.lower(), guaranteed_profit=0.0)

    opt_size = max(0.0, pulp.value(size))
    gross_profit = opt_size * profit_per_set

    # Generate orders at executable prices
    orders: list[TradeOrder] = []
    total_cost = 0.0
    total_rev = 0.0

    for i, market in enumerate(group.markets):
        ep = exec_prices[i]
        if mid_sum > 1.0:
            order_cost = -ep * opt_size
            orders.append(TradeOrder(
                market_condition_id=market.condition_id,
                outcome_idx=0,
                side="sell",
                size=opt_size,
                price=ep,
                expected_cost=order_cost,
            ))
            total_rev += ep * opt_size
        else:
            order_cost = ep * opt_size
            orders.append(TradeOrder(
                market_condition_id=market.condition_id,
                outcome_idx=0,
                side="buy",
                size=opt_size,
                price=ep,
                expected_cost=order_cost,
            ))
            total_cost += order_cost

    # ── Calculate all costs ───────────────────────────────────────
    # Trading fees per leg — uses live API when token_id available
    trading_fees = 0.0
    for i, market in enumerate(group.markets):
        cat = market.category.lower() if market.category else "other"
        token_id = market.outcomes[0].token_id if market.outcomes else ""
        trading_fees += calculate_taker_fee(opt_size, exec_prices[i], cat, token_id)

    # Gas fees: 1 tx per leg (live Polygon gas price)
    gas_fees = estimate_gas_cost(n)

    net_profit = gross_profit - trading_fees - gas_fees

    return SolverResult(
        status="optimal",
        guaranteed_profit=net_profit,
        gross_profit=gross_profit,
        orders=orders,
        total_cost=total_cost,
        total_revenue=total_rev,
        trading_fees=trading_fees,
        gas_fees=gas_fees,
    )


def _capital_turnover_score(group: EventGroup, result: SolverResult) -> float:
    """Score a trade by capital efficiency: profit per dollar per day.

    Prioritises:
      1. High profit/capital ratio (immediate return)
      2. Fast-resolving events (capital freed sooner for reuse)
      3. Fewer legs (lower execution risk)

    This maximises bankroll compound growth rate.
    """
    cost = max(result.total_cost, result.total_revenue, 0.01)
    profit_ratio = result.guaranteed_profit / cost

    # Estimate days until resolution from end_date
    import time
    days_to_resolve = 365.0  # default if no end date
    for m in group.markets:
        if m.end_date:
            try:
                from datetime import datetime
                end = datetime.fromisoformat(m.end_date.replace("Z", "+00:00"))
                delta = (end - datetime.now(end.tzinfo)).total_seconds() / 86400
                if 0 < delta < days_to_resolve:
                    days_to_resolve = delta
            except (ValueError, TypeError):
                pass

    # Annualised return: (profit_ratio / days) * 365
    daily_return = profit_ratio / max(days_to_resolve, 0.5)

    # Penalty for many legs (execution risk)
    leg_penalty = 1.0 / (1 + len(group.markets) * 0.02)

    return daily_return * leg_penalty


def solve_all_partitions(
    partitions: list[EventGroup],
    max_position_per_event: float = 100.0,
    max_total_exposure: float = 500.0,
    min_profit: float = 0.05,
    liquidity_caps: dict[str, float] | None = None,
) -> list[SolverResult]:
    """Solve LP for all partitions and return profitable results.

    Capital allocation strategy:
      1. Solve each partition independently
      2. Score by capital turnover (profit/$/day) — faster-resolving events
         compound bankroll growth because capital is freed sooner
      3. Allocate greedily from highest score down
    """
    # First pass: solve each partition independently
    candidates: list[tuple[EventGroup, SolverResult]] = []
    for group in partitions:
        if not group.is_neg_risk:
            continue
        result = solve_partition_arb(
            group,
            max_position_usd=max_position_per_event,
            liquidity_caps=liquidity_caps,
        )
        if result.is_optimal and result.guaranteed_profit >= min_profit:
            candidates.append((group, result))

    # Sort by capital turnover score (compound growth optimiser)
    candidates.sort(
        key=lambda x: _capital_turnover_score(x[0], x[1]),
        reverse=True,
    )

    # Portfolio-aware greedy allocation:
    # Track which market condition_ids we already have exposure to.
    # If a new arb shares outcomes with an existing position, halve
    # the allocation to avoid correlated risk concentration.
    results: list[SolverResult] = []
    remaining_capital = max_total_exposure
    exposed_markets: set[str] = set()  # condition_ids already in portfolio

    for group, result in candidates:
        # Check for overlap with existing positions
        group_cids = {m.condition_id for m in group.markets}
        overlap = group_cids & exposed_markets
        correlation_factor = 0.5 if overlap else 1.0

        effective_max = min(
            max_position_per_event * correlation_factor,
            remaining_capital,
        )

        if effective_max < 1.0:
            continue

        # Re-solve with adjusted capital if correlation found
        if correlation_factor < 1.0 or result.total_cost > effective_max:
            result = solve_partition_arb(
                group,
                max_position_usd=effective_max,
                liquidity_caps=liquidity_caps,
            )
            if not result.is_optimal or result.guaranteed_profit < min_profit:
                continue

        cost = max(result.total_cost, result.total_revenue)
        if cost <= remaining_capital:
            results.append(result)
            remaining_capital -= cost
            exposed_markets.update(group_cids)
        elif remaining_capital > 1.0:
            scaled = solve_partition_arb(
                group,
                max_position_usd=min(remaining_capital, effective_max),
                liquidity_caps=liquidity_caps,
            )
            if scaled.is_optimal and scaled.guaranteed_profit >= min_profit:
                results.append(scaled)
                remaining_capital -= max(scaled.total_cost, scaled.total_revenue)
                exposed_markets.update(group_cids)

        if remaining_capital <= 0:
            break

    log.info(
        "Solver: %d/%d profitable trades, total profit $%.2f",
        len(results),
        len(candidates),
        sum(r.guaranteed_profit for r in results),
    )
    return results
