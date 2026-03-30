"""Tiered LP solver: slippage-aware arbitrage with order book depth modeling.

Instead of assuming we buy/sell at a single price (best bid/ask), this solver
models each order book level as a separate tier with limited capacity.

For overpriced events (sum YES > 1.0), strategy = BUY NO on all outcomes:
  - Each NO ask level is a tier: {price, available_size}
  - The LP decides how much to buy from each tier
  - Larger orders eat deeper into the book → higher average price
  - Concentration penalty prevents consuming >70% of any tier

This solves the fundamental problem: our order moves the market.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pulp

from polycrossarb.data.models import Market
from polycrossarb.execution.fees import calculate_taker_fee, estimate_gas_cost
from polycrossarb.graph.screener import EventGroup

log = logging.getLogger(__name__)

CONCENTRATION_LIMIT = 0.70  # max fraction of any tier to consume
MIN_ORDER_SHARES = 1.0      # minimum shares per leg
POLYMARKET_MIN_USD = 1.0    # Polymarket minimum order value


@dataclass
class TieredOrder:
    """An order split across multiple price tiers."""
    market_condition_id: str
    outcome_idx: int
    side: str
    tiers: list[tuple[float, float]]  # [(price, size), ...]
    total_size: float
    avg_price: float
    total_cost: float
    neg_risk: bool = False

    @property
    def var_key(self) -> str:
        return f"{self.market_condition_id}:{self.outcome_idx}"


@dataclass
class TieredResult:
    """Result of the tiered LP solver."""
    status: str
    guaranteed_profit: float
    gross_profit: float = 0.0
    orders: list[TieredOrder] = field(default_factory=list)
    total_cost: float = 0.0
    trading_fees: float = 0.0
    gas_fees: float = 0.0
    set_size: float = 0.0

    @property
    def is_profitable(self) -> bool:
        return self.status == "optimal" and self.guaranteed_profit > 0

    @property
    def total_fees(self) -> float:
        return self.trading_fees + self.gas_fees


def _get_no_ask_tiers(market: Market) -> list[tuple[float, float]]:
    """Get NO-side ask tiers from the YES-side order book.

    In neg_risk: NO ask price ≈ 1 - YES bid price.
    Each YES bid level becomes a NO ask level (inverted).
    """
    if not market.outcomes or not market.outcomes[0].order_book:
        return []

    yes_book = market.outcomes[0].order_book
    if not yes_book.bids:
        return []

    # YES bids → NO asks (price inverted, size preserved)
    # YES bid at 0.60 with 100 shares → NO ask at 0.40 with 100 shares
    tiers = []
    for bid in yes_book.bids:
        no_price = 1.0 - bid.price
        if no_price > 0 and bid.size > 0:
            tiers.append((no_price, bid.size))

    # Sort NO asks ascending (cheapest first)
    tiers.sort(key=lambda t: t[0])
    return tiers


def solve_tiered_arb(
    group: EventGroup,
    max_capital: float = 100.0,
    min_profit: float = 0.10,
    max_concentration: float = CONCENTRATION_LIMIT,
) -> TieredResult:
    """Solve arbitrage with tiered order book pricing.

    For overpriced events: BUY NO on all outcomes.
    Models each order book level as a tier with limited capacity.
    """
    if not group.markets or not group.is_neg_risk:
        return TieredResult(status="empty", guaranteed_profit=0.0)

    n = len(group.markets)
    yes_prices = group.yes_prices
    if not yes_prices or any(p <= 0 for p in yes_prices):
        return TieredResult(status="no_prices", guaranteed_profit=0.0)

    yes_sum = sum(yes_prices)
    if yes_sum <= 1.0:
        return TieredResult(status="not_overpriced", guaranteed_profit=0.0)

    # Get NO ask tiers for each outcome
    all_tiers: list[list[tuple[float, float]]] = []
    for market in group.markets:
        tiers = _get_no_ask_tiers(market)
        if not tiers:
            return TieredResult(status="no_book", guaranteed_profit=0.0)
        all_tiers.append(tiers)

    # ── Build LP ──────────────────────────────────────────────────

    prob = pulp.LpProblem("TieredArb", pulp.LpMaximize)

    # S = set size (same number of shares on each outcome)
    S = pulp.LpVariable("S", lowBound=MIN_ORDER_SHARES)

    # x[i][k] = shares bought from tier k of outcome i
    x: dict[int, dict[int, pulp.LpVariable]] = {}
    total_cost_expr = 0

    for i, tiers in enumerate(all_tiers):
        x[i] = {}
        for k, (price, available) in enumerate(tiers):
            # Cap at concentration limit of available size
            max_from_tier = available * max_concentration
            x[i][k] = pulp.LpVariable(f"x_{i}_{k}", lowBound=0, upBound=max_from_tier)
            total_cost_expr += price * x[i][k]

        # Total shares from all tiers of outcome i must equal S
        prob += pulp.lpSum(x[i].values()) == S, f"set_size_{i}"

    # Payout: (N-1) × S (exactly N-1 NOs win when one outcome resolves)
    payout = (n - 1) * S

    # Profit = payout - cost
    profit = payout - total_cost_expr

    # Objective: maximize profit
    prob += profit, "maximize_profit"

    # Capital constraint
    prob += total_cost_expr <= max_capital, "capital_limit"

    # Minimum profit constraint
    prob += profit >= min_profit, "min_profit"

    # ── Solve ─────────────────────────────────────────────────────

    prob.solve(pulp.PULP_CBC_CMD(msg=0, timeLimit=5))

    status = pulp.LpStatus[prob.status]
    if status != "Optimal":
        return TieredResult(status=status.lower(), guaranteed_profit=0.0)

    opt_S = max(0.0, pulp.value(S))
    opt_profit = pulp.value(prob.objective)
    opt_cost = sum(
        tiers[k][0] * pulp.value(x[i][k])
        for i, tiers in enumerate(all_tiers)
        for k in range(len(tiers))
        if pulp.value(x[i][k]) > 0.01
    )

    if opt_profit <= 0 or opt_S < MIN_ORDER_SHARES:
        return TieredResult(status="no_profit", guaranteed_profit=0.0)

    # ── Build orders ──────────────────────────────────────────────

    orders: list[TieredOrder] = []
    total_trading_fees = 0.0

    for i, market in enumerate(group.markets):
        tiers_used: list[tuple[float, float]] = []
        leg_cost = 0.0
        leg_size = 0.0

        for k, (price, _) in enumerate(all_tiers[i]):
            qty = pulp.value(x[i][k])
            if qty > 0.01:
                tiers_used.append((price, qty))
                leg_cost += price * qty
                leg_size += qty

        if leg_size < 0.01:
            continue

        avg_price = leg_cost / leg_size

        # Fee calculation
        cat = market.category.lower() if market.category else "other"
        token_id = market.outcomes[1].token_id if len(market.outcomes) > 1 else ""
        fee = calculate_taker_fee(leg_size, avg_price, cat, token_id)
        total_trading_fees += fee

        orders.append(TieredOrder(
            market_condition_id=market.condition_id,
            outcome_idx=1,  # NO token
            side="buy",
            tiers=tiers_used,
            total_size=leg_size,
            avg_price=avg_price,
            total_cost=leg_cost,
            neg_risk=True,
        ))

    gas_fees = estimate_gas_cost(n)
    net_profit = opt_profit - total_trading_fees - gas_fees

    result = TieredResult(
        status="optimal",
        guaranteed_profit=net_profit,
        gross_profit=opt_profit,
        orders=orders,
        total_cost=opt_cost,
        trading_fees=total_trading_fees,
        gas_fees=gas_fees,
        set_size=opt_S,
    )

    log.info(
        "Tiered solver: %d legs, S=%.1f, cost=$%.2f, gross=$%.4f, fees=$%.4f, net=$%.4f",
        len(orders), opt_S, opt_cost, opt_profit, total_trading_fees + gas_fees, net_profit,
    )
    return result
