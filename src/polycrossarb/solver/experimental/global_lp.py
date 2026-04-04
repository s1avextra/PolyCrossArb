"""Global portfolio LP: optimizes across multiple events simultaneously.

Instead of solving each event independently, this solver allocates
capital across the best N events, respecting global risk limits.

Key features:
  - Piecewise-linear cost modeling (accounts for slippage per tier)
  - Global capital constraint (total across all events)
  - Per-event position limits
  - Concentration penalty (max 70% of any book level)
  - Maker-aware pricing: uses limit order prices instead of market orders
    to capture maker rebates (20-50% of fees returned)

For overpriced events: BUY NO on all outcomes (same strategy as tiered).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pulp

from polycrossarb.data.models import Market
from polycrossarb.execution.fees import estimate_gas_cost
from polycrossarb.graph.screener import EventGroup

log = logging.getLogger(__name__)


@dataclass
class GlobalOrder:
    """An order from the global solver."""
    event_id: str
    event_title: str
    market_condition_id: str
    outcome_idx: int
    side: str
    size: float
    price: float  # limit price to place
    total_cost: float
    neg_risk: bool = False

    @property
    def var_key(self) -> str:
        return f"{self.market_condition_id}:{self.outcome_idx}"


@dataclass
class GlobalResult:
    """Result of the global portfolio optimizer."""
    status: str
    total_profit: float = 0.0
    events_traded: int = 0
    orders: list[GlobalOrder] = field(default_factory=list)
    total_capital_used: float = 0.0
    total_fees: float = 0.0
    per_event: dict[str, dict] = field(default_factory=dict)


def _build_no_segments(market: Market, num_segments: int = 5) -> list[dict]:
    """Build piecewise-linear cost segments from the YES bid book.

    Each YES bid level → NO ask segment (inverted price).
    Segments have increasing marginal cost as we go deeper.
    """
    if not market.outcomes or not market.outcomes[0].order_book:
        return []

    yes_book = market.outcomes[0].order_book
    if not yes_book.bids:
        return []

    # Walk the YES bid book, converting to NO ask segments
    segments = []
    for bid in yes_book.bids:
        no_price = 1.0 - bid.price
        if no_price > 0 and bid.size > 0:
            segments.append({
                "price": no_price,
                "max_size": bid.size * 0.70,  # concentration limit
                "raw_size": bid.size,
            })

    # Sort by price ascending (cheapest NO first)
    segments.sort(key=lambda s: s["price"])
    return segments[:num_segments]


def solve_global_portfolio(
    events: list[EventGroup],
    global_bankroll: float = 100.0,
    max_events: int = 5,
    min_profit_per_event: float = 0.05,
    max_capital_pct: float = 0.80,
    maker_rebate: float = 0.20,
) -> GlobalResult:
    """Solve global portfolio LP across multiple events.

    Args:
        events: List of overpriced EventGroups with order books enriched.
        global_bankroll: Total USDC available.
        max_events: Max number of events to trade simultaneously.
        min_profit_per_event: Minimum profit per event to include.
        max_capital_pct: Max fraction of bankroll to deploy.
        maker_rebate: Fraction of taker fees returned for limit orders (0.20 = 20%).
    """
    if not events:
        return GlobalResult(status="no_events")

    prob = pulp.LpProblem("GlobalPortfolio", pulp.LpMaximize)

    # Track global variables
    event_vars: dict[str, dict] = {}
    total_profit_expr = 0
    total_capital_expr = 0
    active_events: list[pulp.LpVariable] = []

    for e_idx, group in enumerate(events):
        n = len(group.markets)
        yes_sum = sum(group.yes_prices)

        if yes_sum <= 1.0:
            continue  # only overpriced

        # Build segments for each outcome's NO side
        all_segments: list[list[dict]] = []
        skip = False
        for market in group.markets:
            segs = _build_no_segments(market)
            if not segs:
                skip = True
                break
            all_segments.append(segs)

        if skip:
            continue

        # Variables for this event
        S_e = pulp.LpVariable(f"S_{e_idx}", lowBound=0)
        active_e = pulp.LpVariable(f"active_{e_idx}", cat="Binary")
        active_events.append(active_e)

        # Tier variables: x[i][k] = shares from segment k of outcome i
        x: dict[int, dict[int, pulp.LpVariable]] = {}
        event_cost = 0

        for i, segs in enumerate(all_segments):
            x[i] = {}
            for k, seg in enumerate(segs):
                x[i][k] = pulp.LpVariable(
                    f"x_{e_idx}_{i}_{k}", lowBound=0, upBound=seg["max_size"],
                )
                # Maker pricing: place limit at slightly better price
                # Instead of taking at seg["price"], rest at seg["price"] - 0.005
                maker_price = max(0.001, seg["price"] - 0.005)
                event_cost += maker_price * x[i][k]

            # All tiers of outcome i sum to S_e
            prob += pulp.lpSum(x[i].values()) == S_e, f"basket_{e_idx}_{i}"

        # Payout for this event: (N-1) × S_e
        event_payout = (n - 1) * S_e

        # Fee adjustment: maker gets rebate
        effective_fee_rate = 0.005 * (1 - maker_rebate)  # net fee after rebate
        event_profit = event_payout - (1 + effective_fee_rate) * event_cost

        # Link active binary: S_e can only be > 0 if active_e = 1
        big_M = global_bankroll * 10
        prob += S_e <= big_M * active_e, f"link_{e_idx}"

        # Per-event capital limit (20% of bankroll)
        per_event_cap = global_bankroll * 0.20
        prob += event_cost <= per_event_cap, f"event_cap_{e_idx}"

        # Minimum profit if active
        prob += event_profit >= min_profit_per_event * active_e - big_M * (1 - active_e), f"min_profit_{e_idx}"

        total_profit_expr += event_profit
        total_capital_expr += event_cost

        event_vars[group.event_id] = {
            "idx": e_idx,
            "group": group,
            "S": S_e,
            "active": active_e,
            "cost": event_cost,
            "profit": event_profit,
            "x": x,
            "segments": all_segments,
        }

    if not event_vars:
        return GlobalResult(status="no_valid_events")

    # ── Global constraints ────────────────────────────────────────

    # Total capital
    prob += total_capital_expr <= global_bankroll * max_capital_pct, "global_capital"

    # Max simultaneous events
    prob += pulp.lpSum(active_events) <= max_events, "max_events"

    # Objective
    prob += total_profit_expr, "maximize_total_profit"

    # ── Solve ─────────────────────────────────────────────────────

    prob.solve(pulp.PULP_CBC_CMD(msg=0, timeLimit=15))

    status = pulp.LpStatus[prob.status]
    if status != "Optimal":
        return GlobalResult(status=status.lower())

    # ── Extract results ───────────────────────────────────────────

    orders: list[GlobalOrder] = []
    per_event: dict[str, dict] = {}
    total_profit = 0.0
    total_capital = 0.0
    events_traded = 0

    for eid, ev in event_vars.items():
        is_active = pulp.value(ev["active"]) > 0.5
        if not is_active:
            continue

        events_traded += 1
        S_val = pulp.value(ev["S"])
        cost_val = pulp.value(ev["cost"])
        profit_val = pulp.value(ev["profit"])

        total_profit += profit_val
        total_capital += cost_val

        group = ev["group"]
        per_event[eid] = {
            "title": group.event_title,
            "set_size": S_val,
            "cost": cost_val,
            "profit": profit_val,
        }

        # Build orders per leg
        for i, market in enumerate(group.markets):
            leg_size = 0
            leg_cost = 0
            # Find the best price we're filling at
            best_price = 0
            for k, seg in enumerate(ev["segments"][i]):
                qty = pulp.value(ev["x"][i][k])
                if qty > 0.01:
                    maker_price = max(0.001, seg["price"] - 0.005)
                    leg_size += qty
                    leg_cost += maker_price * qty
                    best_price = maker_price

            if leg_size > 0.01:
                avg_price = leg_cost / leg_size
                token_id = market.outcomes[1].token_id if len(market.outcomes) > 1 else ""
                orders.append(GlobalOrder(
                    event_id=eid,
                    event_title=group.event_title,
                    market_condition_id=market.condition_id,
                    outcome_idx=1,  # NO token
                    side="buy",
                    size=leg_size,
                    price=avg_price,
                    total_cost=leg_cost,
                    neg_risk=True,
                ))

    gas = estimate_gas_cost(len(orders))
    total_fees = gas  # maker fees are already in the LP (with rebate)

    result = GlobalResult(
        status="optimal",
        total_profit=total_profit - gas,
        events_traded=events_traded,
        orders=orders,
        total_capital_used=total_capital,
        total_fees=total_fees,
        per_event=per_event,
    )

    log.info(
        "Global LP: %d/%d events, %d orders, capital $%.2f, profit $%.4f",
        events_traded, len(event_vars), len(orders), total_capital, total_profit,
    )
    return result
