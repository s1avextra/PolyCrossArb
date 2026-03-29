"""Arbitrage detection for single-market and cross-market opportunities.

Single-market arbitrage exists when:
  - OVERPRICED: sum of outcome prices > 1.0 → sell all outcomes, guaranteed profit
  - UNDERPRICED: sum of outcome prices < 1.0 → buy all outcomes, guaranteed profit

The margin is the absolute deviation from 1.0.  On Polymarket (LMSR/CLOB),
a 2-outcome market should have YES + NO = 1.0.  Multi-outcome markets should
sum to 1.0 across all mutually exclusive outcomes.
"""
from __future__ import annotations

import logging
from collections import defaultdict

from polycrossarb.config import settings
from polycrossarb.data.models import ArbOpportunity, Market

log = logging.getLogger(__name__)


def detect_single_market_arbs(
    markets: list[Market],
    min_margin: float | None = None,
) -> list[ArbOpportunity]:
    """Scan markets for single-market arbitrage (outcome prices != 1.0).

    For each market with 2+ outcomes:
      - If sum(prices) > 1.0: "overpriced" arb — sell all outcomes
      - If sum(prices) < 1.0: "underpriced" arb — buy all outcomes
    """
    if min_margin is None:
        min_margin = settings.min_arb_margin

    opportunities: list[ArbOpportunity] = []

    for market in markets:
        if market.closed or not market.active:
            continue
        if market.num_outcomes < 2:
            continue

        # Skip markets where all prices are 0 (no data)
        if all(o.price == 0 for o in market.outcomes):
            continue

        price_sum = market.outcome_price_sum
        deviation = price_sum - 1.0

        if abs(deviation) < min_margin:
            continue

        if deviation > 0:
            # Overpriced: sell all outcomes → collect price_sum, pay out 1.0
            arb_type = "single_over"
            profit_per_dollar = deviation / price_sum
            details = (
                f"SUM={price_sum:.4f} (>{1.0:.2f}): sell all outcomes. "
                f"Collect ${price_sum:.4f}, pay $1.00 on resolution. "
                f"Profit: ${deviation:.4f} per set."
            )
        else:
            # Underpriced: buy all outcomes → pay price_sum, receive 1.0
            arb_type = "single_under"
            profit_per_dollar = -deviation / price_sum
            details = (
                f"SUM={price_sum:.4f} (<{1.0:.2f}): buy all outcomes. "
                f"Pay ${price_sum:.4f}, receive $1.00 on resolution. "
                f"Profit: ${-deviation:.4f} per set."
            )

        opp = ArbOpportunity(
            arb_type=arb_type,
            markets=[market],
            margin=abs(deviation),
            profit_per_dollar=profit_per_dollar,
            details=details,
        )
        opportunities.append(opp)

    opportunities.sort(key=lambda o: o.margin, reverse=True)
    return opportunities


def detect_single_market_orderbook_arbs(
    markets: list[Market],
    min_margin: float | None = None,
) -> list[ArbOpportunity]:
    """Detect arbs using actual order book prices (best bid/ask) rather than
    mid-prices for more realistic profit estimation.

    For overpriced markets: use best bids (what we can sell at).
    For underpriced markets: use best asks (what we can buy at).
    """
    if min_margin is None:
        min_margin = settings.min_arb_margin

    opportunities: list[ArbOpportunity] = []

    for market in markets:
        if market.closed or not market.active:
            continue
        if market.num_outcomes < 2:
            continue

        # Check if order books are available
        if not all(o.order_book is not None for o in market.outcomes):
            continue

        # Check overpriced: can we sell all at bids summing > 1?
        bid_sum = 0.0
        all_bids = True
        for o in market.outcomes:
            bb = o.order_book.best_bid  # type: ignore[union-attr]
            if bb is None:
                all_bids = False
                break
            bid_sum += bb

        if all_bids and bid_sum - 1.0 > min_margin:
            deviation = bid_sum - 1.0
            opp = ArbOpportunity(
                arb_type="single_over_book",
                markets=[market],
                margin=deviation,
                profit_per_dollar=deviation / bid_sum,
                details=(
                    f"BOOK BID SUM={bid_sum:.4f}: sell all at bids. "
                    f"Profit ${deviation:.4f}/set (executable)."
                ),
            )
            opportunities.append(opp)

        # Check underpriced: can we buy all at asks summing < 1?
        ask_sum = 0.0
        all_asks = True
        for o in market.outcomes:
            ba = o.order_book.best_ask  # type: ignore[union-attr]
            if ba is None:
                all_asks = False
                break
            ask_sum += ba

        if all_asks and 1.0 - ask_sum > min_margin:
            deviation = 1.0 - ask_sum
            opp = ArbOpportunity(
                arb_type="single_under_book",
                markets=[market],
                margin=deviation,
                profit_per_dollar=deviation / ask_sum,
                details=(
                    f"BOOK ASK SUM={ask_sum:.4f}: buy all at asks. "
                    f"Profit ${deviation:.4f}/set (executable)."
                ),
            )
            opportunities.append(opp)

    opportunities.sort(key=lambda o: o.margin, reverse=True)
    return opportunities


def group_markets_by_event(markets: list[Market]) -> dict[str, list[Market]]:
    """Group markets by their event_id (shared event on Polymarket).

    On Polymarket, multi-outcome events (e.g. "Who wins the election?") are
    split into separate binary YES/NO markets, all sharing the same event.
    The YES prices across these markets should sum to 1.0 if outcomes are
    mutually exclusive and exhaustive.
    """
    groups: dict[str, list[Market]] = defaultdict(list)
    for m in markets:
        if m.event_id and m.active and not m.closed:
            groups[m.event_id].append(m)
    return {k: v for k, v in groups.items() if len(v) > 1}


def detect_cross_market_arbs(
    markets: list[Market],
    min_margin: float | None = None,
    exclusive_only: bool = True,
) -> list[ArbOpportunity]:
    """Detect cross-market arbitrage across event-grouped markets.

    For mutually exclusive events split into binary markets:
      - Sum of YES prices across all markets in the event should = 1.0
      - If sum > 1.0: overpriced — sell YES on all, guaranteed profit
      - If sum < 1.0: underpriced — buy YES on all, guaranteed profit

    Args:
        exclusive_only: If True (default), only consider neg_risk events
            which are confirmed mutually exclusive by Polymarket. Non-exclusive
            events (e.g. "which states will X visit") are NOT arbitrageable
            because multiple outcomes can be true simultaneously.
    """
    if min_margin is None:
        min_margin = settings.min_arb_margin

    event_groups = group_markets_by_event(markets)

    # Filter to mutually exclusive groups if requested
    if exclusive_only:
        event_groups = {
            k: v for k, v in event_groups.items()
            if any(m.neg_risk for m in v)
        }
    opportunities: list[ArbOpportunity] = []

    for event_id, group in event_groups.items():
        # FILTER: max legs — arbs with 10+ legs almost never execute atomically
        if len(group) > settings.max_legs_per_trade:
            continue

        yes_prices: list[tuple[Market, float]] = []
        skip = False
        for m in group:
            if not m.outcomes or m.outcomes[0].price == 0:
                skip = True
                break

            price = m.outcomes[0].price

            # FILTER: skip legs with probability below threshold (illiquid)
            if price < settings.min_leg_probability:
                skip = True
                break

            yes_prices.append((m, price))

        if skip or len(yes_prices) < 2:
            continue

        price_sum = sum(p for _, p in yes_prices)
        deviation = price_sum - 1.0

        if abs(deviation) < min_margin:
            continue

        event_title = group[0].event_title or group[0].event_slug or event_id
        neg_risk = any(m.neg_risk for m in group)

        if deviation > 0:
            arb_type = "cross_over"
            profit_per_dollar = deviation / price_sum
            details = (
                f"EVENT: {event_title}\n"
                f"      YES SUM={price_sum:.4f} across {len(group)} markets (>{1.0:.2f}): "
                f"sell YES on all.\n"
                f"      Profit: ${deviation:.4f}/set. "
                f"{'[neg_risk=confirmed exclusive]' if neg_risk else '[check exclusivity]'}"
            )
        else:
            arb_type = "cross_under"
            profit_per_dollar = -deviation / price_sum
            details = (
                f"EVENT: {event_title}\n"
                f"      YES SUM={price_sum:.4f} across {len(group)} markets (<{1.0:.2f}): "
                f"buy YES on all.\n"
                f"      Profit: ${-deviation:.4f}/set. "
                f"{'[neg_risk=confirmed exclusive]' if neg_risk else '[check exclusivity]'}"
            )

        opp = ArbOpportunity(
            arb_type=arb_type,
            markets=group,
            margin=abs(deviation),
            profit_per_dollar=profit_per_dollar,
            details=details,
        )
        opportunities.append(opp)

    opportunities.sort(key=lambda o: o.margin, reverse=True)
    return opportunities
