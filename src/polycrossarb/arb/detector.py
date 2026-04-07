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

        # Determine execution strategy based on on-chain capability
        if settings.enable_onchain_execution:
            if arb_type == "single_over":
                exec_strategy = "split_then_sell"
            else:
                exec_strategy = "buy_then_merge"
        else:
            exec_strategy = "clob_only"

        opp = ArbOpportunity(
            arb_type=arb_type,
            markets=[market],
            margin=abs(deviation),
            profit_per_dollar=profit_per_dollar,
            details=details,
            execution_strategy=exec_strategy,
        )
        opportunities.append(opp)

    opportunities.sort(key=lambda o: o.margin, reverse=True)
    return opportunities


def detect_single_market_orderbook_arbs(
    markets: list[Market],
    min_margin: float | None = None,
) -> list[ArbOpportunity]:
    """Detect arbs using actual order book prices (best bid/ask).

    This is the vidarx pattern: find binary markets where
    ask_YES + ask_NO < 1.0, buy both, merge on-chain for $1.00.

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
        # With split: mint tokens at $1.00, sell at bids
        bid_sum = 0.0
        all_bids = True
        min_bid_depth = float("inf")
        for o in market.outcomes:
            bb = o.order_book.best_bid
            if bb is None:
                all_bids = False
                break
            bid_sum += bb
            depth = sum(l.size * l.price for l in o.order_book.bids)
            min_bid_depth = min(min_bid_depth, depth)

        if all_bids and bid_sum - 1.0 > min_margin and min_bid_depth >= 5.0:
            deviation = bid_sum - 1.0
            exec_strategy = "split_then_sell" if settings.enable_onchain_execution else "clob_only"
            opp = ArbOpportunity(
                arb_type="single_over_book",
                markets=[market],
                margin=deviation,
                profit_per_dollar=deviation / bid_sum,
                details=(
                    f"SPLIT+SELL: bids={bid_sum:.4f} > $1.00. "
                    f"Mint at $1.00, sell at bids. "
                    f"Profit ${deviation:.4f}/set. Depth ${min_bid_depth:.0f}"
                ),
                execution_strategy=exec_strategy,
            )
            opportunities.append(opp)

        # Check underpriced: can we buy all at asks summing < 1?
        # With merge: buy at asks, merge on-chain for $1.00
        ask_sum = 0.0
        all_asks = True
        min_ask_depth = float("inf")
        for o in market.outcomes:
            ba = o.order_book.best_ask
            if ba is None:
                all_asks = False
                break
            ask_sum += ba
            depth = sum(l.size * l.price for l in o.order_book.asks)
            min_ask_depth = min(min_ask_depth, depth)

        if all_asks and 1.0 - ask_sum > min_margin and min_ask_depth >= 5.0:
            deviation = 1.0 - ask_sum
            exec_strategy = "buy_then_merge" if settings.enable_onchain_execution else "clob_only"
            opp = ArbOpportunity(
                arb_type="single_under_book",
                markets=[market],
                margin=deviation,
                profit_per_dollar=deviation / ask_sum,
                details=(
                    f"BUY+MERGE: asks={ask_sum:.4f} < $1.00. "
                    f"Buy all at asks, merge for $1.00. "
                    f"Profit ${deviation:.4f}/set. Depth ${min_ask_depth:.0f}"
                ),
                execution_strategy=exec_strategy,
            )
            opportunities.append(opp)

    # Prioritize: binary first, buy_then_merge second, then margin
    opportunities.sort(
        key=lambda o: (len(o.markets) == 1, o.execution_strategy == "buy_then_merge", o.margin),
        reverse=True,
    )
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
        # FILTER: skip augmented neg_risk events (outcomes may be incomplete)
        # Augmented events have placeholders and "Other" that can change definitions.
        # Trading them assumes all outcomes are listed, which isn't guaranteed.
        if any(m.neg_risk_augmented for m in group):
            continue

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

            # FILTER: skip legs with no real trading activity
            if m.volume < settings.min_leg_volume_usd:
                skip = True
                break

            yes_prices.append((m, price))

        if skip or len(yes_prices) < 2:
            continue

        price_sum = sum(p for _, p in yes_prices)
        deviation = price_sum - 1.0

        if abs(deviation) < min_margin:
            continue

        # Only trade overpriced if configured — UNLESS on-chain merge is enabled,
        # which makes underpriced arbs safe (merge redeems to USDC.e, no exit problem)
        if settings.only_overpriced and not settings.enable_onchain_execution and deviation < 0:
            continue

        # Tick size check: margin must survive rounding.
        # With $0.01 ticks and N legs, the tick cost is N × $0.01.
        # The arb margin must be larger than this or it vanishes in rounding.
        n_legs = len(yes_prices)
        tick_cost = n_legs * 0.01
        if abs(deviation) < tick_cost:
            continue

        # CRITICAL: verify this is a COMPLETE outcome set, not a partial group.
        # An underpriced group (sum < 1.0) could mean:
        #   a) All outcomes present, slightly underpriced → real arb
        #   b) Missing outcomes → NOT an arb, just incomplete data
        #
        # For underpriced: only accept if sum > 0.85 (at most 15% "missing").
        # Below that, outcomes are clearly missing.
        # For overpriced: sum > 1.0 is always a valid arb if all outcomes are present.
        # But if sum >> 1.0 (like 1.6), some outcomes may overlap — also suspicious.
        if deviation < 0 and price_sum < 0.85:
            continue  # too much missing — incomplete group, not an arb

        if deviation > 0 and price_sum > 1.5:
            continue  # sum way too high — likely overlapping/duplicate outcomes

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

        # Determine execution strategy
        if settings.enable_onchain_execution:
            if arb_type == "cross_over":
                exec_strategy = "split_then_sell"
            else:
                exec_strategy = "buy_then_merge"
        else:
            exec_strategy = "clob_only"

        opp = ArbOpportunity(
            arb_type=arb_type,
            markets=group,
            margin=abs(deviation),
            profit_per_dollar=profit_per_dollar,
            details=details,
            execution_strategy=exec_strategy,
        )
        opportunities.append(opp)

    # Prioritize: binary (2-leg) first, buy_then_merge second, then margin
    opportunities.sort(
        key=lambda o: (len(o.markets) == 2, o.execution_strategy == "buy_then_merge", o.margin),
        reverse=True,
    )
    return opportunities
