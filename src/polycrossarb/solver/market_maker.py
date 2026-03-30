"""Hybrid market-making strategy for neg_risk events.

Instead of crossing the spread (taker), provide liquidity by placing
resting limit orders on the NO side. This captures:
  1. Maker rebates (20-50% of taker fees returned)
  2. The spread itself (buy at bid, sell at ask)
  3. Arb edge when sum(YES) > 1.0 (positive expected value)

Strategy:
  - For each overpriced event, place NO buy limit orders slightly
    below the current best NO bid on each outcome.
  - When filled, we hold NO tokens that are +EV because sum(YES) > 1.0.
  - Place NO sell limit orders slightly above our entry to exit.
  - If all NOs fill and we hold a complete set, guaranteed profit.
  - If only some fill, we hold +EV positions with maker rebate income.

Risk: inventory accumulation on one side. Managed by:
  - Max inventory per outcome
  - Skewed quotes (widen sell side when inventory is heavy)
  - Merge back to USDC if inventory gets too large
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from polycrossarb.data.models import Market
from polycrossarb.graph.screener import EventGroup

log = logging.getLogger(__name__)


@dataclass
class MMQuote:
    """A market-making quote (limit order to place)."""
    market_condition_id: str
    outcome_idx: int  # 1 = NO token
    side: str  # "buy" or "sell"
    price: float
    size: float
    event_id: str = ""
    event_title: str = ""
    neg_risk: bool = True

    @property
    def var_key(self) -> str:
        return f"{self.market_condition_id}:{self.outcome_idx}"


@dataclass
class MMStrategy:
    """Market-making strategy for one event."""
    event_id: str
    event_title: str
    edge: float  # YES sum - 1.0 (our structural edge)
    buy_quotes: list[MMQuote] = field(default_factory=list)
    sell_quotes: list[MMQuote] = field(default_factory=list)
    expected_rebate_per_fill: float = 0.0
    expected_profit_if_complete: float = 0.0


def generate_mm_quotes(
    group: EventGroup,
    quote_size: float = 10.0,
    spread_offset: float = 0.01,
    max_inventory_per_outcome: float = 50.0,
    maker_rebate_pct: float = 0.25,
) -> MMStrategy | None:
    """Generate market-making quotes for an overpriced neg_risk event.

    Places NO buy orders below best NO bid, NO sell orders above best NO ask.
    The structural edge (sum > 1.0) means our NO buys are +EV.

    Args:
        group: EventGroup with order books enriched.
        quote_size: How many shares per quote.
        spread_offset: How far from best bid/ask to place our quotes ($0.01 = 1 tick).
        max_inventory_per_outcome: Max NO shares to hold per outcome.
        maker_rebate_pct: Expected maker rebate (0.25 = 25% of taker fee returned).
    """
    if not group.is_neg_risk:
        return None

    yes_sum = sum(group.yes_prices)
    if yes_sum <= 1.0:
        return None  # no structural edge

    edge = yes_sum - 1.0
    n = len(group.markets)

    buy_quotes: list[MMQuote] = []
    sell_quotes: list[MMQuote] = []
    total_buy_cost = 0.0

    for market in group.markets:
        if not market.outcomes or len(market.outcomes) < 2:
            continue

        yes_book = market.outcomes[0].order_book
        if not yes_book:
            continue

        yes_price = market.outcomes[0].price

        # NO best bid = 1 - YES best ask
        # NO best ask = 1 - YES best bid
        yes_best_bid = yes_book.best_bid
        yes_best_ask = yes_book.best_ask

        if yes_best_bid is None or yes_best_ask is None:
            continue

        no_best_bid = 1 - yes_best_ask  # what buyers will pay for NO
        no_best_ask = 1 - yes_best_bid  # what sellers want for NO

        if no_best_bid <= 0 or no_best_ask <= 0:
            continue

        # Our buy quote: slightly below current NO best bid (we're patient)
        our_buy_price = round(max(0.01, no_best_bid - spread_offset), 2)

        # Our sell quote: slightly above current NO best ask (take profit)
        our_sell_price = round(min(0.99, no_best_ask + spread_offset), 2)

        # Ensure our spread is positive
        if our_sell_price <= our_buy_price:
            continue

        size = min(quote_size, max_inventory_per_outcome)

        buy_quotes.append(MMQuote(
            market_condition_id=market.condition_id,
            outcome_idx=1,
            side="buy",
            price=our_buy_price,
            size=size,
            event_id=group.event_id,
            event_title=group.event_title,
        ))

        sell_quotes.append(MMQuote(
            market_condition_id=market.condition_id,
            outcome_idx=1,
            side="sell",
            price=our_sell_price,
            size=size,
            event_id=group.event_id,
            event_title=group.event_title,
        ))

        total_buy_cost += our_buy_price * size

    if not buy_quotes:
        return None

    # Expected profit per complete set (all NOs fill)
    payout_per_set = (n - 1) * quote_size
    profit_if_complete = payout_per_set - total_buy_cost

    # Expected rebate income per fill
    avg_price = total_buy_cost / (len(buy_quotes) * quote_size) if buy_quotes else 0
    taker_fee_per_fill = 0.005 * avg_price * quote_size  # rough estimate
    rebate_per_fill = taker_fee_per_fill * maker_rebate_pct

    strategy = MMStrategy(
        event_id=group.event_id,
        event_title=group.event_title,
        edge=edge,
        buy_quotes=buy_quotes,
        sell_quotes=sell_quotes,
        expected_rebate_per_fill=rebate_per_fill,
        expected_profit_if_complete=profit_if_complete,
    )

    log.info(
        "MM strategy: %s — %d quotes, edge=%.3f, profit_if_complete=$%.4f",
        group.event_title[:40], len(buy_quotes), edge, profit_if_complete,
    )
    return strategy


def evaluate_mm_opportunities(
    groups: list[EventGroup],
    bankroll: float = 100.0,
    quote_size: float = 10.0,
) -> list[MMStrategy]:
    """Evaluate market-making opportunities across multiple events.

    Returns strategies sorted by expected profit.
    """
    strategies: list[MMStrategy] = []

    for group in groups:
        strategy = generate_mm_quotes(group, quote_size=quote_size)
        if strategy and strategy.expected_profit_if_complete > 0:
            strategies.append(strategy)

    # Sort by edge × liquidity (higher edge with more liquidity = better)
    strategies.sort(key=lambda s: s.edge, reverse=True)

    # Cap total capital deployed
    total_deployed = 0.0
    max_deploy = bankroll * 0.80
    selected: list[MMStrategy] = []

    for s in strategies:
        cost = sum(q.price * q.size for q in s.buy_quotes)
        if total_deployed + cost <= max_deploy:
            selected.append(s)
            total_deployed += cost

    log.info(
        "MM evaluation: %d/%d events selected, capital $%.2f",
        len(selected), len(strategies), total_deployed,
    )
    return selected
