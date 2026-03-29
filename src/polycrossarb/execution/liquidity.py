"""Order book liquidity analysis: VWAP, slippage, depth estimation."""
from __future__ import annotations

import logging
from dataclasses import dataclass

from polycrossarb.data.models import Market, OrderBook

log = logging.getLogger(__name__)


@dataclass
class LiquidityProfile:
    """Liquidity analysis for a single outcome."""
    market_condition_id: str
    outcome_idx: int
    best_bid: float | None
    best_ask: float | None
    bid_depth_usd: float  # total USD available on bid side
    ask_depth_usd: float  # total USD available on ask side
    spread: float | None  # ask - bid
    spread_pct: float | None

    @property
    def is_liquid(self) -> bool:
        return (self.bid_depth_usd > 10 or self.ask_depth_usd > 10) and self.spread is not None


@dataclass
class SlippageEstimate:
    """Estimated execution quality for a given order."""
    side: str  # "buy" or "sell"
    size: float  # requested size in shares
    vwap: float | None  # volume-weighted average price
    mid_price: float | None
    slippage: float  # vwap - mid (buy) or mid - vwap (sell)
    slippage_pct: float
    fill_pct: float  # what % of the order can be filled from the book
    executable: bool


def analyze_liquidity(market: Market) -> list[LiquidityProfile]:
    """Analyze liquidity for all outcomes in a market."""
    profiles: list[LiquidityProfile] = []

    for i, outcome in enumerate(market.outcomes):
        book = outcome.order_book
        if book is None:
            profiles.append(LiquidityProfile(
                market_condition_id=market.condition_id,
                outcome_idx=i,
                best_bid=None, best_ask=None,
                bid_depth_usd=0, ask_depth_usd=0,
                spread=None, spread_pct=None,
            ))
            continue

        bb = book.best_bid
        ba = book.best_ask
        spread = (ba - bb) if (bb is not None and ba is not None) else None
        mid = book.mid_price

        bid_depth = sum(lvl.price * lvl.size for lvl in book.bids)
        ask_depth = sum(lvl.price * lvl.size for lvl in book.asks)

        profiles.append(LiquidityProfile(
            market_condition_id=market.condition_id,
            outcome_idx=i,
            best_bid=bb,
            best_ask=ba,
            bid_depth_usd=bid_depth,
            ask_depth_usd=ask_depth,
            spread=spread,
            spread_pct=spread / mid if spread is not None and mid else None,
        ))

    return profiles


def estimate_slippage(
    book: OrderBook,
    side: str,
    size: float,
) -> SlippageEstimate:
    """Estimate slippage for a given order size against the order book."""
    mid = book.mid_price

    vwap = book.vwap(side, size)

    if vwap is None:
        # Insufficient liquidity
        levels = book.asks if side == "ask" else book.bids
        available = sum(lvl.size for lvl in levels)
        return SlippageEstimate(
            side=side, size=size, vwap=None, mid_price=mid,
            slippage=float("inf"), slippage_pct=float("inf"),
            fill_pct=available / size if size > 0 else 0,
            executable=False,
        )

    if mid is None:
        mid = vwap

    if side == "ask":  # buying
        slippage = vwap - mid
    else:  # selling
        slippage = mid - vwap

    slippage_pct = slippage / mid if mid > 0 else 0

    return SlippageEstimate(
        side=side, size=size, vwap=vwap, mid_price=mid,
        slippage=slippage, slippage_pct=slippage_pct,
        fill_pct=1.0, executable=True,
    )


def compute_max_executable_size(
    markets: list[Market],
    side: str,
    max_slippage_pct: float = 0.02,
) -> float:
    """Compute the maximum size that can be executed across all markets in a
    partition with acceptable slippage.

    For a partition arb, we need to trade equal size on all outcomes.
    The bottleneck is the least liquid outcome.
    """
    min_size = float("inf")

    for market in markets:
        for outcome in market.outcomes:
            if outcome.order_book is None:
                return 0.0

            book = outcome.order_book
            book_side = "ask" if side == "buy" else "bid"
            levels = book.asks if book_side == "ask" else book.bids

            if not levels:
                return 0.0

            # Binary search for max size within slippage budget
            total_available = sum(lvl.size for lvl in levels)
            if total_available == 0:
                return 0.0

            # Check slippage at 50% of depth
            test_size = total_available * 0.5
            est = estimate_slippage(book, book_side, test_size)
            if est.executable and est.slippage_pct <= max_slippage_pct:
                min_size = min(min_size, test_size)
            else:
                # Try smaller sizes
                for frac in [0.25, 0.1, 0.05]:
                    test_size = total_available * frac
                    est = estimate_slippage(book, book_side, test_size)
                    if est.executable and est.slippage_pct <= max_slippage_pct:
                        min_size = min(min_size, test_size)
                        break
                else:
                    return 0.0

    return min_size if min_size < float("inf") else 0.0
