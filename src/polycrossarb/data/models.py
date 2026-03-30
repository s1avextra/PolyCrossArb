from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class OrderBookLevel:
    price: float
    size: float


@dataclass(frozen=True)
class OrderBook:
    bids: list[OrderBookLevel] = field(default_factory=list)
    asks: list[OrderBookLevel] = field(default_factory=list)

    @property
    def best_bid(self) -> float | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> float | None:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2
        return self.best_bid or self.best_ask

    def depth_at_price(self, side: str, max_price: float | None = None) -> float:
        """Total size available up to max_price on a given side."""
        levels = self.asks if side == "ask" else self.bids
        total = 0.0
        for lvl in levels:
            if side == "ask" and max_price is not None and lvl.price > max_price:
                break
            if side == "bid" and max_price is not None and lvl.price < max_price:
                break
            total += lvl.size
        return total

    def vwap(self, side: str, size: float) -> float | None:
        """Volume-weighted average price to fill `size` on `side`."""
        levels = self.asks if side == "ask" else self.bids
        remaining = size
        cost = 0.0
        for lvl in levels:
            fill = min(remaining, lvl.size)
            cost += fill * lvl.price
            remaining -= fill
            if remaining <= 0:
                break
        if remaining > 0:
            return None  # insufficient liquidity
        return cost / size


@dataclass
class Outcome:
    token_id: str
    name: str
    price: float
    order_book: OrderBook | None = None


@dataclass
class Market:
    condition_id: str
    question: str
    slug: str
    outcomes: list[Outcome] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    category: str = ""
    active: bool = True
    closed: bool = False
    volume: float = 0.0
    liquidity: float = 0.0
    end_date: str = ""
    event_slug: str = ""
    event_id: str = ""
    event_title: str = ""
    group_slug: str = ""
    neg_risk: bool = False
    neg_risk_augmented: bool = False  # True = outcomes may be incomplete (has placeholders)

    @property
    def outcome_price_sum(self) -> float:
        return sum(o.price for o in self.outcomes)

    @property
    def num_outcomes(self) -> int:
        return len(self.outcomes)


@dataclass
class ArbOpportunity:
    """A detected arbitrage opportunity."""
    arb_type: str  # "single_over", "single_under", "cross_market"
    markets: list[Market]
    margin: float  # profit margin as fraction (e.g. 0.03 = 3%)
    profit_per_dollar: float
    details: str = ""

    @property
    def market_ids(self) -> list[str]:
        return [m.condition_id for m in self.markets]
