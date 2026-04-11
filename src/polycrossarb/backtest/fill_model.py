"""Backtest fill models — synthetic L2 books with one-tick adverse fills.

Adapted from PredictionMarketTakerFillModel in
evan-kolberg/prediction-market-backtesting (NautilusTrader fork).

The realistic taker model is: when you cross the spread with a market
order on a prediction market, you pay one tick worse than the touch.
This is a deterministic one-tick adverse move that models real-world
slippage without needing full L3 (queue position) data.

Models:
    OneTickTakerFillModel  -- recommended for non-limit fills
    BookWalkTakerFillModel -- walks the actual L2 book if depth is known
    PerfectFillModel       -- no slippage, useful as a sanity baseline
"""
from __future__ import annotations

from dataclasses import dataclass


# Polymarket tick size (ETHEREUM CLOB) — most markets use 1 cent ticks,
# some BTC candle markets use 0.001. The actual price_increment is
# returned by the CLOB's /book endpoint per token.
DEFAULT_TICK = 0.01


@dataclass
class FillResult:
    """Result of simulating a fill against a book."""
    filled_size: float
    fill_price: float
    fill_cost: float           # signed: positive for buy, negative for sell
    slippage_per_share: float  # how much worse than touch
    success: bool
    reason: str = ""


def one_tick_adverse_price(
    side: str,
    best_bid: float,
    best_ask: float,
    tick_size: float = DEFAULT_TICK,
) -> float:
    """Price you actually get when crossing the spread with a market order.

    Buying: pay best_ask + 1 tick (you cross harder than the touch)
    Selling: receive best_bid - 1 tick

    Clamped to [tick_size, 1 - tick_size] so we never quote outside [0, 1].
    """
    if side == "buy":
        price = best_ask + tick_size
    else:
        price = best_bid - tick_size
    return max(tick_size, min(1.0 - tick_size, price))


class OneTickTakerFillModel:
    """Synthetic-book taker fill model.

    For market orders, returns the touch price moved one tick adverse.
    For limit orders, returns the limit price (no adverse move) since
    a limit order doesn't take liquidity if it sits in the book.

    This is the model used by NautilusTrader's prediction-market fork.
    It's a one-tick deterministic slippage — simple, conservative, and
    matches typical Polymarket retail fill quality.
    """

    def __init__(self, tick_size: float = DEFAULT_TICK):
        self.tick_size = tick_size

    def fill(
        self,
        side: str,
        size: float,
        best_bid: float,
        best_ask: float,
        order_type: str = "market",
        limit_price: float | None = None,
    ) -> FillResult:
        """Simulate a fill against the synthetic adverse book.

        Args:
            side: "buy" or "sell"
            size: number of shares
            best_bid: current best bid
            best_ask: current best ask
            order_type: "market" or "limit"
            limit_price: required for limit orders

        Returns:
            FillResult with success=True if the order would fill.
        """
        if size <= 0:
            return FillResult(0, 0, 0, 0, False, "size <= 0")
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            return FillResult(0, 0, 0, 0, False, "invalid book")

        if order_type == "limit":
            if limit_price is None:
                return FillResult(0, 0, 0, 0, False, "limit price required")

            # A passive limit order only fills if it crosses the spread.
            # If it doesn't cross, the backtest treats it as resting (we
            # don't model queue position here — see queue_position=True
            # in NautilusTrader if you need it).
            if side == "buy" and limit_price >= best_ask:
                fill_price = best_ask  # touch fill, no adverse move
            elif side == "sell" and limit_price <= best_bid:
                fill_price = best_bid
            else:
                return FillResult(0, 0, 0, 0, False, "limit not crossed")
            slippage = 0.0
        else:
            # Market order — one-tick adverse move
            fill_price = one_tick_adverse_price(side, best_bid, best_ask, self.tick_size)
            touch = best_ask if side == "buy" else best_bid
            slippage = abs(fill_price - touch)

        cost = fill_price * size * (-1 if side == "sell" else 1)
        return FillResult(
            filled_size=size,
            fill_price=fill_price,
            fill_cost=cost,
            slippage_per_share=slippage,
            success=True,
        )


class BookWalkTakerFillModel:
    """Fill model that walks the actual L2 book level by level.

    Use when you have real depth data (e.g. from PMXT replays).
    Falls back to one-tick adverse if depth is exhausted.
    """

    def __init__(self, tick_size: float = DEFAULT_TICK):
        self.tick_size = tick_size

    def fill(
        self,
        side: str,
        size: float,
        bids: list[tuple[float, float]],   # [(price, size), ...] sorted desc
        asks: list[tuple[float, float]],   # [(price, size), ...] sorted asc
    ) -> FillResult:
        """Walk the book to compute VWAP fill."""
        if size <= 0:
            return FillResult(0, 0, 0, 0, False, "size <= 0")

        levels = asks if side == "buy" else bids
        if not levels:
            return FillResult(0, 0, 0, 0, False, "empty side")

        remaining = size
        total_cost = 0.0
        for price, avail in levels:
            if remaining <= 0:
                break
            take = min(remaining, avail)
            total_cost += take * price
            remaining -= take

        if remaining > 0:
            # Not enough depth — fall through with one-tick adverse on remainder
            last = levels[-1][0]
            if side == "buy":
                synth = last + self.tick_size
            else:
                synth = max(self.tick_size, last - self.tick_size)
            total_cost += remaining * synth
            remaining = 0

        vwap = total_cost / size
        touch = levels[0][0]
        slippage = abs(vwap - touch)
        cost = vwap * size * (-1 if side == "sell" else 1)
        return FillResult(
            filled_size=size,
            fill_price=vwap,
            fill_cost=cost,
            slippage_per_share=slippage,
            success=True,
        )


class PerfectFillModel:
    """Fill at touch price, no slippage. Use as a sanity baseline only."""

    def fill(
        self,
        side: str,
        size: float,
        best_bid: float,
        best_ask: float,
        **_kwargs,
    ) -> FillResult:
        if size <= 0:
            return FillResult(0, 0, 0, 0, False, "size <= 0")
        price = best_ask if side == "buy" else best_bid
        cost = price * size * (-1 if side == "sell" else 1)
        return FillResult(
            filled_size=size,
            fill_price=price,
            fill_cost=cost,
            slippage_per_share=0.0,
            success=True,
        )
