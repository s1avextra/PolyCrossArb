"""L2 replay engine — uses real Polymarket order book history.

Replaces the synthetic-price model in replay_engine.py with actual
order book events from the PMXT archive. This eliminates the
self-referential `_simulate_market_price` function and gives the
backtest 1:1 fidelity with what the market actually showed.

Pipeline:
    1. Load L2 events for a time range from PMXTLoader
    2. For each token_id, maintain a live in-memory book (LocalBook)
    3. Run the trading strategy on each tick — same code as live
    4. When the strategy fires a signal, simulate the fill with
       OneTickTakerFillModel + StaticLatencyConfig
    5. At resolution, compute realized P&L from the actual outcome

The strategy code is the SAME object used in live mode. That means
backtest and live diverge only in (a) the fill model and (b) the
latency model. If those two match reality, paper P&L predicts live
P&L.

Note: this engine is a foundation. Specific strategy adapters
(candle, weather, arb) plug in via the `strategy_callback` argument.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterator

from polymomentum.backtest.fill_model import (
    FillResult,
    OneTickTakerFillModel,
)
from polymomentum.backtest.latency_model import StaticLatencyConfig
from polymomentum.backtest.pmxt_loader import (
    BookSnapshot,
    L2Event,
    PMXTLoader,
    PriceChange,
)
from polymomentum.execution.fees import polymarket_fee

log = logging.getLogger(__name__)


@dataclass
class TokenBook:
    """Mutable in-memory L2 book for a single token, replayed from PMXT events."""
    token_id: str
    bids: dict[float, float] = field(default_factory=dict)  # price -> size
    asks: dict[float, float] = field(default_factory=dict)
    best_bid: float = 0.0
    best_ask: float = 0.0
    last_update_ts: float = 0.0

    def apply_snapshot(self, snap: BookSnapshot) -> None:
        self.bids = {lvl.price: lvl.size for lvl in snap.bids if lvl.size > 0}
        self.asks = {lvl.price: lvl.size for lvl in snap.asks if lvl.size > 0}
        self.best_bid = snap.best_bid or (max(self.bids.keys()) if self.bids else 0.0)
        self.best_ask = snap.best_ask or (min(self.asks.keys()) if self.asks else 0.0)
        self.last_update_ts = snap.timestamp

    def apply_change(self, chg: PriceChange) -> None:
        side = chg.change_side or chg.side
        book = self.bids if side == "buy" else self.asks
        if chg.change_size <= 0:
            book.pop(chg.change_price, None)
        else:
            book[chg.change_price] = chg.change_size
        # Update best touch
        if self.bids:
            self.best_bid = max(self.bids.keys())
        if self.asks:
            self.best_ask = min(self.asks.keys())
        if chg.best_bid > 0:
            self.best_bid = chg.best_bid
        if chg.best_ask > 0:
            self.best_ask = chg.best_ask
        self.last_update_ts = chg.timestamp

    def mid(self) -> float:
        if self.best_bid > 0 and self.best_ask > 0:
            return (self.best_bid + self.best_ask) / 2
        return 0.0

    def ask_levels(self) -> list[tuple[float, float]]:
        return sorted(self.asks.items(), key=lambda kv: kv[0])

    def bid_levels(self) -> list[tuple[float, float]]:
        return sorted(self.bids.items(), key=lambda kv: -kv[0])


@dataclass
class BacktestOrder:
    """An order placed by a strategy during backtest."""
    timestamp: float       # when signal fired
    token_id: str
    side: str              # "buy" or "sell"
    size: float
    order_type: str        # "market" or "limit"
    limit_price: float | None = None
    fee_rate: float = 0.0  # taker fee_rate as decimal (e.g. 0.072 = 7.2%)
    maker_fee_rate: float = 0.0  # fee for maker fills (typically 0%)


@dataclass
class BacktestFill:
    """A simulated fill against historical L2 data."""
    order: BacktestOrder
    fill_timestamp: float  # order_timestamp + insert latency
    fill_price: float
    filled_size: float
    cost: float
    fee: float
    slippage: float
    book_age_ms: float     # how stale was the book when we filled
    success: bool
    reason: str = ""


# Strategy callback signature:
#   fn(timestamp_s, token_id, book, history) -> list[BacktestOrder]
# - timestamp_s: current event timestamp
# - token_id:    the token whose book just updated
# - book:        TokenBook (current state)
# - history:     dict[token_id, list[(ts, mid)]] — recent mids for context
#
# Return [] if no order, or list of BacktestOrder objects.
StrategyCallback = Callable[
    [float, str, "TokenBook", dict[str, list[tuple[float, float]]]],
    list[BacktestOrder],
]


class L2BacktestEngine:
    """Event-driven L2 backtest engine using real PMXT order book history.

    Designed to give 1:1 correspondence between backtest and live trading:
        - Same strategy code (callback signature)
        - Real historical bid/ask, not synthetic
        - Realistic fills via OneTickTakerFillModel
        - Latency-shifted execution via StaticLatencyConfig
        - NautilusTrader-correct fee formula
    """

    def __init__(
        self,
        loader: PMXTLoader | None = None,
        fill_model: OneTickTakerFillModel | None = None,
        latency: StaticLatencyConfig | None = None,
        history_window_seconds: float = 300.0,
    ):
        self.loader = loader or PMXTLoader()
        self.fill_model = fill_model or OneTickTakerFillModel()
        self.latency = latency or StaticLatencyConfig()
        self.history_window_seconds = history_window_seconds

        self._books: dict[str, TokenBook] = {}
        self._history: dict[str, list[tuple[float, float]]] = {}
        self._pending_orders: list[BacktestOrder] = []
        self.fills: list[BacktestFill] = []
        self.event_count = 0

    def _ensure_book(self, token_id: str) -> TokenBook:
        if token_id not in self._books:
            self._books[token_id] = TokenBook(token_id=token_id)
        return self._books[token_id]

    def _record_history(self, token_id: str, ts: float, mid: float) -> None:
        if mid <= 0:
            return
        hist = self._history.setdefault(token_id, [])
        hist.append((ts, mid))
        # Trim old history
        cutoff = ts - self.history_window_seconds
        while hist and hist[0][0] < cutoff:
            hist.pop(0)

    def _flush_pending_orders(self, current_ts: float) -> None:
        """Fill any pending orders whose latency window has elapsed at current_ts.

        Lookahead guard: this method MUST be called BEFORE the current event's
        book update is applied, so the fill uses the book state as it was at
        (order.timestamp + latency), not the post-event state. The caller in
        `replay()` enforces this ordering.

        The recorded fill_timestamp is clamped to `order.timestamp + latency`
        rather than `current_ts` so trade-level analysis sees the intended
        fill instant — not the arbitrary later event that happened to
        trigger the flush.
        """
        latency_s = self.latency.total_insert_ms() / 1000.0
        ready = [o for o in self._pending_orders if current_ts - o.timestamp >= latency_s]
        if not ready:
            return

        for order in ready:
            self._pending_orders.remove(order)
            fill_ts = order.timestamp + latency_s
            book = self._books.get(order.token_id)
            if book is None or book.best_bid <= 0 or book.best_ask <= 0:
                self.fills.append(BacktestFill(
                    order=order,
                    fill_timestamp=fill_ts,
                    fill_price=0,
                    filled_size=0,
                    cost=0,
                    fee=0,
                    slippage=0,
                    book_age_ms=0,
                    success=False,
                    reason="no book at fill time",
                ))
                continue

            result: FillResult = self.fill_model.fill(
                side=order.side,
                size=order.size,
                best_bid=book.best_bid,
                best_ask=book.best_ask,
                order_type=order.order_type,
                limit_price=order.limit_price,
            )

            if not result.success:
                self.fills.append(BacktestFill(
                    order=order,
                    fill_timestamp=fill_ts,
                    fill_price=0,
                    filled_size=0,
                    cost=0,
                    fee=0,
                    slippage=0,
                    book_age_ms=(fill_ts - book.last_update_ts) * 1000,
                    success=False,
                    reason=result.reason,
                ))
                continue

            # Use maker fee rate when the fill model indicates a maker fill
            effective_rate = order.maker_fee_rate if result.reason == "maker_fill" else order.fee_rate
            fee = polymarket_fee(result.filled_size, result.fill_price, effective_rate)
            self.fills.append(BacktestFill(
                order=order,
                fill_timestamp=fill_ts,
                fill_price=result.fill_price,
                filled_size=result.filled_size,
                cost=result.fill_cost,
                fee=fee,
                slippage=result.slippage_per_share,
                book_age_ms=(fill_ts - book.last_update_ts) * 1000,
                success=True,
            ))

    def replay(
        self,
        start: datetime,
        end: datetime,
        token_ids: set[str],
        strategy: StrategyCallback,
        fee_rate: float = 0.0,
    ) -> None:
        """Replay historical L2 data and run strategy callback on every event.

        Args:
            start: UTC datetime — first hour to replay
            end: UTC datetime — last hour (inclusive)
            token_ids: which tokens to track
            strategy: callable that returns BacktestOrder list per event
            fee_rate: default fee_rate for orders without one set
        """
        log.info("L2 replay: %s -> %s, %d tokens", start, end, len(token_ids))
        t0 = time.time()

        for event in self.loader.load_range(start, end, token_ids=token_ids):
            self.event_count += 1
            token_id = ""
            if event.snapshot:
                token_id = event.snapshot.token_id
            elif event.change:
                token_id = event.change.token_id
            else:
                continue

            # Flush pending orders BEFORE applying this event's update. This
            # ensures a fill ready at (order.ts + latency) uses the book state
            # from before `event` arrived — no lookahead from book changes
            # that happened after the intended fill instant.
            self._flush_pending_orders(event.timestamp)

            book = self._ensure_book(token_id)
            if event.snapshot:
                book.apply_snapshot(event.snapshot)
            elif event.change:
                book.apply_change(event.change)

            mid = book.mid()
            if mid > 0:
                self._record_history(token_id, event.timestamp, mid)

            # Call strategy with the (now updated) book
            orders = strategy(event.timestamp, token_id, book, self._history)
            for order in orders:
                if order.fee_rate == 0.0:
                    order.fee_rate = fee_rate
                self._pending_orders.append(order)

        elapsed = time.time() - t0
        log.info(
            "L2 replay complete: %d events, %d fills (%d ok / %d fail), %.1fs",
            self.event_count,
            len(self.fills),
            sum(1 for f in self.fills if f.success),
            sum(1 for f in self.fills if not f.success),
            elapsed,
        )

    def summary(self) -> dict:
        successful = [f for f in self.fills if f.success]
        total_cost = sum(abs(f.cost) for f in successful)
        total_fees = sum(f.fee for f in successful)
        avg_slippage = (
            sum(f.slippage for f in successful) / len(successful)
            if successful else 0.0
        )
        avg_book_age_ms = (
            sum(f.book_age_ms for f in successful) / len(successful)
            if successful else 0.0
        )
        return {
            "events_processed": self.event_count,
            "fills_total": len(self.fills),
            "fills_success": len(successful),
            "fills_failed": len(self.fills) - len(successful),
            "total_cost": round(total_cost, 4),
            "total_fees": round(total_fees, 5),
            "avg_slippage": round(avg_slippage, 5),
            "avg_book_age_ms": round(avg_book_age_ms, 1),
            "tokens_tracked": len(self._books),
        }
