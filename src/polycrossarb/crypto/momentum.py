"""BTC momentum detector for candle trading.

Tracks BTC price over short windows (1-15 minutes) to detect
directional momentum. When BTC has been consistently moving in
one direction, the candle outcome becomes predictable.

Key insight: BTC price momentum within a 5-15 minute window
is highly persistent. If BTC has gone up $50 in the first 10
minutes of a 15-minute candle, it's very likely to finish up.

Confidence model:
  - Direction consistency (% of ticks in the same direction)
  - Magnitude (how far price has moved relative to volatility)
  - Time remaining (less time = more locked in)
"""
from __future__ import annotations

import logging
import math
import time
from collections import deque
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class MomentumSignal:
    """Detected momentum signal for a candle window."""
    direction: str          # "up" or "down"
    confidence: float       # 0.0 to 1.0
    price_change: float     # absolute $ change from window open
    price_change_pct: float # % change
    consistency: float      # fraction of ticks in same direction
    minutes_elapsed: float  # how long into the window
    minutes_remaining: float
    current_price: float
    open_price: float       # price at start of window


class MomentumDetector:
    """Tracks BTC price momentum for candle direction prediction.

    Maintains a rolling window of price ticks and computes
    directional momentum signals.
    """

    def __init__(self):
        self._ticks: deque[tuple[float, float]] = deque(maxlen=5000)  # (timestamp, price)
        self._window_opens: dict[str, float] = {}  # contract_id -> open price

    def add_tick(self, price: float):
        """Record a new price tick."""
        self._ticks.append((time.time(), price))

    def set_window_open(self, contract_id: str, price: float):
        """Set the opening price for a candle window."""
        self._window_opens[contract_id] = price

    def get_open_price(self, contract_id: str) -> float | None:
        """Get the opening price for a candle window."""
        return self._window_opens.get(contract_id)

    def detect(
        self,
        contract_id: str,
        window_start_ago_minutes: float,
        minutes_remaining: float,
        current_price: float,
    ) -> MomentumSignal | None:
        """Detect momentum for a specific candle window.

        Args:
            contract_id: The candle contract identifier.
            window_start_ago_minutes: How many minutes ago the window started.
            minutes_remaining: Minutes until resolution.
            current_price: Current BTC price.
        """
        if not self._ticks or minutes_remaining <= 0:
            return None

        now = time.time()
        window_start = now - (window_start_ago_minutes * 60)

        # Get the open price (price at window start)
        open_price = self._window_opens.get(contract_id)
        if open_price is None:
            # Estimate from tick history
            for ts, price in self._ticks:
                if ts >= window_start:
                    open_price = price
                    self._window_opens[contract_id] = open_price
                    break

        if open_price is None or open_price <= 0:
            return None

        # Calculate price change
        price_change = current_price - open_price
        price_change_pct = price_change / open_price

        # Direction
        direction = "up" if price_change >= 0 else "down"

        # Consistency: what fraction of recent ticks agree with the direction?
        recent_ticks = [
            (ts, p) for ts, p in self._ticks
            if ts >= window_start
        ]

        if len(recent_ticks) < 3:
            return None

        consistent = 0
        for i in range(1, len(recent_ticks)):
            tick_dir = recent_ticks[i][1] - recent_ticks[i - 1][1]
            if (direction == "up" and tick_dir >= 0) or (direction == "down" and tick_dir <= 0):
                consistent += 1

        consistency = consistent / max(len(recent_ticks) - 1, 1)

        # Confidence model
        minutes_elapsed = window_start_ago_minutes

        # Base: how much of the window has passed?
        time_factor = min(1.0, minutes_elapsed / (minutes_elapsed + minutes_remaining))

        # Magnitude: how significant is the move relative to typical volatility?
        # Typical 5-min BTC move: ~$20-50. Anything >$100 is very significant.
        magnitude = min(1.0, abs(price_change) / 100.0)

        # Combine factors
        confidence = (
            0.40 * time_factor +       # more time elapsed = more locked in
            0.30 * consistency +        # more consistent = stronger trend
            0.30 * magnitude            # bigger move = more significant
        )

        # Clamp
        confidence = max(0.10, min(0.95, confidence))

        # Boost if near end of window with clear direction
        if minutes_remaining < 2 and abs(price_change) > 20:
            confidence = min(0.95, confidence + 0.15)

        # Reduce if move is tiny (could reverse)
        if abs(price_change) < 5:
            confidence *= 0.5

        return MomentumSignal(
            direction=direction,
            confidence=confidence,
            price_change=price_change,
            price_change_pct=price_change_pct,
            consistency=consistency,
            minutes_elapsed=minutes_elapsed,
            minutes_remaining=minutes_remaining,
            current_price=current_price,
            open_price=open_price,
        )
