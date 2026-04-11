"""BTC momentum detector for candle trading.

Tracks BTC price over short windows (1-60 minutes) to detect
directional momentum. When BTC has been consistently moving in
one direction, the candle outcome becomes predictable.

Confidence model (volatility-normalized):
  - Z-score: price move / (σ × √window) — vol-adjusted magnitude
  - Time factor: how locked-in the outcome is (time elapsed / total)
  - Consistency: fraction of ticks agreeing with direction
  - Reversion count: how many times price crossed through open (noise indicator)
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
    z_score: float = 0.0    # volatility-normalized magnitude
    reversion_count: int = 0  # times price crossed open


class MomentumDetector:
    """Tracks BTC price momentum for candle direction prediction.

    Uses volatility-normalized signals instead of fixed dollar thresholds.
    """

    def __init__(self, realized_vol: float | None = None):
        self._ticks: deque[tuple[float, float]] = deque(maxlen=5000)  # (timestamp, price)
        self._window_opens: dict[str, float] = {}  # contract_id -> open price
        self._realized_vol: float = realized_vol or 0.50  # annualized, updated externally

    @property
    def realized_vol(self) -> float:
        return self._realized_vol

    def set_realized_vol(self, vol: float):
        """Update realized volatility from price feed."""
        if vol > 0:
            self._realized_vol = vol

    def add_tick(self, price: float, timestamp: float | None = None):
        """Record a new price tick.

        Args:
            price: BTC spot
            timestamp: explicit unix seconds (None = wall clock, used in live mode).
                       Backtest replay must pass historical timestamps.
        """
        ts = timestamp if timestamp is not None else time.time()
        self._ticks.append((ts, price))

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
        now_ts: float | None = None,
    ) -> MomentumSignal | None:
        """Detect momentum for a specific candle window.

        Args:
            contract_id: The candle contract identifier.
            window_start_ago_minutes: How many minutes ago the window started.
            minutes_remaining: Minutes until resolution.
            current_price: Current BTC price.
            now_ts: Explicit "now" in unix seconds (None = wall clock).
                    Backtest replay must pass historical timestamps.
        """
        if not self._ticks or minutes_remaining <= 0:
            return None

        now = now_ts if now_ts is not None else time.time()
        window_start = now - (window_start_ago_minutes * 60)

        # Get the open price (price at window start)
        open_price = self._window_opens.get(contract_id)
        if open_price is None:
            for ts, price in self._ticks:
                if ts >= window_start:
                    open_price = price
                    self._window_opens[contract_id] = open_price
                    break

        if open_price is None or open_price <= 0:
            return None

        # Price change
        price_change = current_price - open_price
        price_change_pct = price_change / open_price

        # Direction
        direction = "up" if price_change >= 0 else "down"

        # Collect ticks in this window. Walk the deque from the newest
        # tick backwards and stop as soon as we cross window_start —
        # ticks are always appended in monotonic time order, so this is
        # O(k) where k is the number of ticks inside the window
        # (typically a few hundred for a 5-min candle with 2 Hz feed),
        # vs O(n) on the deque comprehension that used to scan all
        # 5000 entries on every call. Hot path: backtest replays call
        # detect() ~140k times per replay-hour.
        recent_ticks: list[tuple[float, float]] = []
        for ts, p in reversed(self._ticks):
            if ts < window_start:
                break
            recent_ticks.append((ts, p))
        if len(recent_ticks) < 3:
            return None
        recent_ticks.reverse()

        # Consistency + reversion count
        consistent = 0
        reversion_count = 0
        prev_side = None  # True = above open, False = below
        for i in range(1, len(recent_ticks)):
            tick_dir = recent_ticks[i][1] - recent_ticks[i - 1][1]
            if (direction == "up" and tick_dir >= 0) or (direction == "down" and tick_dir <= 0):
                consistent += 1
            # Count crossings through open price
            curr_side = recent_ticks[i][1] >= open_price
            if prev_side is not None and curr_side != prev_side:
                reversion_count += 1
            prev_side = curr_side

        consistency = consistent / max(len(recent_ticks) - 1, 1)

        minutes_elapsed = window_start_ago_minutes
        total_window = minutes_elapsed + minutes_remaining

        # ── Volatility-normalized magnitude (z-score) ──────────────
        # Expected move for this window based on realized vol
        # sigma_window = price × annual_vol × sqrt(window_minutes / minutes_per_year)
        sigma_window = open_price * self._realized_vol * math.sqrt(total_window / 525600)
        sigma_window = max(sigma_window, 1.0)  # floor at $1 to avoid div-by-zero
        z_score = abs(price_change) / sigma_window

        # ── Time factor ────────────────────────────────────────────
        time_factor = min(1.0, minutes_elapsed / total_window) if total_window > 0 else 0

        # ── Reversion penalty ──────────────────────────────────────
        # More crossings through open = noisier, less directional
        reversion_penalty = max(0.0, 1.0 - reversion_count * 0.05)

        # ── Confidence model (reweighted) ──────────────────────────
        # Z-score replaces the old fixed-$ magnitude threshold
        # Saturate z-score contribution at z=3.0
        z_factor = min(1.0, z_score / 3.0)

        confidence = (
            0.35 * time_factor +
            0.35 * z_factor +
            0.15 * consistency +
            0.15 * reversion_penalty
        )

        # Clamp
        confidence = max(0.10, min(0.95, confidence))

        # Boost near resolution with strong vol-adjusted move
        if minutes_remaining < 2.0 and z_score > 1.0:
            confidence = min(0.95, confidence + 0.15)
        elif minutes_remaining < 1.0 and z_score > 0.5:
            confidence = min(0.95, confidence + 0.20)

        # Reduce if move is sub-0.3 sigma (noise)
        if z_score < 0.3:
            confidence *= 0.4

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
            z_score=z_score,
            reversion_count=reversion_count,
        )
