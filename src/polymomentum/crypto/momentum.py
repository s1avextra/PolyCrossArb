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

import enum
import logging
import math
import time
from collections import deque
from dataclasses import dataclass

log = logging.getLogger(__name__)


class VolatilityRegime(enum.Enum):
    """Volatility regime classification.

    Compares trailing short-window realized vol to a longer baseline.
    During HIGH/EXTREME regimes, MM lag widens and edge per trade
    increases — scale position size accordingly.

    Thresholds (ratio of short_vol / baseline_vol):
      LOW      < 0.5   — unusually quiet, tighter spreads
      NORMAL   0.5–1.5 — typical conditions
      HIGH     1.5–2.5 — elevated (news/FOMC), wider MM lag
      EXTREME  > 2.5   — liquidation cascades / black swan
    """
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    EXTREME = "extreme"


def classify_vol_regime(
    short_vol: float,
    baseline_vol: float,
) -> VolatilityRegime:
    """Classify current volatility regime.

    Args:
        short_vol: Short-window annualized realized vol (e.g., 15-min).
        baseline_vol: Longer baseline annualized vol (e.g., 24h).

    Returns:
        The volatility regime classification.
    """
    if baseline_vol <= 0 or short_vol <= 0:
        return VolatilityRegime.NORMAL

    ratio = short_vol / baseline_vol
    if ratio > 2.5:
        return VolatilityRegime.EXTREME
    if ratio > 1.5:
        return VolatilityRegime.HIGH
    if ratio < 0.5:
        return VolatilityRegime.LOW
    return VolatilityRegime.NORMAL


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

    def __init__(
        self,
        realized_vol: float | None = None,
        noise_z_threshold: float = 0.3,
        fast_vol_half_life_min: float = 15.0,
        slow_vol_half_life_min: float = 240.0,
        floor_vol: float = 0.10,
    ):
        self._ticks: deque[tuple[float, float]] = deque(maxlen=5000)  # (timestamp, price)
        self._window_opens: dict[str, float] = {}  # contract_id -> open price
        self._noise_z: float = noise_z_threshold  # below this z, confidence *= 0.4

        # Backwards-compat seed: if caller passes realized_vol explicitly,
        # use it until the EWMA estimator warms up.
        self._seed_vol: float = realized_vol if realized_vol and realized_vol > 0 else 0.50

        # EWMA fast/slow realized vol estimators.
        # Fast (~15 min half-life): primary sigma input for z-score.
        # Slow (~4h half-life):      baseline for regime ratio.
        # Floor prevents degenerate z explosions when vol collapses.
        self._fast_tau_s = fast_vol_half_life_min * 60.0 / math.log(2)
        self._slow_tau_s = slow_vol_half_life_min * 60.0 / math.log(2)
        self._floor_vol: float = floor_vol
        self._fast_var: float = 0.0      # EWMA of per-second variance
        self._slow_var: float = 0.0
        self._ewma_warmed: bool = False

    @property
    def realized_vol(self) -> float:
        """Current sigma source — EWMA fast when warm, seed otherwise.

        This is the value consumed by ``detect()`` for z-score computation.
        It replaces the old externally-set 24h rolling estimate.
        """
        if not self._ewma_warmed:
            return self._seed_vol
        seconds_per_year = 365.25 * 86400
        v = math.sqrt(max(0.0, self._fast_var * seconds_per_year))
        return max(self._floor_vol, min(5.0, v))

    @property
    def slow_realized_vol(self) -> float:
        """Slow EWMA vol (4h half-life) — baseline for regime ratio."""
        if not self._ewma_warmed:
            return self._seed_vol
        seconds_per_year = 365.25 * 86400
        v = math.sqrt(max(0.0, self._slow_var * seconds_per_year))
        return max(self._floor_vol, min(5.0, v))

    @property
    def vol_ratio(self) -> float:
        """fast/slow — regime indicator. >1.5 HIGH, <0.5 LOW."""
        slow = self.slow_realized_vol
        if slow <= 0:
            return 1.0
        return self.realized_vol / slow

    def set_realized_vol(self, vol: float):
        """Legacy setter — updates the seed used before EWMA warms up.

        After EWMA has seen enough ticks (~20), ``realized_vol`` is
        computed internally from the tick stream and this setter has
        no effect. Kept for backwards compatibility with the live
        pipeline's ``set_realized_vol(price_feed.volatility)`` call.
        """
        if vol > 0 and not self._ewma_warmed:
            self._seed_vol = vol

    def add_tick(self, price: float, timestamp: float | None = None):
        """Record a new price tick and update EWMA vol estimators.

        Args:
            price: BTC spot
            timestamp: explicit unix seconds (None = wall clock, used in live mode).
                       Backtest replay must pass historical timestamps.
        """
        ts = timestamp if timestamp is not None else time.time()

        # Update EWMA of squared log returns before appending the new tick.
        # Hot path — keep branches minimal.
        if self._ticks:
            last_ts, last_price = self._ticks[-1]
            dt = ts - last_ts
            if dt > 0 and last_price > 0 and price > 0:
                log_return = math.log(price / last_price)
                r2_rate = (log_return * log_return) / dt  # per-second squared return
                if self._ewma_warmed:
                    fast_alpha = 1.0 - math.exp(-dt / self._fast_tau_s)
                    slow_alpha = 1.0 - math.exp(-dt / self._slow_tau_s)
                    self._fast_var = (1 - fast_alpha) * self._fast_var + fast_alpha * r2_rate
                    self._slow_var = (1 - slow_alpha) * self._slow_var + slow_alpha * r2_rate
                else:
                    self._fast_var = r2_rate
                    self._slow_var = r2_rate
                    self._ewma_warmed = True

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
        reference_signal: MomentumSignal | None = None,
    ) -> MomentumSignal | None:
        """Detect momentum for a specific candle window.

        Args:
            contract_id: The candle contract identifier.
            window_start_ago_minutes: How many minutes ago the window started.
            minutes_remaining: Minutes until resolution.
            current_price: Current BTC price.
            now_ts: Explicit "now" in unix seconds (None = wall clock).
                    Backtest replay must pass historical timestamps.
            reference_signal: Optional momentum signal from a leading reference
                asset (e.g. BTC when evaluating ETH/SOL). When both the
                local and reference signals agree on direction and the
                reference has high confidence, the local signal receives a
                confidence boost.
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
        # Expected move for this window based on EWMA fast realized vol
        # (self-calibrating — adapts to current regime in ~15 min).
        # sigma_window = price × annual_vol × sqrt(window_minutes / minutes_per_year)
        current_vol = self.realized_vol
        sigma_window = open_price * current_vol * math.sqrt(total_window / 525600)
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

        # Boost near resolution — scale with z_score to avoid over-boosting
        # weak signals during high-vol regimes where z is inflated by
        # stale 24h sigma. Proportional boost rewards strong moves more.
        if minutes_remaining < 1.0 and z_score > 0.5:
            confidence = min(0.95, confidence + 0.10 * min(z_score, 2.0))
        elif minutes_remaining < 2.0 and z_score > 1.0:
            confidence = min(0.95, confidence + 0.05 * min(z_score, 3.0))

        # Reduce if move is sub-threshold sigma (noise)
        if z_score < self._noise_z:
            confidence *= 0.4

        # Cross-asset agreement is factored in by the decision function
        # (cross_asset_boost lowers zone thresholds), NOT here. Keeping
        # the signal's confidence a pure reflection of the asset's own
        # momentum avoids double-boosting that admits very weak signals.
        _ = reference_signal  # reserved for future per-asset correlation weighting

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
