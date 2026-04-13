"""Strategy protocol and implementations for the backtest harness.

A Strategy encapsulates the decision logic so that the backtest adapter
can run different strategies against the same L2 replay data without
rewriting the event loop.

Strategies:
  - BaselineStrategy  — current production logic (decide_candle_trade)
  - EwmaVolStrategy   — replaces stale 24h vol with EWMA fast/slow estimates
  - RegimeStrategy    — per-regime ZoneConfig tables with hysteresis
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Protocol

from polycrossarb.crypto.decision import (
    CandleDecision,
    SkipReason,
    ZoneConfig,
    decide_candle_trade,
)
from polycrossarb.crypto.momentum import (
    MomentumSignal,
    VolatilityRegime,
    classify_vol_regime,
)


class Strategy(Protocol):
    """Swappable decision strategy for the backtest adapter."""

    name: str

    def on_tick(self, ts_s: float, price: float) -> None:
        """Update internal state from a raw price tick. Called before decide()."""

    def decide(
        self,
        signal: MomentumSignal,
        *,
        minutes_elapsed: float,
        minutes_remaining: float,
        window_minutes: float,
        up_price: float,
        down_price: float,
        btc_price: float,
        open_btc: float,
        implied_vol: float,
        cross_asset_boost: float = 0.0,
    ) -> CandleDecision | SkipReason:
        """Return a trade decision or skip reason."""

    def snapshot(self) -> dict:
        """Return a debug snapshot of internal state (e.g. current vol estimate)."""


# ── Baseline: the current production strategy ────────────────────


@dataclass
class BaselineStrategy:
    """Wraps decide_candle_trade with a pinned ZoneConfig.

    This is the current production behavior — use as a control variant
    when testing new strategies.
    """
    name: str = "baseline"
    zone_config: ZoneConfig = field(default_factory=ZoneConfig)
    min_confidence: float = 0.60
    min_edge: float = 0.07
    skip_dead_zone: bool = True

    def on_tick(self, ts_s: float, price: float) -> None:
        pass

    def decide(
        self,
        signal: MomentumSignal,
        *,
        minutes_elapsed: float,
        minutes_remaining: float,
        window_minutes: float,
        up_price: float,
        down_price: float,
        btc_price: float,
        open_btc: float,
        implied_vol: float,
        cross_asset_boost: float = 0.0,
    ) -> CandleDecision | SkipReason:
        return decide_candle_trade(
            signal=signal,
            minutes_elapsed=minutes_elapsed,
            minutes_remaining=minutes_remaining,
            window_minutes=window_minutes,
            up_price=up_price,
            down_price=down_price,
            btc_price=btc_price,
            open_btc=open_btc,
            implied_vol=implied_vol,
            min_confidence=self.min_confidence,
            min_edge=self.min_edge,
            skip_dead_zone=self.skip_dead_zone,
            zone_config=self.zone_config,
            cross_asset_boost=cross_asset_boost,
        )

    def snapshot(self) -> dict:
        return {"name": self.name}


# ── EWMA vol: self-normalizing sigma ─────────────────────────────


class EwmaVolStrategy:
    """EWMA fast/slow realized vol estimator replacing 24h rolling.

    Maintains two exponentially-weighted estimators of squared log-returns:
      fast: ~15-min half-life (primary sigma for z-score)
      slow: ~4h half-life (baseline for regime ratio)

    Overrides `implied_vol` passed to decide_candle_trade with the fast
    EWMA estimate. Everything else stays the same — this isolates the
    effect of sigma choice on trade count and PnL.

    The canonical HFT vol estimator (Andersen-Bollerslev, 2001) — O(1)
    per tick, stateless across restarts.
    """

    def __init__(
        self,
        name: str = "ewma_vol",
        fast_half_life_min: float = 15.0,
        slow_half_life_min: float = 240.0,
        floor_vol: float = 0.10,
        zone_config: ZoneConfig | None = None,
        min_confidence: float = 0.60,
        min_edge: float = 0.07,
        skip_dead_zone: bool = True,
    ):
        self.name = name
        # EWMA decay per second (per-tick decay is derived from dt at update time).
        # alpha = 1 - exp(-dt / tau), tau = half_life / ln(2)
        self._fast_tau_s = fast_half_life_min * 60.0 / math.log(2)
        self._slow_tau_s = slow_half_life_min * 60.0 / math.log(2)
        self._floor_vol = floor_vol

        self.zone_config = zone_config or ZoneConfig()
        self.min_confidence = min_confidence
        self.min_edge = min_edge
        self.skip_dead_zone = skip_dead_zone

        # EWMA of variance of log returns (per-second rate)
        self._fast_var: float = 0.0
        self._slow_var: float = 0.0
        self._last_ts: float | None = None
        self._last_price: float | None = None
        self._warmed_up: bool = False
        self._n_ticks: int = 0

    def on_tick(self, ts_s: float, price: float) -> None:
        if self._last_ts is None or self._last_price is None:
            self._last_ts = ts_s
            self._last_price = price
            return
        dt = ts_s - self._last_ts
        if dt <= 0 or price <= 0 or self._last_price <= 0:
            return

        log_return = math.log(price / self._last_price)
        # Per-second squared return rate
        r2_rate = (log_return * log_return) / dt

        fast_alpha = 1.0 - math.exp(-dt / self._fast_tau_s)
        slow_alpha = 1.0 - math.exp(-dt / self._slow_tau_s)

        if not self._warmed_up:
            self._fast_var = r2_rate
            self._slow_var = r2_rate
            self._warmed_up = True
        else:
            self._fast_var = (1 - fast_alpha) * self._fast_var + fast_alpha * r2_rate
            self._slow_var = (1 - slow_alpha) * self._slow_var + slow_alpha * r2_rate

        self._last_ts = ts_s
        self._last_price = price
        self._n_ticks += 1

    @property
    def fast_vol_annualized(self) -> float:
        """EWMA fast annualized vol. Floored at `floor_vol`."""
        if not self._warmed_up:
            return self._floor_vol
        seconds_per_year = 365.25 * 86400
        v = math.sqrt(max(0.0, self._fast_var * seconds_per_year))
        return max(self._floor_vol, min(5.0, v))

    @property
    def slow_vol_annualized(self) -> float:
        if not self._warmed_up:
            return self._floor_vol
        seconds_per_year = 365.25 * 86400
        v = math.sqrt(max(0.0, self._slow_var * seconds_per_year))
        return max(self._floor_vol, min(5.0, v))

    @property
    def vol_ratio(self) -> float:
        """fast/slow — regime indicator. >1.5 = HIGH, <0.5 = LOW."""
        slow = self.slow_vol_annualized
        if slow <= 0:
            return 1.0
        return self.fast_vol_annualized / slow

    def decide(
        self,
        signal: MomentumSignal,
        *,
        minutes_elapsed: float,
        minutes_remaining: float,
        window_minutes: float,
        up_price: float,
        down_price: float,
        btc_price: float,
        open_btc: float,
        implied_vol: float,
        cross_asset_boost: float = 0.0,
    ) -> CandleDecision | SkipReason:
        # Override vol with EWMA fast — this is the WHOLE POINT
        override_vol = self.fast_vol_annualized
        return decide_candle_trade(
            signal=signal,
            minutes_elapsed=minutes_elapsed,
            minutes_remaining=minutes_remaining,
            window_minutes=window_minutes,
            up_price=up_price,
            down_price=down_price,
            btc_price=btc_price,
            open_btc=open_btc,
            implied_vol=override_vol,
            min_confidence=self.min_confidence,
            min_edge=self.min_edge,
            skip_dead_zone=self.skip_dead_zone,
            zone_config=self.zone_config,
            cross_asset_boost=cross_asset_boost,
        )

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "n_ticks": self._n_ticks,
            "fast_vol": round(self.fast_vol_annualized, 4),
            "slow_vol": round(self.slow_vol_annualized, 4),
            "vol_ratio": round(self.vol_ratio, 3),
        }


# ── Regime-conditional: per-regime ZoneConfig tables ─────────────


class RegimeConditionalStrategy:
    """Keeps separate ZoneConfigs per volatility regime.

    Uses EWMA fast/slow ratio as the regime classifier (not the current
    production classify_vol_regime which uses 24h baseline).

    Hysteresis: requires 2 consecutive classifications to switch regime,
    prevents flickering at bucket boundaries.
    """

    def __init__(
        self,
        name: str = "regime",
        low_config: ZoneConfig | None = None,
        normal_config: ZoneConfig | None = None,
        high_config: ZoneConfig | None = None,
        extreme_config: ZoneConfig | None = None,
        fast_half_life_min: float = 15.0,
        slow_half_life_min: float = 240.0,
        floor_vol: float = 0.10,
        min_confidence: float = 0.60,
        min_edge: float = 0.07,
        skip_dead_zone: bool = True,
        hysteresis_ticks: int = 2,
    ):
        self.name = name
        # Default: same config everywhere — user overrides what they want
        default = ZoneConfig()
        self._configs: dict[VolatilityRegime, ZoneConfig] = {
            VolatilityRegime.LOW: low_config or default,
            VolatilityRegime.NORMAL: normal_config or default,
            VolatilityRegime.HIGH: high_config or default,
            VolatilityRegime.EXTREME: extreme_config or default,
        }
        self.min_confidence = min_confidence
        self.min_edge = min_edge
        self.skip_dead_zone = skip_dead_zone

        self._vol = EwmaVolStrategy(
            fast_half_life_min=fast_half_life_min,
            slow_half_life_min=slow_half_life_min,
            floor_vol=floor_vol,
        )
        self._current_regime = VolatilityRegime.NORMAL
        self._pending_regime: VolatilityRegime | None = None
        self._pending_count = 0
        self._hysteresis = hysteresis_ticks

    def on_tick(self, ts_s: float, price: float) -> None:
        self._vol.on_tick(ts_s, price)
        fast = self._vol.fast_vol_annualized
        slow = self._vol.slow_vol_annualized
        proposed = classify_vol_regime(fast, slow)

        if proposed == self._current_regime:
            self._pending_regime = None
            self._pending_count = 0
            return

        if proposed == self._pending_regime:
            self._pending_count += 1
            if self._pending_count >= self._hysteresis:
                self._current_regime = proposed
                self._pending_regime = None
                self._pending_count = 0
        else:
            self._pending_regime = proposed
            self._pending_count = 1

    @property
    def current_regime(self) -> VolatilityRegime:
        return self._current_regime

    def decide(
        self,
        signal: MomentumSignal,
        *,
        minutes_elapsed: float,
        minutes_remaining: float,
        window_minutes: float,
        up_price: float,
        down_price: float,
        btc_price: float,
        open_btc: float,
        implied_vol: float,
        cross_asset_boost: float = 0.0,
    ) -> CandleDecision | SkipReason:
        zone_cfg = self._configs[self._current_regime]
        override_vol = self._vol.fast_vol_annualized
        return decide_candle_trade(
            signal=signal,
            minutes_elapsed=minutes_elapsed,
            minutes_remaining=minutes_remaining,
            window_minutes=window_minutes,
            up_price=up_price,
            down_price=down_price,
            btc_price=btc_price,
            open_btc=open_btc,
            implied_vol=override_vol,
            min_confidence=self.min_confidence,
            min_edge=self.min_edge,
            skip_dead_zone=self.skip_dead_zone,
            zone_config=zone_cfg,
            cross_asset_boost=cross_asset_boost,
        )

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "regime": self._current_regime.name,
            **self._vol.snapshot(),
        }
