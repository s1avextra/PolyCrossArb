"""Pure decision function for candle trading.

Extracted from CandlePipeline._scan_loop so that LIVE and BACKTEST run
the SAME decision code path. Any change to the strategy automatically
applies to both modes.

Inputs:
    - MomentumSignal       (from MomentumDetector — same in live + backtest)
    - market state         (up_price, down_price, open_btc, current_btc)
    - vol estimate         (implied_vol — Deribit live, historical for backtest)
    - timing               (minutes_elapsed, minutes_remaining, window_minutes)
    - thresholds           (min_confidence, min_edge — configurable)

Output:
    - CandleDecision  (or None if no trade)

The decision logic:
    1. 3-zone entry timing (early/primary/late) with escalating thresholds
    2. Skip 80-90% confidence dead zone (backtest-validated bad bucket)
    3. Skip price out-of-range (< 0.10 or > 0.90)
    4. Black-Scholes fair value computation
    5. Edge filter
    6. Detect YES+NO mispricing for risk-free overlay
"""
from __future__ import annotations

from dataclasses import dataclass

from polymomentum.crypto.fair_value import compute_fair_value
from polymomentum.crypto.momentum import MomentumSignal


@dataclass
class CandleDecision:
    """A trade decision from the candle strategy."""
    direction: str          # "up" or "down"
    confidence: float
    z_score: float
    zone: str               # "early", "primary", "late"
    fair_value: float
    market_price: float
    edge: float
    minutes_remaining: float
    yes_no_vig: float       # up_price + down_price - 1.0


@dataclass
class SkipReason:
    """Why a potential trade was skipped (for backtest analysis)."""
    reason: str
    zone: str = ""
    detail: str = ""


# Default thresholds — match production CandlePipeline
DEFAULT_MIN_CONFIDENCE = 0.60
DEFAULT_MIN_EDGE = 0.07
DEFAULT_DEAD_ZONE_LO = 0.80
DEFAULT_DEAD_ZONE_HI = 0.90
DEFAULT_MIN_PRICE = 0.10
DEFAULT_MAX_PRICE = 0.90
DEFAULT_EDGE_CAP = 0.25  # edges above this are stale-data signals


@dataclass(frozen=True)
class ZoneConfig:
    """Tunable per-zone thresholds. Lifted from settings.candle_zone_*.

    A single ZoneConfig is shared by live (CandlePipeline) and backtest
    (CandleStrategyAdapter) so any tuning applies to both with no code
    edits. Frozen so it can be hashed and reused as a singleton — the
    backtest hot path constructs millions of ZoneConfig defaults if we
    don't reuse one.

    Zones (elapsed %):
      early    [0%, 40%)
      primary  [40%, 80%)
      late     [80%, 95%)
      terminal [95%, 100%]  — last ~15s of a 5-min candle
    """
    early_min_confidence: float = 0.55
    early_min_z: float = 2.0
    early_min_edge: float = 0.03
    primary_min_z: float = 1.0
    late_min_confidence: float = 0.65
    late_min_z: float = 0.5
    late_min_edge: float = 0.08
    terminal_min_confidence: float = 0.55
    terminal_min_z: float = 0.3
    terminal_min_edge: float = 0.03
    dead_zone_lo: float = DEFAULT_DEAD_ZONE_LO
    dead_zone_hi: float = DEFAULT_DEAD_ZONE_HI
    min_price: float = DEFAULT_MIN_PRICE
    max_price: float = DEFAULT_MAX_PRICE
    edge_cap: float = DEFAULT_EDGE_CAP


# Singleton default — avoids constructing a fresh ZoneConfig() per call
# in hot loops (the live bot calls decide_candle_trade ~2 Hz, but the
# backtest may call it tens of thousands of times per second).
_DEFAULT_ZONE_CONFIG = ZoneConfig()


def zone_for(elapsed_pct: float) -> str:
    """Map elapsed-pct to zone name.

    Terminal zone (≥95%) captures the last ~15s of a 5-min candle where
    BTC outcome is ~90%+ determined but the MM may still show stale prices.
    """
    if elapsed_pct < 0.40:
        return "early"
    if elapsed_pct < 0.80:
        return "primary"
    if elapsed_pct < 0.95:
        return "late"
    return "terminal"


def zone_thresholds(
    zone: str,
    min_confidence: float,
    min_edge: float,
    cfg: ZoneConfig | None = None,
) -> tuple[float, float, float]:
    """Return (min_confidence, min_z_score, min_edge) for the given zone."""
    cfg = cfg or _DEFAULT_ZONE_CONFIG
    if zone == "early":
        return (cfg.early_min_confidence, cfg.early_min_z, cfg.early_min_edge)
    if zone == "primary":
        return (min_confidence, cfg.primary_min_z, min_edge)
    if zone == "terminal":
        return (cfg.terminal_min_confidence, cfg.terminal_min_z, cfg.terminal_min_edge)
    return (cfg.late_min_confidence, cfg.late_min_z, max(min_edge, cfg.late_min_edge))


def decide_candle_trade(
    signal: MomentumSignal,
    minutes_elapsed: float,
    minutes_remaining: float,
    window_minutes: float,
    up_price: float,
    down_price: float,
    btc_price: float,
    open_btc: float,
    implied_vol: float,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    min_edge: float = DEFAULT_MIN_EDGE,
    skip_dead_zone: bool = True,
    zone_config: ZoneConfig | None = None,
    cross_asset_boost: float = 0.0,
) -> CandleDecision | SkipReason:
    """Decide whether to enter a candle trade.

    Returns a CandleDecision on go-trade or a SkipReason on skip.
    Pure function — no side effects, no time.time(), no I/O.

    Same logic used in live (CandlePipeline._scan_loop) and backtest.

    Args:
        cross_asset_boost: confidence boost from a reference asset (e.g. BTC
            for ETH/SOL contracts). Applied as a flat reduction to zone
            confidence and z-score thresholds when positive.
    """
    cfg = zone_config or _DEFAULT_ZONE_CONFIG

    # ── 4-Zone entry timing ──────────────────────────────────────
    elapsed_pct = minutes_elapsed / window_minutes if window_minutes > 0 else 1.0
    zone = zone_for(elapsed_pct)
    z_min_conf, z_min_z, z_min_edge = zone_thresholds(zone, min_confidence, min_edge, cfg)

    # Cross-asset boost lowers thresholds when a reference asset strongly agrees
    if cross_asset_boost > 0:
        z_min_conf = max(0.40, z_min_conf - cross_asset_boost)
        z_min_z = max(0.1, z_min_z - cross_asset_boost)

    # ── Confidence and z-score gates ────────────────────────────
    if signal.confidence < z_min_conf:
        return SkipReason(
            reason="low_confidence",
            zone=zone,
            detail=f"{signal.confidence:.2f} < {z_min_conf:.2f}",
        )

    if signal.z_score < z_min_z:
        return SkipReason(
            reason="low_z_score",
            zone=zone,
            detail=f"{signal.z_score:.2f} < {z_min_z:.2f}",
        )

    # ── Dead zone filter (80-90% confidence) ────────────────────
    if skip_dead_zone and cfg.dead_zone_lo <= signal.confidence < cfg.dead_zone_hi:
        return SkipReason(reason="dead_zone_80_90", zone=zone)

    # ── Pick the side and check price range ─────────────────────
    if signal.direction == "up":
        market_price = up_price
    else:
        market_price = down_price

    if market_price < cfg.min_price or market_price > cfg.max_price:
        return SkipReason(
            reason="price_out_of_range",
            zone=zone,
            detail=f"{market_price:.2f}",
        )

    # ── YES+NO mispricing (free arb overlay) ────────────────────
    yes_no_vig = up_price + down_price - 1.0

    # ── Black-Scholes fair value ────────────────────────────────
    days_remaining = minutes_remaining / 1440.0
    fv_result = compute_fair_value(
        btc_price=btc_price,
        strike=open_btc,
        days_to_expiry=days_remaining,
        volatility=implied_vol,
        market_price=market_price,
    )
    if signal.direction == "up":
        fair_value = fv_result.fair_price
    else:
        fair_value = 1.0 - fv_result.fair_price

    edge = fair_value - market_price

    # Cap edges that signal stale data — but NOT in terminal zone.
    # At very short time horizons (<15s), BS fair value is nearly binary
    # (close to 0.0 or 1.0) for any directional move, producing edges
    # of ~0.50. These are real edges (outcome is ~determined), not stale.
    if zone != "terminal" and edge > cfg.edge_cap:
        return SkipReason(
            reason="edge_too_high_stale",
            zone=zone,
            detail=f"{edge:.2f}",
        )

    if edge < z_min_edge:
        return SkipReason(
            reason="low_edge",
            zone=zone,
            detail=f"{edge:.3f} < {z_min_edge:.3f}",
        )

    return CandleDecision(
        direction=signal.direction,
        confidence=signal.confidence,
        z_score=signal.z_score,
        zone=zone,
        fair_value=fair_value,
        market_price=market_price,
        edge=edge,
        minutes_remaining=minutes_remaining,
        yes_no_vig=yes_no_vig,
    )


def zone_config_from_settings() -> ZoneConfig:
    """Build a ZoneConfig from the global Settings (live mode)."""
    from polymomentum.config import settings
    return ZoneConfig(
        early_min_confidence=settings.candle_zone_early_min_confidence,
        early_min_z=settings.candle_zone_early_min_z,
        early_min_edge=settings.candle_zone_early_min_edge,
        primary_min_z=settings.candle_zone_primary_min_z,
        late_min_confidence=settings.candle_zone_late_min_confidence,
        late_min_z=settings.candle_zone_late_min_z,
        late_min_edge=settings.candle_zone_late_min_edge,
        terminal_min_confidence=settings.candle_zone_terminal_min_confidence,
        terminal_min_z=settings.candle_zone_terminal_min_z,
        terminal_min_edge=settings.candle_zone_terminal_min_edge,
        dead_zone_lo=settings.candle_dead_zone_lo,
        dead_zone_hi=settings.candle_dead_zone_hi,
        min_price=settings.candle_min_price,
        max_price=settings.candle_max_price,
        edge_cap=settings.candle_edge_cap,
    )
