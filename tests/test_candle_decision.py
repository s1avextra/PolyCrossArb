"""Golden tests for the pure candle decision function.

These tests are the live↔backtest parity guarantee: live and L2 backtest
both call ``decide_candle_trade``, so any change here applies to both
modes. If a test breaks, the strategy semantics changed.
"""
from __future__ import annotations

import pytest

from polycrossarb.crypto.decision import (
    CandleDecision,
    SkipReason,
    ZoneConfig,
    decide_candle_trade,
    zone_for,
    zone_thresholds,
)
from polycrossarb.crypto.momentum import MomentumSignal


# ── Helpers ────────────────────────────────────────────────────────


def make_signal(
    direction: str = "up",
    confidence: float = 0.75,
    z_score: float = 2.5,
    open_price: float = 60_000.0,
    current_price: float = 60_005.0,
    minutes_elapsed: float = 3.0,
    minutes_remaining: float = 2.0,
) -> MomentumSignal:
    """Build a MomentumSignal with sensible defaults for testing.

    Defaults use a $5 BTC move on a $60k base — small enough that the
    Black-Scholes fair value lands in the [0.50, 0.60] band for short
    windows, well below the 0.25 stale-edge cap. Tests that need
    extreme moves override these explicitly.
    """
    price_change = current_price - open_price
    return MomentumSignal(
        direction=direction,
        confidence=confidence,
        price_change=price_change,
        price_change_pct=price_change / open_price,
        consistency=0.85,
        minutes_elapsed=minutes_elapsed,
        minutes_remaining=minutes_remaining,
        current_price=current_price,
        open_price=open_price,
        z_score=z_score,
    )


def call_decide(
    signal: MomentumSignal,
    *,
    minutes_elapsed: float = 3.0,
    minutes_remaining: float = 2.0,
    window_minutes: float = 5.0,
    up_price: float = 0.40,
    down_price: float = 0.60,
    btc_price: float = 60_005.0,
    open_btc: float = 60_000.0,
    implied_vol: float = 0.50,
    min_confidence: float = 0.60,
    min_edge: float = 0.03,
    skip_dead_zone: bool = True,
    zone_config: ZoneConfig | None = None,
):
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
        min_confidence=min_confidence,
        min_edge=min_edge,
        skip_dead_zone=skip_dead_zone,
        zone_config=zone_config,
    )


# ── Zone classifier ────────────────────────────────────────────────


@pytest.mark.parametrize("elapsed_pct,expected_zone", [
    (0.00, "early"),
    (0.39, "early"),
    (0.40, "primary"),
    (0.79, "primary"),
    (0.80, "late"),
    (1.00, "late"),
    (1.50, "late"),
])
def test_zone_for_boundaries(elapsed_pct: float, expected_zone: str) -> None:
    assert zone_for(elapsed_pct) == expected_zone


def test_zone_thresholds_default_early() -> None:
    cfg = ZoneConfig()
    conf, z, edge = zone_thresholds("early", min_confidence=0.60, min_edge=0.07, cfg=cfg)
    assert (conf, z, edge) == (0.55, 2.0, 0.03)


def test_zone_thresholds_default_primary_uses_passed_values() -> None:
    cfg = ZoneConfig()
    conf, z, edge = zone_thresholds("primary", min_confidence=0.60, min_edge=0.07, cfg=cfg)
    assert (conf, z, edge) == (0.60, 1.0, 0.07)


def test_zone_thresholds_default_late_floors_edge_at_8pct() -> None:
    cfg = ZoneConfig()
    # min_edge=0.05 should be raised to 0.08 in late zone
    _, _, edge = zone_thresholds("late", min_confidence=0.60, min_edge=0.05, cfg=cfg)
    assert edge == 0.08
    # min_edge=0.10 stays at 0.10 (no flooring needed)
    _, _, edge2 = zone_thresholds("late", min_confidence=0.60, min_edge=0.10, cfg=cfg)
    assert edge2 == 0.10


# ── Skip reasons ────────────────────────────────────────────────────


def test_low_confidence_skipped_in_early_zone() -> None:
    sig = make_signal(confidence=0.40, z_score=3.0)
    res = call_decide(sig, minutes_elapsed=0.5, window_minutes=5.0)
    assert isinstance(res, SkipReason)
    assert res.reason == "low_confidence"
    assert res.zone == "early"


def test_low_z_score_skipped_in_early_zone() -> None:
    sig = make_signal(confidence=0.70, z_score=1.5)  # >= early conf, < early z
    res = call_decide(sig, minutes_elapsed=0.5, window_minutes=5.0)
    assert isinstance(res, SkipReason)
    assert res.reason == "low_z_score"
    assert res.zone == "early"


def test_low_confidence_in_primary_zone() -> None:
    sig = make_signal(confidence=0.55, z_score=2.0)  # < default 0.60
    res = call_decide(sig, minutes_elapsed=3.0, window_minutes=5.0)
    assert isinstance(res, SkipReason)
    assert res.reason == "low_confidence"
    assert res.zone == "primary"


def test_late_zone_requires_higher_confidence() -> None:
    sig = make_signal(confidence=0.60, z_score=1.0)  # < late 0.65
    res = call_decide(sig, minutes_elapsed=4.5, window_minutes=5.0)
    assert isinstance(res, SkipReason)
    assert res.reason == "low_confidence"
    assert res.zone == "late"


def test_dead_zone_skip_active_by_default() -> None:
    sig = make_signal(confidence=0.85, z_score=2.5)
    res = call_decide(sig, minutes_elapsed=3.0, window_minutes=5.0)
    assert isinstance(res, SkipReason)
    assert res.reason == "dead_zone_80_90"
    assert res.zone == "primary"


def test_dead_zone_can_be_disabled() -> None:
    sig = make_signal(confidence=0.85, z_score=2.5)
    res = call_decide(
        sig,
        minutes_elapsed=3.0,
        window_minutes=5.0,
        skip_dead_zone=False,
    )
    # With dead zone disabled and a small +$5 BTC move, BS fair ~0.55 and
    # market 0.40 → edge ~0.15 (between 0.07 and the 0.25 cap) → trade.
    assert isinstance(res, CandleDecision), f"got {res}"
    assert res.confidence == 0.85


def test_price_below_min_skipped() -> None:
    sig = make_signal(direction="up", confidence=0.75, z_score=2.5)
    res = call_decide(sig, up_price=0.05, down_price=0.95)
    assert isinstance(res, SkipReason)
    assert res.reason == "price_out_of_range"


def test_price_above_max_skipped() -> None:
    sig = make_signal(direction="up", confidence=0.75, z_score=2.5)
    res = call_decide(sig, up_price=0.95, down_price=0.05)
    assert isinstance(res, SkipReason)
    assert res.reason == "price_out_of_range"


def test_edge_cap_skips_stale_signals() -> None:
    # Massive BTC move (huge edge) should look like stale data and skip
    sig = make_signal(
        direction="up", confidence=0.75, z_score=3.0,
        open_price=60_000.0, current_price=70_000.0,
    )
    res = call_decide(
        sig,
        btc_price=70_000.0, open_btc=60_000.0,
        up_price=0.20, down_price=0.80,
        minutes_remaining=1.0, minutes_elapsed=4.0, window_minutes=5.0,
    )
    assert isinstance(res, SkipReason)
    assert res.reason == "edge_too_high_stale"


def test_low_edge_in_primary_zone_skipped() -> None:
    sig = make_signal(direction="up", confidence=0.75, z_score=2.0)
    # btc unchanged → BS fair ~= 0.5; market 0.49 → edge ~0.01 < 0.03
    res = call_decide(
        sig,
        btc_price=60_000.0, open_btc=60_000.0,
        up_price=0.49, down_price=0.51,
        minutes_elapsed=3.0, minutes_remaining=2.0, window_minutes=5.0,
        min_edge=0.03,
    )
    assert isinstance(res, SkipReason)
    assert res.reason == "low_edge"


# ── Successful decisions ────────────────────────────────────────────


def test_primary_zone_decision_up_direction() -> None:
    sig = make_signal(direction="up", confidence=0.75, z_score=2.0)
    res = call_decide(
        sig,
        up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=3.0, minutes_remaining=2.0, window_minutes=5.0,
    )
    assert isinstance(res, CandleDecision), f"got {res}"
    assert res.direction == "up"
    assert res.zone == "primary"
    assert res.market_price == 0.40
    assert res.fair_value > res.market_price  # otherwise edge would be negative
    assert res.edge > 0
    # YES+NO vig should equal up+down-1
    assert res.yes_no_vig == pytest.approx(0.0, abs=1e-9)


def test_down_direction_uses_down_price() -> None:
    sig = make_signal(
        direction="down", confidence=0.75, z_score=2.0,
        open_price=60_000.0, current_price=59_995.0,
    )
    res = call_decide(
        sig,
        up_price=0.60, down_price=0.40,
        btc_price=59_995.0, open_btc=60_000.0,
        minutes_elapsed=3.0, minutes_remaining=2.0, window_minutes=5.0,
    )
    assert isinstance(res, CandleDecision), f"got {res}"
    assert res.direction == "down"
    assert res.market_price == 0.40
    assert res.edge > 0


def test_yes_no_vig_reflects_book_skew() -> None:
    sig = make_signal(direction="up", confidence=0.75, z_score=2.0)
    res = call_decide(
        sig,
        up_price=0.42, down_price=0.62,  # sums to 1.04 — 4% vig
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=3.0, minutes_remaining=2.0, window_minutes=5.0,
    )
    assert isinstance(res, CandleDecision), f"got {res}"
    assert res.yes_no_vig == pytest.approx(0.04, abs=1e-9)


def test_late_zone_late_min_edge_floor() -> None:
    """Late zone enforces min_edge >= 0.08 even if caller passes 0.03.

    Construct btc==open so BS fair value lands cleanly at ~0.50, then
    use market_price 0.45 → edge ~0.05 (passes 0.03 primary floor but
    fails the 0.08 late floor). Confidence 0.78 stays below dead zone.
    """
    sig = make_signal(direction="up", confidence=0.78, z_score=1.0)
    res = call_decide(
        sig,
        up_price=0.45, down_price=0.55,
        btc_price=60_000.0, open_btc=60_000.0,
        minutes_elapsed=9.0, minutes_remaining=1.0, window_minutes=10.0,
        min_edge=0.03,  # caller passes loose min_edge
    )
    assert isinstance(res, SkipReason)
    assert res.reason == "low_edge"
    assert res.zone == "late"


# ── ZoneConfig override behavior ────────────────────────────────────


def test_zone_config_override_dead_zone_window() -> None:
    """Lifting dead_zone_hi lets previously-blocked confidence through."""
    sig = make_signal(confidence=0.85, z_score=2.0)
    cfg = ZoneConfig(dead_zone_lo=0.95, dead_zone_hi=0.98)  # tiny dead zone
    res = call_decide(
        sig,
        up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=3.0, minutes_remaining=2.0, window_minutes=5.0,
        zone_config=cfg,
    )
    assert isinstance(res, CandleDecision), f"got {res}"


def test_zone_config_override_edge_cap_tightens() -> None:
    sig = make_signal(direction="up", confidence=0.75, z_score=2.0)
    cfg = ZoneConfig(edge_cap=0.10)  # very tight cap
    # Set up a state with sizable edge
    res = call_decide(
        sig,
        up_price=0.30, down_price=0.70,
        btc_price=60_500.0, open_btc=60_000.0,
        minutes_elapsed=3.0, minutes_remaining=2.0, window_minutes=5.0,
        zone_config=cfg,
    )
    if isinstance(res, SkipReason):
        # If skipped, it must be the edge cap firing
        assert res.reason in ("edge_too_high_stale",)
    else:
        assert res.edge <= 0.10


def test_zone_config_override_late_min_edge_lifts() -> None:
    sig = make_signal(direction="up", confidence=0.78, z_score=1.0)
    cfg = ZoneConfig(late_min_edge=0.20)  # require huge edge in late
    res = call_decide(
        sig,
        up_price=0.40, down_price=0.60,
        btc_price=60_000.0, open_btc=60_000.0,
        minutes_elapsed=9.0, minutes_remaining=1.0, window_minutes=10.0,
        min_edge=0.03,
        zone_config=cfg,
    )
    # btc==open → BS fair ≈ 0.50, market 0.40 → edge ≈ 0.10, < 0.20 floor
    assert isinstance(res, SkipReason)
    assert res.reason == "low_edge"
    assert res.zone == "late"


def test_zero_window_minutes_falls_into_late_zone() -> None:
    sig = make_signal(confidence=0.75, z_score=2.0)
    res = call_decide(
        sig,
        minutes_elapsed=1.0,
        minutes_remaining=0.0,
        window_minutes=0.0,
    )
    # window_minutes=0 → elapsed_pct = 1.0 → late zone
    # minutes_remaining=0 means BS days_remaining=0 — fair value clamps but
    # we still get a deterministic skip or decision. Either way the zone tag
    # must be "late".
    if isinstance(res, SkipReason):
        assert res.zone == "late"
    else:
        assert res.zone == "late"
