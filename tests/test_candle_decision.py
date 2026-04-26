"""Golden tests for the pure candle decision function.

These tests are the live↔backtest parity guarantee: live and L2 backtest
both call ``decide_candle_trade``, so any change here applies to both
modes. If a test breaks, the strategy semantics changed.
"""
from __future__ import annotations

import pytest

from polymomentum.crypto.decision import (
    CandleDecision,
    SkipReason,
    ZoneConfig,
    decide_candle_trade,
    zone_for,
    zone_thresholds,
)
from polymomentum.crypto.momentum import (
    MomentumSignal,
    VolatilityRegime,
    classify_vol_regime,
)


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
    (0.94, "late"),
    (0.95, "terminal"),
    (0.99, "terminal"),
    (1.00, "terminal"),
    (1.50, "terminal"),
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


def test_negative_ev_skipped_when_confidence_below_market_price() -> None:
    """If confidence < market_price + buffer, expected value is negative.

    Mirrors yesterday's Trade #2: bot fired UP at fill $0.797 with 61%
    confidence — break-even WR for that price is ~80%, so the trade had
    ~20pp negative EV. Filter must catch this.

    Uses terminal zone (elapsed >= 95%) where 0.55 confidence floor
    applies, so 0.61 conf passes the primary gate and we reach the EV
    filter. Then 0.61 < 0.795 + 0.05 → skip with negative_ev.
    """
    sig = make_signal(direction="up", confidence=0.61, z_score=1.0,
                      minutes_elapsed=4.85, minutes_remaining=0.15)
    res = call_decide(
        sig,
        up_price=0.795, down_price=0.205,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=4.85, minutes_remaining=0.15, window_minutes=5.0,
    )
    assert isinstance(res, SkipReason)
    assert res.reason == "negative_ev"


def test_positive_ev_passes_filter() -> None:
    """Confidence comfortably above market price + buffer should pass."""
    sig = make_signal(direction="up", confidence=0.70, z_score=1.5,
                      minutes_elapsed=4.85, minutes_remaining=0.15)
    res = call_decide(
        sig,
        up_price=0.45, down_price=0.55,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=4.85, minutes_remaining=0.15, window_minutes=5.0,
    )
    # 0.70 > 0.45 + 0.05 — easily clears EV filter
    assert isinstance(res, CandleDecision)


def test_ev_filter_disabled_with_negative_buffer() -> None:
    """Setting min_ev_buffer < 0 disables the filter (allows negative-EV trades)."""
    sig = make_signal(direction="up", confidence=0.61, z_score=1.0,
                      minutes_elapsed=4.85, minutes_remaining=0.15)
    cfg = ZoneConfig(min_ev_buffer=-1.0)
    res = call_decide(
        sig,
        up_price=0.795, down_price=0.205,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=4.85, minutes_remaining=0.15, window_minutes=5.0,
        zone_config=cfg,
    )
    # No longer skipped for negative_ev. Could be a CandleDecision or some
    # other skip reason (edge, etc.) — just NOT negative_ev.
    if isinstance(res, SkipReason):
        assert res.reason != "negative_ev"


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


def test_zero_window_minutes_falls_into_terminal_zone() -> None:
    sig = make_signal(confidence=0.75, z_score=2.0)
    res = call_decide(
        sig,
        minutes_elapsed=1.0,
        minutes_remaining=0.0,
        window_minutes=0.0,
    )
    # window_minutes=0 → elapsed_pct = 1.0 → terminal zone
    # minutes_remaining=0 means BS days_remaining=0 — fair value clamps but
    # we still get a deterministic skip or decision. Either way the zone tag
    # must be "terminal".
    if isinstance(res, SkipReason):
        assert res.zone == "terminal"
    else:
        assert res.zone == "terminal"


# ── Terminal zone ──────────────────────────────────────────────────


def test_zone_thresholds_default_terminal() -> None:
    cfg = ZoneConfig()
    conf, z, edge = zone_thresholds("terminal", min_confidence=0.60, min_edge=0.07, cfg=cfg)
    assert (conf, z, edge) == (0.55, 0.3, 0.03)


def test_terminal_zone_relaxed_confidence_admits_trade() -> None:
    """Terminal zone accepts confidence 0.56, which late zone would reject."""
    sig = make_signal(direction="up", confidence=0.56, z_score=0.5)
    # 4.8 min elapsed of 5 min → 96% → terminal zone
    res = call_decide(
        sig,
        up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=4.8, minutes_remaining=0.2, window_minutes=5.0,
    )
    # Terminal thresholds: conf >= 0.55, z >= 0.3, edge >= 0.03
    # This signal passes all three.
    assert isinstance(res, CandleDecision), f"got {res}"
    assert res.zone == "terminal"
    assert res.confidence == 0.56


def test_terminal_zone_relaxed_z_admits_trade() -> None:
    """Terminal zone accepts z_score=0.4, which late zone requires >= 0.5."""
    sig = make_signal(direction="up", confidence=0.70, z_score=0.4)
    res = call_decide(
        sig,
        up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=4.8, minutes_remaining=0.2, window_minutes=5.0,
    )
    assert isinstance(res, CandleDecision), f"got {res}"
    assert res.zone == "terminal"


def test_terminal_zone_rejects_below_thresholds() -> None:
    """Even terminal zone has floors — confidence 0.40 is rejected."""
    sig = make_signal(direction="up", confidence=0.40, z_score=0.2)
    res = call_decide(
        sig,
        up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=4.8, minutes_remaining=0.2, window_minutes=5.0,
    )
    assert isinstance(res, SkipReason)
    assert res.zone == "terminal"
    assert res.reason == "low_confidence"


def test_terminal_zone_low_z_rejected() -> None:
    """Terminal zone z_score floor is 0.3."""
    sig = make_signal(direction="up", confidence=0.70, z_score=0.2)
    res = call_decide(
        sig,
        up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=4.8, minutes_remaining=0.2, window_minutes=5.0,
    )
    assert isinstance(res, SkipReason)
    assert res.zone == "terminal"
    assert res.reason == "low_z_score"


def test_terminal_zone_boundary_at_95pct() -> None:
    """At exactly 95% elapsed, zone switches from late to terminal."""
    sig = make_signal(direction="up", confidence=0.56, z_score=0.4)
    # 94% → late zone, confidence 0.56 < late's 0.65 → rejected
    res_late = call_decide(
        sig,
        up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=4.7, minutes_remaining=0.3, window_minutes=5.0,
    )
    assert isinstance(res_late, SkipReason)
    assert res_late.zone == "late"

    # 96% → terminal zone, same signal now passes
    res_term = call_decide(
        sig,
        up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=4.8, minutes_remaining=0.2, window_minutes=5.0,
    )
    assert isinstance(res_term, CandleDecision), f"got {res_term}"
    assert res_term.zone == "terminal"


def test_zone_config_override_terminal_thresholds() -> None:
    """Terminal thresholds can be overridden via ZoneConfig."""
    cfg = ZoneConfig(terminal_min_confidence=0.80, terminal_min_z=1.0, terminal_min_edge=0.10)
    sig = make_signal(direction="up", confidence=0.70, z_score=0.5)
    res = call_decide(
        sig,
        up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=4.8, minutes_remaining=0.2, window_minutes=5.0,
        zone_config=cfg,
    )
    assert isinstance(res, SkipReason)
    assert res.zone == "terminal"
    assert res.reason == "low_confidence"


# ── Cross-asset boost ─────────────────────────────────────────────


def test_cross_asset_boost_lowers_confidence_threshold() -> None:
    """A signal that fails confidence gate without boost passes with it."""
    sig = make_signal(direction="up", confidence=0.55, z_score=2.0)
    # Primary zone min_confidence default = 0.60, so 0.55 fails
    res_no_boost = call_decide(
        sig,
        up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0,
        minutes_elapsed=3.0, minutes_remaining=2.0, window_minutes=5.0,
    )
    assert isinstance(res_no_boost, SkipReason)
    assert res_no_boost.reason == "low_confidence"

    # With 0.10 cross_asset_boost, threshold drops to 0.50 → signal passes
    res_boosted = decide_candle_trade(
        signal=sig,
        minutes_elapsed=3.0, minutes_remaining=2.0, window_minutes=5.0,
        up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0,
        implied_vol=0.50,
        min_confidence=0.60, min_edge=0.03,
        cross_asset_boost=0.10,
    )
    assert isinstance(res_boosted, CandleDecision), f"got {res_boosted}"


def test_cross_asset_boost_zero_has_no_effect() -> None:
    """Zero boost should be identical to no boost."""
    sig = make_signal(direction="up", confidence=0.55, z_score=2.0)
    res = decide_candle_trade(
        signal=sig,
        minutes_elapsed=3.0, minutes_remaining=2.0, window_minutes=5.0,
        up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0,
        implied_vol=0.50,
        min_confidence=0.60, min_edge=0.03,
        cross_asset_boost=0.0,
    )
    assert isinstance(res, SkipReason)
    assert res.reason == "low_confidence"


def test_cross_asset_boost_has_confidence_floor() -> None:
    """Even with maximum boost, confidence threshold doesn't drop below 0.40."""
    sig = make_signal(direction="up", confidence=0.42, z_score=2.0)
    # up_price=0.30 keeps EV filter happy (conf 0.42 > price 0.30 + buffer 0.05).
    # The test's purpose is the cross-asset boost confidence floor —
    # EV filter is orthogonal.
    res = decide_candle_trade(
        signal=sig,
        minutes_elapsed=3.0, minutes_remaining=2.0, window_minutes=5.0,
        up_price=0.30, down_price=0.70,
        btc_price=60_005.0, open_btc=60_000.0,
        implied_vol=0.50,
        min_confidence=0.60, min_edge=0.03,
        cross_asset_boost=0.50,  # huge boost
    )
    # 0.60 - 0.50 = 0.10, clamped to 0.40. Signal 0.42 >= 0.40, passes.
    assert isinstance(res, CandleDecision), f"got {res}"


# ── VolatilityRegime classification ───────────────────────────────


@pytest.mark.parametrize("short,baseline,expected", [
    (0.50, 0.50, VolatilityRegime.NORMAL),  # ratio 1.0
    (0.20, 0.50, VolatilityRegime.LOW),      # ratio 0.4
    (0.80, 0.50, VolatilityRegime.HIGH),     # ratio 1.6
    (1.50, 0.50, VolatilityRegime.EXTREME),  # ratio 3.0
    (0.0, 0.50, VolatilityRegime.NORMAL),    # zero short → NORMAL
    (0.50, 0.0, VolatilityRegime.NORMAL),    # zero baseline → NORMAL
])
def test_classify_vol_regime(short: float, baseline: float, expected: VolatilityRegime) -> None:
    assert classify_vol_regime(short, baseline) == expected


def test_vol_regime_boundary_high() -> None:
    """Exactly at 1.5 ratio is HIGH, below is NORMAL."""
    assert classify_vol_regime(0.75, 0.50) == VolatilityRegime.NORMAL  # 1.5 exactly
    assert classify_vol_regime(0.76, 0.50) == VolatilityRegime.HIGH    # 1.52 > 1.5


def test_vol_regime_boundary_extreme() -> None:
    """Exactly at 2.5 ratio is HIGH, above is EXTREME."""
    assert classify_vol_regime(1.25, 0.50) == VolatilityRegime.HIGH     # 2.5 exactly
    assert classify_vol_regime(1.26, 0.50) == VolatilityRegime.EXTREME  # 2.52 > 2.5


# ── Boundary condition tests ─────────────────────────────────────


def test_dead_zone_boundary_left_inclusive() -> None:
    """confidence=0.80 is exactly on dead_zone_lo — should be SKIPPED (left-inclusive)."""
    signal = make_signal(confidence=0.80, z_score=2.0)
    result = decide_candle_trade(
        signal=signal, minutes_elapsed=3.0, minutes_remaining=2.0,
        window_minutes=5.0, up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0, implied_vol=0.50,
    )
    assert isinstance(result, SkipReason)
    assert result.reason == "dead_zone_80_90"


def test_dead_zone_boundary_right_exclusive() -> None:
    """confidence=0.90 is exactly on dead_zone_hi — should NOT be skipped (right-exclusive)."""
    signal = make_signal(confidence=0.90, z_score=2.0)
    result = decide_candle_trade(
        signal=signal, minutes_elapsed=3.0, minutes_remaining=2.0,
        window_minutes=5.0, up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0, implied_vol=0.50,
    )
    # Should pass the dead zone filter — may still be skipped by edge,
    # but NOT by dead_zone_80_90.
    if isinstance(result, SkipReason):
        assert result.reason != "dead_zone_80_90"


def test_terminal_z_boundary_exact() -> None:
    """z_score exactly at terminal_min_z (0.3) should PASS (< is exclusive)."""
    signal = make_signal(
        confidence=0.60, z_score=0.3,
        minutes_elapsed=4.8, minutes_remaining=0.2,
    )
    result = decide_candle_trade(
        signal=signal, minutes_elapsed=4.8, minutes_remaining=0.2,
        window_minutes=5.0, up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0, implied_vol=0.50,
    )
    # z=0.3 == terminal_min_z: should NOT be skipped for low_z_score
    if isinstance(result, SkipReason):
        assert result.reason != "low_z_score"


def test_terminal_z_just_below_boundary() -> None:
    """z_score at 0.29 (just below terminal_min_z=0.3) should be SKIPPED."""
    signal = make_signal(
        confidence=0.60, z_score=0.29,
        minutes_elapsed=4.8, minutes_remaining=0.2,
    )
    result = decide_candle_trade(
        signal=signal, minutes_elapsed=4.8, minutes_remaining=0.2,
        window_minutes=5.0, up_price=0.40, down_price=0.60,
        btc_price=60_005.0, open_btc=60_000.0, implied_vol=0.50,
    )
    assert isinstance(result, SkipReason)
    assert result.reason == "low_z_score"
