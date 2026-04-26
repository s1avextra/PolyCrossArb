//! Pure decision function for candle trading.
//!
//! Same logic used in live and backtest. Mirrors
//! `src/polymomentum/crypto/decision.py` for parity.

use serde::{Deserialize, Serialize};

use crate::fair_value::binary_option_price_with_rate;
use crate::strategy::momentum::MomentumSignal;

pub const DEFAULT_MIN_CONFIDENCE: f64 = 0.60;
pub const DEFAULT_MIN_EDGE: f64 = 0.07;
pub const DEFAULT_DEAD_ZONE_LO: f64 = 0.80;
pub const DEFAULT_DEAD_ZONE_HI: f64 = 0.90;
pub const DEFAULT_MIN_PRICE: f64 = 0.10;
pub const DEFAULT_MAX_PRICE: f64 = 0.90;
pub const DEFAULT_EDGE_CAP: f64 = 0.25;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleDecision {
    pub direction: String,
    pub confidence: f64,
    pub z_score: f64,
    pub zone: String,
    pub fair_value: f64,
    pub market_price: f64,
    pub edge: f64,
    pub minutes_remaining: f64,
    pub yes_no_vig: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkipReason {
    pub reason: String,
    pub zone: String,
    pub detail: String,
}

impl SkipReason {
    pub fn new(reason: &str, zone: &str, detail: impl Into<String>) -> Self {
        Self {
            reason: reason.to_string(),
            zone: zone.to_string(),
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ZoneConfig {
    pub early_min_confidence: f64,
    pub early_min_z: f64,
    pub early_min_edge: f64,
    pub primary_min_z: f64,
    pub late_min_confidence: f64,
    pub late_min_z: f64,
    pub late_min_edge: f64,
    pub terminal_min_confidence: f64,
    pub terminal_min_z: f64,
    pub terminal_min_edge: f64,
    pub dead_zone_lo: f64,
    pub dead_zone_hi: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub edge_cap: f64,
    pub min_ev_buffer: f64,
}

impl Default for ZoneConfig {
    fn default() -> Self {
        Self {
            early_min_confidence: 0.55,
            early_min_z: 2.0,
            early_min_edge: 0.03,
            primary_min_z: 1.0,
            late_min_confidence: 0.65,
            late_min_z: 0.5,
            late_min_edge: 0.08,
            terminal_min_confidence: 0.55,
            terminal_min_z: 0.3,
            terminal_min_edge: 0.03,
            dead_zone_lo: DEFAULT_DEAD_ZONE_LO,
            dead_zone_hi: DEFAULT_DEAD_ZONE_HI,
            min_price: DEFAULT_MIN_PRICE,
            max_price: DEFAULT_MAX_PRICE,
            edge_cap: DEFAULT_EDGE_CAP,
            min_ev_buffer: 0.05,
        }
    }
}

impl ZoneConfig {
    pub fn from_settings(s: &crate::config::Settings) -> Self {
        Self {
            early_min_confidence: s.candle_zone_early_min_confidence,
            early_min_z: s.candle_zone_early_min_z,
            early_min_edge: s.candle_zone_early_min_edge,
            primary_min_z: s.candle_zone_primary_min_z,
            late_min_confidence: s.candle_zone_late_min_confidence,
            late_min_z: s.candle_zone_late_min_z,
            late_min_edge: s.candle_zone_late_min_edge,
            terminal_min_confidence: s.candle_zone_terminal_min_confidence,
            terminal_min_z: s.candle_zone_terminal_min_z,
            terminal_min_edge: s.candle_zone_terminal_min_edge,
            dead_zone_lo: s.candle_dead_zone_lo,
            dead_zone_hi: s.candle_dead_zone_hi,
            min_price: s.candle_min_price,
            max_price: s.candle_max_price,
            edge_cap: s.candle_edge_cap,
            min_ev_buffer: s.candle_min_ev_buffer,
        }
    }
}

pub fn zone_for(elapsed_pct: f64) -> &'static str {
    if elapsed_pct < 0.40 {
        "early"
    } else if elapsed_pct < 0.80 {
        "primary"
    } else if elapsed_pct < 0.95 {
        "late"
    } else {
        "terminal"
    }
}

pub fn zone_thresholds(
    zone: &str,
    min_confidence: f64,
    min_edge: f64,
    cfg: &ZoneConfig,
) -> (f64, f64, f64) {
    match zone {
        "early" => (cfg.early_min_confidence, cfg.early_min_z, cfg.early_min_edge),
        "primary" => (min_confidence, cfg.primary_min_z, min_edge),
        "terminal" => (
            cfg.terminal_min_confidence,
            cfg.terminal_min_z,
            cfg.terminal_min_edge,
        ),
        _ => (
            cfg.late_min_confidence,
            cfg.late_min_z,
            min_edge.max(cfg.late_min_edge),
        ),
    }
}

#[derive(Debug, Clone)]
pub enum DecisionResult {
    Trade(CandleDecision),
    Skip(SkipReason),
}

#[allow(clippy::too_many_arguments)]
pub fn decide_candle_trade(
    signal: &MomentumSignal,
    minutes_elapsed: f64,
    minutes_remaining: f64,
    window_minutes: f64,
    up_price: f64,
    down_price: f64,
    btc_price: f64,
    open_btc: f64,
    implied_vol: f64,
    min_confidence: f64,
    min_edge: f64,
    skip_dead_zone: bool,
    zone_config: &ZoneConfig,
    cross_asset_boost: f64,
) -> DecisionResult {
    let cfg = zone_config;

    // 4-zone entry timing
    let elapsed_pct = if window_minutes > 0.0 {
        minutes_elapsed / window_minutes
    } else {
        1.0
    };
    let zone = zone_for(elapsed_pct);
    let (mut z_min_conf, mut z_min_z, z_min_edge) =
        zone_thresholds(zone, min_confidence, min_edge, cfg);

    if cross_asset_boost > 0.0 {
        z_min_conf = (z_min_conf - cross_asset_boost).max(0.40);
        z_min_z = (z_min_z - cross_asset_boost).max(0.1);
    }

    if signal.confidence < z_min_conf {
        return DecisionResult::Skip(SkipReason::new(
            "low_confidence",
            zone,
            format!("{:.2} < {:.2}", signal.confidence, z_min_conf),
        ));
    }

    if signal.z_score < z_min_z {
        return DecisionResult::Skip(SkipReason::new(
            "low_z_score",
            zone,
            format!("{:.2} < {:.2}", signal.z_score, z_min_z),
        ));
    }

    if skip_dead_zone
        && signal.confidence >= cfg.dead_zone_lo
        && signal.confidence < cfg.dead_zone_hi
    {
        return DecisionResult::Skip(SkipReason::new("dead_zone_80_90", zone, ""));
    }

    let market_price = if signal.direction == "up" {
        up_price
    } else {
        down_price
    };

    if market_price < cfg.min_price || market_price > cfg.max_price {
        return DecisionResult::Skip(SkipReason::new(
            "price_out_of_range",
            zone,
            format!("{:.2}", market_price),
        ));
    }

    if signal.confidence < market_price + cfg.min_ev_buffer {
        return DecisionResult::Skip(SkipReason::new(
            "negative_ev",
            zone,
            format!(
                "conf={:.2}<price={:.2}+{:.2}",
                signal.confidence, market_price, cfg.min_ev_buffer
            ),
        ));
    }

    let yes_no_vig = up_price + down_price - 1.0;

    let days_remaining = minutes_remaining / 1440.0;
    let raw_fair = binary_option_price_with_rate(
        btc_price,
        open_btc,
        days_remaining,
        implied_vol,
        0.05,
    );
    let fair_value = if signal.direction == "up" {
        raw_fair
    } else {
        1.0 - raw_fair
    };
    let edge = fair_value - market_price;

    if zone != "terminal" && edge > cfg.edge_cap {
        return DecisionResult::Skip(SkipReason::new(
            "edge_too_high_stale",
            zone,
            format!("{:.2}", edge),
        ));
    }

    if edge < z_min_edge {
        return DecisionResult::Skip(SkipReason::new(
            "low_edge",
            zone,
            format!("{:.3} < {:.3}", edge, z_min_edge),
        ));
    }

    DecisionResult::Trade(CandleDecision {
        direction: signal.direction.clone(),
        confidence: signal.confidence,
        z_score: signal.z_score,
        zone: zone.to_string(),
        fair_value,
        market_price,
        edge,
        minutes_remaining,
        yes_no_vig,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_signal(confidence: f64, z: f64, direction: &str) -> MomentumSignal {
        MomentumSignal {
            direction: direction.to_string(),
            confidence,
            price_change: 100.0,
            price_change_pct: 0.001,
            consistency: 0.8,
            minutes_elapsed: 4.0,
            minutes_remaining: 1.0,
            current_price: 70_100.0,
            open_price: 70_000.0,
            z_score: z,
            reversion_count: 1,
        }
    }

    #[test]
    fn skips_low_confidence() {
        let sig = mk_signal(0.40, 1.5, "up");
        let cfg = ZoneConfig::default();
        let r = decide_candle_trade(
            &sig, 4.0, 1.0, 5.0, 0.5, 0.5, 70_100.0, 70_000.0, 0.5,
            DEFAULT_MIN_CONFIDENCE, DEFAULT_MIN_EDGE, true, &cfg, 0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "low_confidence"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn skips_dead_zone() {
        let sig = mk_signal(0.85, 1.5, "up");
        let cfg = ZoneConfig::default();
        let r = decide_candle_trade(
            &sig, 4.0, 1.0, 5.0, 0.5, 0.5, 70_100.0, 70_000.0, 0.5,
            DEFAULT_MIN_CONFIDENCE, DEFAULT_MIN_EDGE, true, &cfg, 0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "dead_zone_80_90"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn skips_price_out_of_range() {
        let sig = mk_signal(0.95, 1.5, "up");
        let cfg = ZoneConfig::default();
        let r = decide_candle_trade(
            &sig, 4.0, 1.0, 5.0, 0.95, 0.05, 70_100.0, 70_000.0, 0.5,
            DEFAULT_MIN_CONFIDENCE, DEFAULT_MIN_EDGE, true, &cfg, 0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "price_out_of_range"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn produces_trade_at_terminal() {
        // Strong up momentum, low up price, terminal zone — should fire.
        let sig = mk_signal(0.75, 2.0, "up");
        let cfg = ZoneConfig::default();
        let r = decide_candle_trade(
            &sig, 4.95, 0.05, 5.0, 0.30, 0.70, 70_500.0, 70_000.0, 0.5,
            DEFAULT_MIN_CONFIDENCE, DEFAULT_MIN_EDGE, true, &cfg, 0.0,
        );
        match r {
            DecisionResult::Trade(t) => {
                assert_eq!(t.direction, "up");
                assert_eq!(t.zone, "terminal");
                assert!(t.edge > 0.0);
            }
            DecisionResult::Skip(s) => panic!("expected trade, got skip {}", s.reason),
        }
    }

    #[test]
    fn rejects_negative_ev() {
        // confidence ~ market_price → negative_ev gate
        let sig = mk_signal(0.65, 1.5, "up");
        let cfg = ZoneConfig::default();
        let r = decide_candle_trade(
            &sig, 4.0, 1.0, 5.0, 0.65, 0.35, 70_100.0, 70_000.0, 0.5,
            DEFAULT_MIN_CONFIDENCE, DEFAULT_MIN_EDGE, true, &cfg, 0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "negative_ev"),
            _ => panic!("expected skip"),
        }
    }
}
