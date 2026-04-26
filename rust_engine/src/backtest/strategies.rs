//! Strategy variants for the backtest harness.
//!
//! Each variant wraps the live `decide_candle_trade` with a different
//! `ZoneConfig`. The harness loops one variant at a time over the same PMXT
//! v2 + BTC tape so per-strategy P&L is comparable.

use crate::strategy::decision::ZoneConfig;

/// Tunable knobs the harness varies. The variant name is what shows up in
/// the report.
#[derive(Debug, Clone)]
pub struct StrategyVariant {
    pub name: String,
    pub zone_config: ZoneConfig,
    pub skip_dead_zone: bool,
    pub min_confidence: f64,
    pub min_edge: f64,
    /// Fraction of bankroll per trade (capped by `max_per_market_usd`).
    pub position_pct: f64,
    /// Hard cap on position size (USD).
    pub max_per_market_usd: f64,
    /// Use maker-first fill model instead of one-tick taker.
    pub prefer_maker: bool,
    /// Default fee rate for taker fills.
    pub default_fee_rate: f64,
}

impl StrategyVariant {
    pub fn baseline() -> Self {
        Self {
            name: "baseline".into(),
            zone_config: ZoneConfig::default(),
            skip_dead_zone: true,
            min_confidence: 0.60,
            min_edge: 0.07,
            position_pct: 0.10,
            max_per_market_usd: 20.0,
            prefer_maker: false,
            default_fee_rate: 0.072,
        }
    }

    pub fn terminal_only() -> Self {
        let mut cfg = ZoneConfig::default();
        cfg.early_min_confidence = 1.1;
        cfg.early_min_z = 100.0;
        cfg.late_min_confidence = 1.1;
        cfg.late_min_z = 100.0;
        cfg.primary_min_z = 100.0;
        Self {
            name: "terminal_only".into(),
            zone_config: cfg,
            ..Self::baseline()
        }
    }

    pub fn aggressive_terminal() -> Self {
        let mut cfg = ZoneConfig::default();
        cfg.early_min_confidence = 1.1;
        cfg.early_min_z = 100.0;
        cfg.late_min_confidence = 1.1;
        cfg.late_min_z = 100.0;
        cfg.primary_min_z = 100.0;
        cfg.terminal_min_confidence = 0.50;
        cfg.terminal_min_z = 0.20;
        cfg.terminal_min_edge = 0.02;
        cfg.min_ev_buffer = 0.03;
        Self {
            name: "aggressive_terminal".into(),
            zone_config: cfg,
            ..Self::baseline()
        }
    }

    pub fn conservative_terminal() -> Self {
        let mut cfg = ZoneConfig::default();
        cfg.early_min_confidence = 1.1;
        cfg.early_min_z = 100.0;
        cfg.late_min_confidence = 1.1;
        cfg.late_min_z = 100.0;
        cfg.primary_min_z = 100.0;
        cfg.terminal_min_confidence = 0.65;
        cfg.terminal_min_z = 0.50;
        cfg.terminal_min_edge = 0.07;
        cfg.min_ev_buffer = 0.07;
        Self {
            name: "conservative_terminal".into(),
            zone_config: cfg,
            ..Self::baseline()
        }
    }

    pub fn maker_first() -> Self {
        Self {
            name: "maker_first".into(),
            prefer_maker: true,
            default_fee_rate: 0.0,
            ..Self::baseline()
        }
    }

    /// Very loose confidence/z thresholds — forces trades to fire so we can
    /// verify the harness wiring + resolver. Don't use this for production
    /// numbers; it'll over-fire on noise.
    pub fn loose_smoke() -> Self {
        let mut cfg = ZoneConfig::default();
        cfg.early_min_confidence = 0.15;
        cfg.early_min_z = 0.10;
        cfg.early_min_edge = 0.0;
        cfg.late_min_confidence = 0.15;
        cfg.late_min_z = 0.10;
        cfg.late_min_edge = 0.0;
        cfg.terminal_min_confidence = 0.15;
        cfg.terminal_min_z = 0.10;
        cfg.terminal_min_edge = 0.0;
        cfg.primary_min_z = 0.10;
        cfg.min_ev_buffer = -1.0;
        Self {
            name: "loose_smoke".into(),
            zone_config: cfg,
            skip_dead_zone: false,
            min_confidence: 0.15,
            min_edge: 0.0,
            position_pct: 0.10,
            max_per_market_usd: 20.0,
            prefer_maker: false,
            default_fee_rate: 0.072,
        }
    }

    /// Same loose gates as `loose_smoke` but assumes maker fills (0% fee).
    /// On the 2026-04-26 4-hour run, `loose_smoke` produced 64.6% WR but
    /// fees ate the edge; this variant tests whether maker pricing flips
    /// the result to positive. (Optimistic: a real maker route needs a
    /// fill-rate model, but this tells us if it's worth pursuing.)
    pub fn loose_maker() -> Self {
        Self {
            name: "loose_maker".into(),
            prefer_maker: true,
            default_fee_rate: 0.0,
            ..Self::loose_smoke()
        }
    }
}

/// Default sweep set for the harness.
pub fn default_variants() -> Vec<StrategyVariant> {
    vec![
        StrategyVariant::loose_smoke(),
        StrategyVariant::loose_maker(),
        StrategyVariant::baseline(),
        StrategyVariant::terminal_only(),
        StrategyVariant::aggressive_terminal(),
        StrategyVariant::conservative_terminal(),
        StrategyVariant::maker_first(),
    ]
}
