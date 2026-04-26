//! Strategy variants — each is a `ZoneConfig` + dead-zone toggle + label.
//!
//! Add new variants here, then they appear in `polymomentum-engine sweep`.

use crate::strategy::decision::ZoneConfig;

#[derive(Debug, Clone)]
pub struct Strategy {
    pub name: String,
    pub zone_config: ZoneConfig,
    pub skip_dead_zone: bool,
    pub min_confidence: f64,
    pub min_edge: f64,
    pub prefer_maker: bool,
}

pub fn baseline() -> Strategy {
    Strategy {
        name: "baseline".into(),
        zone_config: ZoneConfig::default(),
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
    }
}

/// Disable early/primary/late zones via unreachable thresholds. Only terminal
/// (≥95% elapsed) zone trades fire.
pub fn terminal_only() -> Strategy {
    let mut cfg = ZoneConfig::default();
    cfg.early_min_confidence = 1.1; // unreachable
    cfg.early_min_z = 100.0;
    cfg.late_min_confidence = 1.1;
    cfg.late_min_z = 100.0;
    cfg.primary_min_z = 100.0;
    Strategy {
        name: "terminal_only".into(),
        zone_config: cfg,
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
    }
}

/// Looser terminal entry — wider eligibility, see if the extra trades hold up.
pub fn aggressive_terminal() -> Strategy {
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
    Strategy {
        name: "aggressive_terminal".into(),
        zone_config: cfg,
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
    }
}

/// Tighter terminal — require strong agreement before firing.
pub fn conservative_terminal() -> Strategy {
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
    Strategy {
        name: "conservative_terminal".into(),
        zone_config: cfg,
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
    }
}

/// Disable dead-zone filter (allow 0.80-0.90 confidence trades).
pub fn no_dead_zone() -> Strategy {
    Strategy {
        name: "no_dead_zone".into(),
        zone_config: ZoneConfig::default(),
        skip_dead_zone: false,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
    }
}

/// Disable the entry-price EV filter — see how many of those rejected trades
/// would actually have lost money.
pub fn ev_off() -> Strategy {
    let mut cfg = ZoneConfig::default();
    cfg.min_ev_buffer = -1.0;
    Strategy {
        name: "ev_off".into(),
        zone_config: cfg,
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
    }
}

/// Tighter EV filter — require larger expected-value buffer.
pub fn ev_strict() -> Strategy {
    let mut cfg = ZoneConfig::default();
    cfg.min_ev_buffer = 0.10;
    Strategy {
        name: "ev_strict".into(),
        zone_config: cfg,
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: false,
    }
}

/// Maker-first — assumes a posted limit gets filled at improvement, with 0%
/// fee. Optimistic; a real maker route needs a fallback and timing.
pub fn maker_first() -> Strategy {
    Strategy {
        name: "maker_first".into(),
        zone_config: ZoneConfig::default(),
        skip_dead_zone: true,
        min_confidence: 0.60,
        min_edge: 0.07,
        prefer_maker: true,
    }
}

/// Default sweep set.
pub fn default_strategies() -> Vec<Strategy> {
    vec![
        baseline(),
        terminal_only(),
        aggressive_terminal(),
        conservative_terminal(),
        no_dead_zone(),
        ev_off(),
        ev_strict(),
        maker_first(),
    ]
}
