//! Parameter grid for the harness.
//!
//! `SweepGrid` takes a baseline `StrategyVariant` plus per-parameter ranges
//! and generates the cartesian product as a list of variants. The harness
//! then runs each one against the same PMXT v2 hours and ranks them.
//!
//! Heuristic: only sweep dimensions that actually move the needle for the
//! candle strategy — confidence threshold (per zone), z-score threshold
//! (per zone), edge threshold, and the maker/taker selector. Position
//! sizing knobs are independent and can be swept after the gate
//! parameters are tuned.

use crate::backtest::strategies::StrategyVariant;
use crate::strategy::decision::ZoneConfig;

#[derive(Debug, Clone)]
pub struct SweepGrid {
    /// Baseline variant — every sweep variant inherits everything not
    /// overridden in the grid below.
    pub base: StrategyVariant,
    /// Confidence thresholds applied to ALL zones (early/primary/late/
    /// terminal min_confidence). Keep small (≤8 values) — cartesian product.
    pub conf: Vec<f64>,
    /// Z-score thresholds applied to all zones simultaneously.
    pub z: Vec<f64>,
    /// Edge thresholds applied to all zones simultaneously.
    pub edge: Vec<f64>,
    /// EV-buffer values (negative disables the gate).
    pub ev_buffer: Vec<f64>,
    /// Whether to include a maker variant for each (conf, z, edge) cell.
    pub also_maker: bool,
}

impl SweepGrid {
    pub fn small_default(base: StrategyVariant) -> Self {
        Self {
            base,
            conf: vec![0.30, 0.40, 0.50, 0.60],
            z: vec![0.20, 0.50, 1.00],
            edge: vec![0.00, 0.03, 0.07],
            ev_buffer: vec![-1.0, 0.05],
            also_maker: true,
        }
    }

    /// Generate every (conf × z × edge × ev_buffer × {taker, maker?}) cell.
    /// Cartesian product can balloon — keep the grid small.
    pub fn variants(&self) -> Vec<StrategyVariant> {
        let maker_sides: Vec<bool> = if self.also_maker {
            vec![false, true]
        } else {
            vec![false]
        };
        let mut out = Vec::with_capacity(
            self.conf.len() * self.z.len() * self.edge.len() * self.ev_buffer.len() * maker_sides.len(),
        );
        for &conf in &self.conf {
            for &z in &self.z {
                for &edge in &self.edge {
                    for &ev in &self.ev_buffer {
                        for &maker in &maker_sides {
                            let mut cfg = ZoneConfig::default();
                            cfg.early_min_confidence = conf;
                            cfg.late_min_confidence = conf;
                            cfg.terminal_min_confidence = conf;
                            cfg.early_min_z = z;
                            cfg.primary_min_z = z;
                            cfg.late_min_z = z;
                            cfg.terminal_min_z = z;
                            cfg.early_min_edge = edge;
                            cfg.late_min_edge = edge;
                            cfg.terminal_min_edge = edge;
                            cfg.min_ev_buffer = ev;
                            let label = format!(
                                "c{:.2}_z{:.2}_e{:.2}_ev{:+.2}_{}",
                                conf,
                                z,
                                edge,
                                ev,
                                if maker { "mk" } else { "tk" }
                            );
                            out.push(StrategyVariant {
                                name: label,
                                zone_config: cfg,
                                skip_dead_zone: self.base.skip_dead_zone,
                                min_confidence: conf,
                                min_edge: edge,
                                position_pct: self.base.position_pct,
                                max_per_market_usd: self.base.max_per_market_usd,
                                prefer_maker: maker,
                                maker_fill_prob: self.base.maker_fill_prob,
                                maker_seed: self.base.maker_seed,
                                use_perfect_fill: false,
                                default_fee_rate: self.base.default_fee_rate,
                                maker_fee_rate: self.base.maker_fee_rate,
                            });
                        }
                    }
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_grid_has_expected_size() {
        let base = StrategyVariant::baseline();
        let grid = SweepGrid::small_default(base);
        let variants = grid.variants();
        // 4 conf × 3 z × 3 edge × 2 ev × 2 maker = 144
        assert_eq!(variants.len(), 4 * 3 * 3 * 2 * 2);
    }

    #[test]
    fn variants_have_unique_names() {
        let base = StrategyVariant::baseline();
        let grid = SweepGrid::small_default(base);
        let variants = grid.variants();
        let names: std::collections::HashSet<&str> = variants.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(names.len(), variants.len());
    }
}
