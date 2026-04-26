//! Replay engine: walk the captured evaluations per cid, find the first that
//! "would have traded" under a strategy, look up the resolution, compute
//! synthetic P&L.

use std::collections::HashMap;

use serde_json::Value;

use crate::execution::fees::polymarket_fee;
use crate::live::paper_fill::{simulate_paper_fill, PaperFillCfg};
use crate::strategy::decision::{decide_candle_trade, DecisionResult};
use crate::strategy::momentum::MomentumSignal;
use crate::sweep::strategy::Strategy;
use crate::sweep::SweepRun;

#[derive(Debug, Clone)]
pub struct EvaluationRow {
    pub ts_ms: i64,
    pub cid: String,
    pub asset: String,
    pub direction: String,
    pub confidence: f64,
    pub z_score: f64,
    pub consistency: f64,
    pub price_change: f64,
    pub price_change_pct: f64,
    pub minutes_elapsed: f64,
    pub minutes_remaining: f64,
    pub current_price: f64,
    pub open_price: f64,
    pub up_price: f64,
    pub down_price: f64,
    pub implied_vol: f64,
    pub cross_boost: f64,
}

impl EvaluationRow {
    pub fn from_json(v: &Value) -> Option<Self> {
        Some(Self {
            ts_ms: v.get("ts_ms")?.as_i64()?,
            cid: v.get("cid")?.as_str()?.to_string(),
            asset: v.get("asset").and_then(|x| x.as_str()).unwrap_or("BTC").to_string(),
            direction: v.get("dir")?.as_str()?.to_string(),
            confidence: v.get("conf")?.as_f64()?,
            z_score: v.get("z")?.as_f64()?,
            consistency: v.get("cons").and_then(|x| x.as_f64()).unwrap_or(0.0),
            price_change: v.get("chg").and_then(|x| x.as_f64()).unwrap_or(0.0),
            price_change_pct: v.get("chg_pct").and_then(|x| x.as_f64()).unwrap_or(0.0),
            minutes_elapsed: v.get("elapsed_min")?.as_f64()?,
            minutes_remaining: v.get("remaining_min")?.as_f64()?,
            current_price: v.get("px").and_then(|x| x.as_f64()).unwrap_or(0.0),
            open_price: v.get("open").and_then(|x| x.as_f64()).unwrap_or(0.0),
            up_price: v.get("up_price").and_then(|x| x.as_f64()).unwrap_or(0.5),
            down_price: v.get("down_price").and_then(|x| x.as_f64()).unwrap_or(0.5),
            implied_vol: v.get("implied_vol").and_then(|x| x.as_f64()).unwrap_or(0.5),
            cross_boost: v.get("cross_boost").and_then(|x| x.as_f64()).unwrap_or(0.0),
        })
    }

    fn window_minutes(&self) -> f64 {
        self.minutes_elapsed + self.minutes_remaining
    }

    fn to_signal(&self) -> MomentumSignal {
        MomentumSignal {
            direction: self.direction.clone(),
            confidence: self.confidence,
            price_change: self.price_change,
            price_change_pct: self.price_change_pct,
            consistency: self.consistency,
            minutes_elapsed: self.minutes_elapsed,
            minutes_remaining: self.minutes_remaining,
            current_price: self.current_price,
            open_price: self.open_price,
            z_score: self.z_score,
            reversion_count: 0,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // `predicted`, `won`, `open_btc`, `close_btc` are kept for
                    // future enrichment (e.g., scoring against a different
                    // resolver) but the sweep currently only reads `actual`.
pub struct ResolutionRow {
    pub cid: String,
    pub predicted: String,
    pub actual: String,
    pub won: bool,
    pub open_btc: f64,
    pub close_btc: f64,
}

impl ResolutionRow {
    pub fn from_json(v: &Value) -> Option<Self> {
        Some(Self {
            cid: v.get("cid")?.as_str()?.to_string(),
            predicted: v.get("predicted").and_then(|x| x.as_str()).unwrap_or("up").to_string(),
            actual: v.get("actual").and_then(|x| x.as_str()).unwrap_or("up").to_string(),
            won: v.get("won").and_then(|x| x.as_bool()).unwrap_or(false),
            open_btc: v.get("open_btc").and_then(|x| x.as_f64()).unwrap_or(0.0),
            close_btc: v.get("close_btc").and_then(|x| x.as_f64()).unwrap_or(0.0),
        })
    }
}

pub fn run_strategy(
    evaluations: &[EvaluationRow],
    resolutions: &[ResolutionRow],
    strat: &Strategy,
    bankroll_usd: f64,
) -> SweepRun {
    // Group evaluations by cid in time order.
    let mut by_cid: HashMap<String, Vec<&EvaluationRow>> = HashMap::new();
    for e in evaluations {
        by_cid.entry(e.cid.clone()).or_default().push(e);
    }
    for v in by_cid.values_mut() {
        v.sort_by_key(|e| e.ts_ms);
    }

    let resolutions_by_cid: HashMap<String, &ResolutionRow> =
        resolutions.iter().map(|r| (r.cid.clone(), r)).collect();

    let fill_cfg = PaperFillCfg {
        prefer_maker: strat.prefer_maker,
        ..Default::default()
    };

    let mut run = SweepRun {
        strategy_name: strat.name.clone(),
        ..Default::default()
    };
    let mut total_entry_price = 0.0;

    for (cid, evals) in by_cid.iter() {
        // Find the first evaluation that would trade under this strategy.
        let mut hit: Option<(&EvaluationRow, String)> = None;
        for e in evals {
            let signal = e.to_signal();
            let res = decide_candle_trade(
                &signal,
                e.minutes_elapsed,
                e.minutes_remaining,
                e.window_minutes(),
                e.up_price,
                e.down_price,
                e.current_price,
                e.open_price,
                e.implied_vol,
                strat.min_confidence,
                strat.min_edge,
                strat.skip_dead_zone,
                &strat.zone_config,
                e.cross_boost,
            );
            if let DecisionResult::Trade(decision) = res {
                hit = Some((e, decision.zone));
                break;
            }
        }
        let Some((entry, zone)) = hit else { continue };

        let Some(resolution) = resolutions_by_cid.get(cid) else {
            // Hypothetical trade with no recorded resolution — skip
            // (would have happened, but the JSONL window doesn't cover the
            // resolution event, so we can't fairly score it).
            continue;
        };

        // Position sizing — same shape as the live pipeline.
        let position_pct = 0.10_f64;
        let max_per_market = 20.0_f64;
        let position = (bankroll_usd * position_pct).min(max_per_market);
        let market_price = if entry.direction == "up" {
            entry.up_price
        } else {
            entry.down_price
        };
        let Some(fill) = simulate_paper_fill(market_price, position, &fill_cfg) else {
            continue;
        };

        let won = entry.direction == resolution.actual;
        let pnl = if won {
            (1.0 - fill.fill_price) * fill.shares - fill.fee
        } else {
            -fill.fill_price * fill.shares - fill.fee
        };

        // Aggregate
        run.trades += 1;
        if won {
            run.wins += 1;
        } else {
            run.losses += 1;
        }
        run.realized_pnl += pnl;
        run.total_fees += fill.fee;
        total_entry_price += fill.fill_price;

        let zone_stats = run.by_zone.entry(zone).or_default();
        zone_stats.trades += 1;
        zone_stats.pnl += pnl;
        if won {
            zone_stats.wins += 1;
        } else {
            zone_stats.losses += 1;
        }

        let asset_stats = run.by_asset.entry(entry.asset.clone()).or_default();
        asset_stats.trades += 1;
        asset_stats.pnl += pnl;
        if won {
            asset_stats.wins += 1;
        } else {
            asset_stats.losses += 1;
        }
    }

    if run.trades > 0 {
        run.avg_entry_price = total_entry_price / run.trades as f64;
    }

    // Compute polymarket_fee total again for sanity (already accumulated).
    // No-op; left here for reference if we change the fill model.
    let _ = polymarket_fee;
    run
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_eval(cid: &str, ts_ms: i64, conf: f64, z: f64, dir: &str) -> EvaluationRow {
        EvaluationRow {
            ts_ms,
            cid: cid.into(),
            asset: "BTC".into(),
            direction: dir.into(),
            confidence: conf,
            z_score: z,
            consistency: 0.8,
            price_change: 100.0,
            price_change_pct: 0.001,
            minutes_elapsed: 4.95,
            minutes_remaining: 0.05,
            current_price: 70_500.0,
            open_price: 70_000.0,
            up_price: 0.30,
            down_price: 0.70,
            implied_vol: 0.5,
            cross_boost: 0.0,
        }
    }

    fn mk_res(cid: &str, actual: &str) -> ResolutionRow {
        ResolutionRow {
            cid: cid.into(),
            predicted: "up".into(),
            actual: actual.into(),
            won: actual == "up",
            open_btc: 70_000.0,
            close_btc: 70_500.0,
        }
    }

    #[test]
    fn empty_inputs_produce_zero_trades() {
        let strat = crate::sweep::strategy::baseline();
        let run = run_strategy(&[], &[], &strat, 100.0);
        assert_eq!(run.trades, 0);
    }

    #[test]
    fn positive_terminal_trade_records_a_win() {
        let evals = vec![mk_eval("c1", 1, 0.75, 2.0, "up")];
        let res = vec![mk_res("c1", "up")];
        let strat = crate::sweep::strategy::terminal_only();
        let run = run_strategy(&evals, &res, &strat, 100.0);
        assert_eq!(run.trades, 1);
        assert_eq!(run.wins, 1);
        assert!(run.realized_pnl > 0.0);
    }

    #[test]
    fn evaluation_without_resolution_is_skipped() {
        // Trade fires but no resolution recorded — sweep skips (no ground truth).
        let evals = vec![mk_eval("missing", 1, 0.75, 2.0, "up")];
        let res = vec![];
        let strat = crate::sweep::strategy::terminal_only();
        let run = run_strategy(&evals, &res, &strat, 100.0);
        assert_eq!(run.trades, 0);
    }

    #[test]
    fn losing_trade_penalty() {
        let evals = vec![mk_eval("c1", 1, 0.75, 2.0, "up")];
        let res = vec![mk_res("c1", "down")]; // we predicted up, actual down → loss
        let strat = crate::sweep::strategy::terminal_only();
        let run = run_strategy(&evals, &res, &strat, 100.0);
        assert_eq!(run.trades, 1);
        assert_eq!(run.losses, 1);
        assert!(run.realized_pnl < 0.0);
    }
}
