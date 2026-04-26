//! Resolve backtest fills against actual BTC outcomes.
//!
//! After `L2BacktestEngine` runs, each fill is a hypothetical entry in some
//! candle window. This module pairs each fill with the BTC open/close prices
//! at that window and computes realized P&L.

use std::collections::BTreeMap;

use crate::backtest::btc_history::BTCHistory;
use crate::backtest::l2_replay::BacktestFill;
use crate::strategy::decision::CandleDecision;

#[derive(Debug, Clone)]
pub struct ResolvedTrade {
    pub fill: BacktestFill,
    pub decision: CandleDecision,
    pub open_btc: f64,
    pub close_btc: f64,
    pub actual_direction: String,
    pub won: bool,
    /// Realized P&L *before* fees: (1 - fill_price) * size on win, -fill_price * size on loss.
    pub pnl: f64,
    /// Realized P&L net of fees.
    pub pnl_after_fee: f64,
}

impl ResolvedTrade {
    pub fn predicted_direction(&self) -> &str {
        &self.decision.direction
    }
}

#[derive(Debug, Default, Clone)]
pub struct BacktestResults {
    pub trades: Vec<ResolvedTrade>,
    pub unresolved_fills: Vec<BacktestFill>,
}

impl BacktestResults {
    pub fn n_trades(&self) -> usize {
        self.trades.len()
    }

    pub fn n_wins(&self) -> usize {
        self.trades.iter().filter(|t| t.won).count()
    }

    pub fn n_losses(&self) -> usize {
        self.trades.len() - self.n_wins()
    }

    pub fn win_rate(&self) -> f64 {
        if self.trades.is_empty() {
            0.0
        } else {
            self.n_wins() as f64 / self.trades.len() as f64
        }
    }

    pub fn total_pnl(&self) -> f64 {
        self.trades.iter().map(|t| t.pnl_after_fee).sum()
    }

    pub fn total_fees(&self) -> f64 {
        self.trades.iter().map(|t| t.fill.fee).sum()
    }

    pub fn avg_pnl(&self) -> f64 {
        if self.trades.is_empty() {
            0.0
        } else {
            self.total_pnl() / self.trades.len() as f64
        }
    }

    pub fn sharpe(&self) -> f64 {
        if self.trades.len() < 2 {
            return 0.0;
        }
        let pnls: Vec<f64> = self.trades.iter().map(|t| t.pnl_after_fee).collect();
        let mean = pnls.iter().sum::<f64>() / pnls.len() as f64;
        let var = pnls.iter().map(|p| (p - mean).powi(2)).sum::<f64>() / pnls.len() as f64;
        let std = var.sqrt();
        if std > 0.0 { mean / std } else { 0.0 }
    }

    pub fn by_zone(&self) -> BTreeMap<String, ZoneBucket> {
        let mut out: BTreeMap<String, ZoneBucket> = BTreeMap::new();
        for t in &self.trades {
            let bucket = out.entry(t.decision.zone.clone()).or_default();
            bucket.add(t);
        }
        out
    }
}

#[derive(Debug, Default, Clone)]
pub struct ZoneBucket {
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub pnl: f64,
}

impl ZoneBucket {
    fn add(&mut self, t: &ResolvedTrade) {
        self.trades += 1;
        if t.won {
            self.wins += 1;
        } else {
            self.losses += 1;
        }
        self.pnl += t.pnl_after_fee;
    }

    pub fn win_rate(&self) -> f64 {
        let r = self.wins + self.losses;
        if r == 0 { 0.0 } else { self.wins as f64 / r as f64 }
    }
}

/// Window descriptor used by the resolver to look up BTC prices.
#[derive(Debug, Clone)]
pub struct CandleWindow {
    pub condition_id: String,
    /// Start of the window in seconds since epoch.
    pub open_ts_s: f64,
    /// Close of the window in seconds since epoch.
    pub close_ts_s: f64,
}

/// Resolve a list of fills + decisions against the BTC tape. Each fill must
/// carry a `condition_id` (via `BacktestFill.order.condition_id`) that can be
/// looked up in `windows`.
pub fn resolve_fills(
    fills: &[BacktestFill],
    decisions: &[CandleDecision],
    windows: &[CandleWindow],
    btc_history: &BTCHistory,
) -> BacktestResults {
    assert_eq!(
        fills.len(),
        decisions.len(),
        "resolve_fills: fills and decisions must align 1:1"
    );

    let window_by_cid: BTreeMap<String, &CandleWindow> = windows
        .iter()
        .map(|w| (w.condition_id.clone(), w))
        .collect();

    let mut results = BacktestResults::default();
    for (fill, decision) in fills.iter().zip(decisions) {
        if !fill.success {
            results.unresolved_fills.push(fill.clone());
            continue;
        }
        let cid = match fill.order.condition_id.as_str() {
            "" => {
                results.unresolved_fills.push(fill.clone());
                continue;
            }
            c => c,
        };
        let Some(window) = window_by_cid.get(cid) else {
            results.unresolved_fills.push(fill.clone());
            continue;
        };

        let open_btc = btc_history.price_at_seconds(window.open_ts_s);
        let close_btc = btc_history.price_at_seconds(window.close_ts_s);
        if open_btc <= 0.0 || close_btc <= 0.0 {
            results.unresolved_fills.push(fill.clone());
            continue;
        }

        let actual = if close_btc >= open_btc { "up" } else { "down" };
        let won = decision.direction == actual;
        let pnl = if won {
            (1.0 - fill.fill_price) * fill.filled_size
        } else {
            -fill.fill_price * fill.filled_size
        };
        let pnl_after_fee = pnl - fill.fee;

        results.trades.push(ResolvedTrade {
            fill: fill.clone(),
            decision: decision.clone(),
            open_btc,
            close_btc,
            actual_direction: actual.to_string(),
            won,
            pnl,
            pnl_after_fee,
        });
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::l2_replay::{BacktestFill, BacktestOrder};

    fn mk_history() -> BTCHistory {
        let mut h = BTCHistory::default();
        // Build a tick stream from 70_000 to 70_100 over 600 s.
        for i in 0..=600 {
            h.timestamps_ms.push(1_700_000_000_000 + i * 1000);
            h.prices.push(70_000.0 + (i as f64 / 600.0) * 100.0);
        }
        h
    }

    fn mk_decision(direction: &str) -> CandleDecision {
        CandleDecision {
            direction: direction.to_string(),
            confidence: 0.7,
            z_score: 1.5,
            zone: "terminal".to_string(),
            fair_value: 0.6,
            market_price: 0.4,
            edge: 0.2,
            minutes_remaining: 0.05,
            yes_no_vig: 0.0,
        }
    }

    fn mk_fill(cid: &str, fill_price: f64, size: f64, fee: f64) -> BacktestFill {
        BacktestFill {
            order: BacktestOrder {
                timestamp_s: 1_700_000_000.0,
                condition_id: cid.to_string(),
                token_id: "tok".to_string(),
                side: "buy".to_string(),
                size,
                order_type: "market".to_string(),
                limit_price: None,
                fee_rate: 0.072,
                maker_fee_rate: 0.0,
            },
            fill_timestamp_s: 1_700_000_001.0,
            fill_price,
            filled_size: size,
            cost: fill_price * size,
            fee,
            slippage: 0.01,
            book_age_ms: 0.0,
            success: true,
            reason: "".to_string(),
        }
    }

    #[test]
    fn resolves_winning_up_trade() {
        let h = mk_history();
        let windows = vec![CandleWindow {
            condition_id: "c1".into(),
            open_ts_s: 1_700_000_000.0,
            close_ts_s: 1_700_000_300.0,
        }];
        let fills = vec![mk_fill("c1", 0.40, 10.0, 0.10)];
        let decisions = vec![mk_decision("up")];
        let res = resolve_fills(&fills, &decisions, &windows, &h);
        assert_eq!(res.n_trades(), 1);
        let t = &res.trades[0];
        assert!(t.won);
        // pnl = (1 - 0.40) * 10 = 6.0; minus fee 0.10 = 5.90
        assert!((t.pnl_after_fee - 5.9).abs() < 1e-9);
    }

    #[test]
    fn resolves_losing_down_trade() {
        let h = mk_history();
        let windows = vec![CandleWindow {
            condition_id: "c1".into(),
            open_ts_s: 1_700_000_000.0,
            close_ts_s: 1_700_000_300.0,
        }];
        let fills = vec![mk_fill("c1", 0.40, 10.0, 0.10)];
        let decisions = vec![mk_decision("down")]; // BTC went up, we predicted down → loss
        let res = resolve_fills(&fills, &decisions, &windows, &h);
        assert_eq!(res.n_trades(), 1);
        let t = &res.trades[0];
        assert!(!t.won);
        // pnl = -0.40 * 10 = -4.0; minus fee 0.10 = -4.10
        assert!((t.pnl_after_fee + 4.1).abs() < 1e-9);
    }

    #[test]
    fn unknown_window_marks_fill_unresolved() {
        let h = mk_history();
        let fills = vec![mk_fill("missing", 0.40, 10.0, 0.10)];
        let decisions = vec![mk_decision("up")];
        let res = resolve_fills(&fills, &decisions, &[], &h);
        assert_eq!(res.n_trades(), 0);
        assert_eq!(res.unresolved_fills.len(), 1);
    }
}
