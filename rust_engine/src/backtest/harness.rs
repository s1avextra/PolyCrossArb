//! Backtest harness — full A/B runner over PMXT v2 hours.
//!
//! Pipeline per strategy variant:
//!   1. Reset L2 engine + book state
//!   2. Replay each requested PMXT v2 hour
//!   3. Apply momentum + decision on each book update against BTC tape
//!   4. Resolve fills against actual BTC outcomes
//!   5. Aggregate
//!
//! Outputs a per-variant `BacktestResults` you can format with
//! [`render_table`].

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::backtest::btc_history::BTCHistory;
use crate::backtest::fill_model::OneTickTaker;
use crate::backtest::l2_replay::{
    BacktestOrder, L2BacktestEngine, StaticLatencyConfig, Strategy, TokenBook,
};
use crate::backtest::pmxt::{L2Event, PMXTv2Loader};
use crate::backtest::resolver::{resolve_fills, BacktestResults, CandleWindow};
use crate::backtest::strategies::StrategyVariant;
use crate::data::scanner::CandleContract;
use crate::strategy::decision::{decide_candle_trade, CandleDecision, DecisionResult};
use crate::strategy::momentum::{MomentumConfig, MomentumDetector};

#[derive(Debug, Clone)]
pub struct CandleUniverse {
    pub contracts: Vec<CandleContract>,
}

impl CandleUniverse {
    pub fn token_set(&self) -> HashSet<String> {
        let mut s = HashSet::new();
        for c in &self.contracts {
            if !c.up_token_id.is_empty() {
                s.insert(c.up_token_id.clone());
            }
            if !c.down_token_id.is_empty() {
                s.insert(c.down_token_id.clone());
            }
        }
        s
    }

    pub fn condition_id_set(&self) -> HashSet<String> {
        self.contracts
            .iter()
            .map(|c| c.market.condition_id.clone())
            .collect()
    }

    pub fn windows(&self) -> Vec<CandleWindow> {
        self.contracts
            .iter()
            .map(|c| {
                let close = chrono::DateTime::parse_from_rfc3339(&c.end_date)
                    .ok()
                    .map(|d| d.timestamp() as f64)
                    .unwrap_or(0.0);
                let window_minutes = crate::live::window::estimate_window_minutes(&c.window_description);
                let window_minutes = if window_minutes > 0.0 { window_minutes } else { 60.0 };
                CandleWindow {
                    condition_id: c.market.condition_id.clone(),
                    open_ts_s: close - window_minutes * 60.0,
                    close_ts_s: close,
                }
            })
            .collect()
    }

    /// `token_id → CandleContract` lookup so the `BacktestStrategy` can
    /// resolve which contract owns each tick it sees.
    pub fn by_token_id(&self) -> BTreeMap<String, CandleContract> {
        let mut m = BTreeMap::new();
        for c in &self.contracts {
            if !c.up_token_id.is_empty() {
                m.insert(c.up_token_id.clone(), c.clone());
            }
            if !c.down_token_id.is_empty() {
                m.insert(c.down_token_id.clone(), c.clone());
            }
        }
        m
    }
}

/// Strategy adapter: glues the live decision logic onto the L2 backtest engine.
pub struct CandleBacktestStrategy {
    variant: StrategyVariant,
    universe_by_token: BTreeMap<String, CandleContract>,
    momentum: MomentumDetector,
    bankroll_usd: f64,
    btc_history: BTCHistory,
    pub decisions: Vec<CandleDecision>,
    /// Per-condition_id flag so we only enter once per market.
    traded: HashSet<String>,
    /// Last timestamp we fed into the detector — throttle add_tick to once
    /// per second to match live cadence (otherwise the 5k-tick deque rolls
    /// over in seconds at ~870 events/s and we lose window history).
    last_tick_ts_s: f64,
    // Diagnostic counters.
    pub events_seen: u64,
    pub events_for_known_token: u64,
    pub skipped_resolved: u64,
    pub skipped_too_early: u64,
    pub skipped_no_btc: u64,
    pub skipped_no_signal: u64,
    pub skipped_decision: u64,
    pub skipped_wrong_side: u64,
    pub skip_reasons: BTreeMap<String, u64>,
}

impl CandleBacktestStrategy {
    pub fn new(
        variant: StrategyVariant,
        universe: &CandleUniverse,
        bankroll_usd: f64,
        btc_history: BTCHistory,
    ) -> Self {
        let mom_cfg = MomentumConfig {
            noise_z_threshold: 0.3,
            ..Default::default()
        };
        Self {
            variant,
            universe_by_token: universe.by_token_id(),
            momentum: MomentumDetector::new(None, mom_cfg),
            bankroll_usd,
            btc_history,
            decisions: Vec::new(),
            traded: HashSet::new(),
            last_tick_ts_s: 0.0,
            events_seen: 0,
            events_for_known_token: 0,
            skipped_resolved: 0,
            skipped_too_early: 0,
            skipped_no_btc: 0,
            skipped_no_signal: 0,
            skipped_decision: 0,
            skipped_wrong_side: 0,
            skip_reasons: BTreeMap::new(),
        }
    }
}

impl Strategy for CandleBacktestStrategy {
    fn on_event(
        &mut self,
        timestamp_s: f64,
        token_id: &str,
        book: &TokenBook,
        _history: &BTreeMap<String, Vec<(f64, f64)>>,
    ) -> Vec<BacktestOrder> {
        self.events_seen += 1;
        let Some(contract) = self.universe_by_token.get(token_id).cloned() else {
            return Vec::new();
        };
        self.events_for_known_token += 1;
        let cid = contract.market.condition_id.clone();
        if self.traded.contains(&cid) {
            return Vec::new();
        }

        let close = chrono::DateTime::parse_from_rfc3339(&contract.end_date)
            .ok()
            .map(|d| d.timestamp() as f64)
            .unwrap_or(0.0);
        let parsed = crate::live::window::estimate_window_minutes(&contract.window_description);
        let window_minutes = if parsed > 0.0 { parsed } else { 60.0 };
        let open_ts_s = close - window_minutes * 60.0;
        let minutes_remaining = (close - timestamp_s) / 60.0;
        if minutes_remaining <= 0.083 || minutes_remaining > 30.0 {
            self.skipped_resolved += 1;
            return Vec::new();
        }
        let minutes_elapsed = window_minutes - minutes_remaining;
        if minutes_elapsed < 0.5 {
            self.skipped_too_early += 1;
            return Vec::new();
        }

        // Maintain BTC tick history for the momentum detector. Throttle to
        // 1 Hz — the live runtime adds one tick per cycle (~2 Hz) too.
        let btc = self.btc_history.price_at_seconds(timestamp_s);
        if btc <= 0.0 {
            self.skipped_no_btc += 1;
            return Vec::new();
        }
        if timestamp_s - self.last_tick_ts_s >= 1.0 {
            self.momentum.add_tick(btc, Some(timestamp_s));
            self.last_tick_ts_s = timestamp_s;
        }

        if self.momentum.get_open_price(&cid).is_none() {
            let open_btc = self.btc_history.price_at_seconds(open_ts_s);
            if open_btc <= 0.0 {
                self.skipped_no_btc += 1;
                return Vec::new();
            }
            self.momentum.set_window_open(&cid, open_btc);
        }

        let signal = match self.momentum.detect(
            &cid,
            minutes_elapsed,
            minutes_remaining,
            btc,
            Some(timestamp_s),
        ) {
            Some(s) => s,
            None => {
                self.skipped_no_signal += 1;
                return Vec::new();
            }
        };

        // Use the live book's current best ask for the up/down side prices.
        // This is conservative — the strategy entered on the same book the
        // live runtime would see.
        let up_price = if token_id == contract.up_token_id {
            book.best_ask
        } else {
            // up token's book hasn't ticked in this batch — fall back to
            // 1 - down_best_ask.
            (1.0 - book.best_ask).max(0.01)
        };
        let down_price = if token_id == contract.down_token_id {
            book.best_ask
        } else {
            (1.0 - book.best_ask).max(0.01)
        };

        let implied_vol = self.btc_history.realized_vol_at((timestamp_s * 1000.0) as i64, 3600.0);
        let res = decide_candle_trade(
            &signal,
            minutes_elapsed,
            minutes_remaining,
            window_minutes,
            up_price,
            down_price,
            btc,
            signal.open_price,
            implied_vol,
            self.variant.min_confidence,
            self.variant.min_edge,
            self.variant.skip_dead_zone,
            &self.variant.zone_config,
            0.0,
        );
        let decision = match res {
            DecisionResult::Trade(d) => d,
            DecisionResult::Skip(skip) => {
                self.skipped_decision += 1;
                let key = format!("{}_{}", skip.reason, skip.zone);
                *self.skip_reasons.entry(key).or_insert(0) += 1;
                return Vec::new();
            }
        };

        let traded_token = if decision.direction == "up" {
            contract.up_token_id.clone()
        } else {
            contract.down_token_id.clone()
        };
        if traded_token != token_id {
            self.skipped_wrong_side += 1;
            return Vec::new();
        }
        self.traded.insert(cid.clone());
        self.decisions.push(decision.clone());

        let position = (self.bankroll_usd * self.variant.position_pct).min(self.variant.max_per_market_usd);
        let market_price = decision.market_price;
        if market_price <= 0.0 {
            return Vec::new();
        }
        let size = (position / market_price).round().max(1.0);

        vec![BacktestOrder {
            timestamp_s,
            condition_id: cid,
            token_id: traded_token,
            side: "buy".into(),
            size,
            order_type: "market".into(),
            limit_price: None,
            fee_rate: self.variant.default_fee_rate,
            maker_fee_rate: 0.0,
        }]
    }
}

#[derive(Debug, Clone)]
pub struct HarnessRun {
    pub variant: StrategyVariant,
    pub results: BacktestResults,
}

pub struct HarnessConfig {
    pub hours: Vec<DateTime<Utc>>,
    pub universe: CandleUniverse,
    pub btc_history: BTCHistory,
    pub bankroll_usd: f64,
    pub cache_dir: PathBuf,
    pub latency: StaticLatencyConfig,
}

/// Run every variant over the requested hours. Loads PMXT data once per
/// hour from the cache (or downloads it if missing).
pub async fn run_harness(
    cfg: &HarnessConfig,
    variants: &[StrategyVariant],
) -> Result<Vec<HarnessRun>> {
    let loader = PMXTv2Loader::new(&cfg.cache_dir);
    let token_filter = cfg.universe.condition_id_set();

    // Pre-load all events once and reuse across variants.
    let mut events: Vec<L2Event> = Vec::new();
    for &h in &cfg.hours {
        loader.download_hour(h, false).await?;
        let evs = loader.load_cached_hour(h, Some(&token_filter))?;
        events.extend(evs);
    }
    events.sort_by(|a, b| a.timestamp_s.partial_cmp(&b.timestamp_s).unwrap_or(std::cmp::Ordering::Equal));
    tracing::info!(
        events = events.len(),
        cids = token_filter.len(),
        "L2 events loaded for universe",
    );

    let windows = cfg.universe.windows();

    let mut runs = Vec::with_capacity(variants.len());
    for v in variants {
        let mut engine = L2BacktestEngine::new(OneTickTaker::default(), cfg.latency);
        let mut strategy = CandleBacktestStrategy::new(
            v.clone(),
            &cfg.universe,
            cfg.bankroll_usd,
            cfg.btc_history.clone(),
        );
        engine.replay(events.iter().cloned(), &mut strategy, v.default_fee_rate);
        let mut top_skips: Vec<(&String, &u64)> = strategy.skip_reasons.iter().collect();
        top_skips.sort_by(|a, b| b.1.cmp(a.1));
        let top: Vec<String> = top_skips
            .iter()
            .take(5)
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        tracing::info!(
            variant = %v.name,
            events_seen = strategy.events_seen,
            events_for_known_token = strategy.events_for_known_token,
            skipped_resolved = strategy.skipped_resolved,
            skipped_too_early = strategy.skipped_too_early,
            skipped_no_btc = strategy.skipped_no_btc,
            skipped_no_signal = strategy.skipped_no_signal,
            skipped_decision = strategy.skipped_decision,
            skipped_wrong_side = strategy.skipped_wrong_side,
            top_skips = top.join(" | "),
            "strategy diagnostic",
        );
        let decisions = strategy.decisions.clone();
        let res = resolve_fills(&engine.fills, &decisions, &windows, &cfg.btc_history);
        runs.push(HarnessRun { variant: v.clone(), results: res });
    }
    Ok(runs)
}

pub fn render_table(runs: &[HarnessRun]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    writeln!(
        &mut out,
        "{:<24} {:>7} {:>7} {:>7} {:>7} {:>9} {:>11} {:>9}",
        "variant", "trades", "wins", "losses", "WR%", "PnL", "PnL/trade", "fees"
    )
    .unwrap();
    writeln!(&mut out, "{}", "─".repeat(86)).unwrap();
    let mut sorted = runs.to_vec();
    sorted.sort_by(|a, b| b.results.total_pnl().partial_cmp(&a.results.total_pnl()).unwrap_or(std::cmp::Ordering::Equal));
    for r in &sorted {
        writeln!(
            &mut out,
            "{:<24} {:>7} {:>7} {:>7} {:>6.1}% {:>+8.2} {:>+10.3} {:>9.4}",
            r.variant.name,
            r.results.n_trades(),
            r.results.n_wins(),
            r.results.n_losses(),
            100.0 * r.results.win_rate(),
            r.results.total_pnl(),
            r.results.avg_pnl(),
            r.results.total_fees(),
        )
        .unwrap();
    }
    out
}
