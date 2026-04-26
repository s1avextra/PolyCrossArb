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
use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use rayon::prelude::*;

use crate::backtest::btc_history::BTCHistory;
use crate::backtest::fill_model::{Maker, OneTickTaker, Perfect};
use crate::backtest::l2_replay::{
    BacktestOrder, FillModel, L2BacktestEngine, StaticLatencyConfig, Strategy, TokenBook,
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
    btc_history: Arc<BTCHistory>,
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
        btc_history: Arc<BTCHistory>,
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
            maker_fee_rate: self.variant.maker_fee_rate,
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
    pub btc_history: Arc<BTCHistory>,
    pub bankroll_usd: f64,
    pub cache_dir: PathBuf,
    pub latency: StaticLatencyConfig,
    /// Optional shared distilled-cache directory. When set, the harness
    /// checks `<dir>/<hour>.v1.candles.jsonl.gz` BEFORE the per-tenant
    /// sidecar and the parquet. The shared-cache writer is `polymomentum-
    /// engine distill`. See cross_bot_distilled_cache_response.md.
    pub shared_distilled_dir: Option<PathBuf>,
    /// Variant-fan-out parallelism. `None` → use rayon's global pool
    /// (defaults to `num_cpus`). `Some(1)` → serial (matches the
    /// pre-rayon behavior bit-for-bit). `Some(n>1)` → cap at `n`.
    /// Honors `RAYON_NUM_THREADS` env var when this is `None`.
    pub threads: Option<usize>,
}

/// Run every variant over the requested hours. Streams one hour at a time
/// (the parquet expansion is huge — ~500 MB / hour in memory). For each
/// hour, all variants replay in parallel against a shared `Arc<Vec<L2Event>>`
/// — variant-fan-out is the natural unit of parallelism since each variant
/// is independent (its own engine, strategy, RNG seed). Per-variant
/// `BacktestResults` are then merged sequentially.
///
/// Determinism: each variant has its own `maker_seed`, so results are
/// independent of thread count. Output `runs` Vec is in the same order as
/// the input `variants` Vec regardless of thread count (rayon's
/// `par_iter().map().collect()` preserves source order).
pub async fn run_harness(
    cfg: &HarnessConfig,
    variants: &[StrategyVariant],
) -> Result<Vec<HarnessRun>> {
    let loader = PMXTv2Loader::new(&cfg.cache_dir);
    let token_filter = cfg.universe.condition_id_set();
    let windows = cfg.universe.windows();

    // Optional bounded rayon pool. `None` → use the global pool (which respects
    // `RAYON_NUM_THREADS` env var; default is num_cpus). `Some(n)` → build a
    // local pool with exactly n threads.
    let local_pool = match cfg.threads {
        Some(n) if n > 0 => Some(
            rayon::ThreadPoolBuilder::new()
                .num_threads(n)
                .thread_name(|i| format!("harness-{i}"))
                .build()
                .map_err(|e| anyhow::anyhow!("rayon ThreadPoolBuilder: {e}"))?,
        ),
        _ => None,
    };
    let effective_threads = local_pool
        .as_ref()
        .map(|p| p.current_num_threads())
        .unwrap_or_else(rayon::current_num_threads);
    tracing::info!(
        variants = variants.len(),
        threads = effective_threads,
        hours = cfg.hours.len(),
        "harness starting parallel variant fan-out",
    );

    // Per-variant accumulator (merged sequentially after each hour's parallel
    // block). Index-aligned with `variants`.
    let mut variant_state: Vec<BacktestResults> =
        (0..variants.len()).map(|_| BacktestResults::default()).collect();

    for &h in &cfg.hours {
        loader.download_hour(h, false).await?;
        let load_t0 = std::time::Instant::now();

        // Reader fallback chain: shared distilled → per-tenant sidecar → parquet.
        let mut events_vec: Vec<L2Event> = Vec::new();
        let mut source = "parquet";
        if let Some(shared_dir) = &cfg.shared_distilled_dir {
            let path = crate::backtest::distill::shared_cache_path_for_hour(shared_dir, h);
            if path.exists() {
                match crate::backtest::distill::read_distilled(&path) {
                    Ok((mut shared_events, _)) => {
                        shared_events.retain(|e| token_filter.contains(&e.market_id));
                        events_vec = shared_events;
                        source = "shared_distilled";
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, ?path, "shared distilled cache unreadable; falling back");
                    }
                }
            }
        }
        if events_vec.is_empty() {
            events_vec = loader.load_with_sidecar(h, &token_filter)?;
        }
        events_vec.sort_by(|a, b| {
            a.timestamp_s
                .partial_cmp(&b.timestamp_s)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        // Wrap in Arc so all variant tasks share the same buffer (no per-task
        // copy of the ~16 MB event vector).
        let events: Arc<Vec<L2Event>> = Arc::new(events_vec);
        tracing::info!(
            hour = %h,
            events = events.len(),
            cids = token_filter.len(),
            elapsed_ms = load_t0.elapsed().as_millis() as u64,
            source,
            "L2 events loaded",
        );

        // Run every variant against this hour's events in parallel. The closure
        // captures &cfg / &windows by shared reference (rayon guarantees the
        // borrow outlives the parallel scope), and clones the cheap stuff
        // (StrategyVariant, Arc<BTCHistory>, Arc<Vec<L2Event>>).
        let replay_t0 = std::time::Instant::now();
        let run = |v: &StrategyVariant| -> BacktestResults {
            let fm = build_fill_model(v);
            let mut engine = L2BacktestEngine::new(fm, cfg.latency);
            let mut strategy = CandleBacktestStrategy::new(
                v.clone(),
                &cfg.universe,
                cfg.bankroll_usd,
                Arc::clone(&cfg.btc_history),
            );
            engine.replay(events.iter().cloned(), &mut strategy, v.default_fee_rate);

            let mut top_skips: Vec<(&String, &u64)> = strategy.skip_reasons.iter().collect();
            top_skips.sort_by(|a, b| b.1.cmp(a.1));
            let top: Vec<String> = top_skips
                .iter()
                .take(5)
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            tracing::debug!(
                variant = %v.name,
                hour = %h,
                events_seen = strategy.events_seen,
                skipped_decision = strategy.skipped_decision,
                top_skips = top.join(" | "),
                "strategy diagnostic",
            );

            let decisions = strategy.decisions;
            resolve_fills(&engine.fills, &decisions, &windows, &cfg.btc_history)
        };
        let per_variant: Vec<BacktestResults> = if let Some(pool) = &local_pool {
            pool.install(|| variants.par_iter().map(run).collect())
        } else {
            variants.par_iter().map(run).collect()
        };

        // Merge sequentially. Index-aligned with `variants`, so this preserves
        // the input order regardless of thread count.
        for (acc, hour_res) in variant_state.iter_mut().zip(per_variant) {
            acc.trades.extend(hour_res.trades);
            acc.unresolved_fills.extend(hour_res.unresolved_fills);
        }
        tracing::info!(
            hour = %h,
            replay_ms = replay_t0.elapsed().as_millis() as u64,
            variants = variants.len(),
            threads = effective_threads,
            "variants replayed",
        );
    }

    Ok(variants
        .iter()
        .cloned()
        .zip(variant_state)
        .map(|(variant, results)| HarnessRun { variant, results })
        .collect())
}

/// Build the engine's fill model from a strategy variant. `prefer_maker` →
/// probabilistic Maker (with taker fallback); otherwise OneTickTaker.
/// Perfect / BookWalk are reserved for future variants.
fn build_fill_model(v: &StrategyVariant) -> FillModel {
    if v.prefer_maker {
        FillModel::Maker(Maker::new(
            v.maker_fill_prob,
            crate::backtest::fill_model::DEFAULT_TICK,
            v.maker_seed,
        ))
    } else if v.use_perfect_fill {
        FillModel::Perfect(Perfect)
    } else {
        FillModel::OneTickTaker(OneTickTaker::default())
    }
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

pub fn render_zone_breakdown(runs: &[HarnessRun]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let mut sorted = runs.to_vec();
    sorted.sort_by(|a, b| b.results.total_pnl().partial_cmp(&a.results.total_pnl()).unwrap_or(std::cmp::Ordering::Equal));
    for r in &sorted {
        let zones = r.results.by_zone();
        if zones.is_empty() {
            continue;
        }
        writeln!(&mut out, "\n{} — by zone", r.variant.name).unwrap();
        writeln!(
            &mut out,
            "  {:<10} {:>7} {:>7} {:>7} {:>7} {:>9}",
            "zone", "trades", "wins", "losses", "WR%", "PnL"
        )
        .unwrap();
        for (zone, stats) in &zones {
            writeln!(
                &mut out,
                "  {:<10} {:>7} {:>7} {:>7} {:>6.1}% {:>+8.2}",
                zone,
                stats.trades,
                stats.wins,
                stats.losses,
                100.0 * stats.win_rate(),
                stats.pnl
            )
            .unwrap();
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::strategies::StrategyVariant;
    use crate::data::models::{Market, Outcome};
    use crate::data::scanner::CandleContract;

    /// Build a tiny synthetic universe + history for the parallel-vs-serial
    /// determinism test. We don't need the harness to find any trades — we
    /// just need it to run end-to-end and produce per-variant results we can
    /// compare across thread counts.
    fn synthetic_cfg() -> (HarnessConfig, Vec<StrategyVariant>) {
        let contract = CandleContract {
            market: Market {
                condition_id: "0xabc".into(),
                question: "BTC Up or Down - test".into(),
                end_date: "2026-04-26T08:30:00Z".into(),
                outcomes: vec![
                    Outcome { name: "Up".into(), price: 0.5, token_id: "1".into() },
                    Outcome { name: "Down".into(), price: 0.5, token_id: "2".into() },
                ],
                ..Default::default()
            },
            asset: "BTC".into(),
            window_description: "8:00AM-8:30AM ET".into(),
            up_token_id: "1".into(),
            down_token_id: "2".into(),
            end_date: "2026-04-26T08:30:00Z".into(),
            hours_left: 0.0,
            up_price: 0.5,
            down_price: 0.5,
            volume: 0.0,
            liquidity: 0.0,
        };
        let universe = CandleUniverse { contracts: vec![contract] };

        let mut btc = BTCHistory::default();
        // 60 evenly spaced 1-second ticks around the synthetic window.
        let base_ms = 1745654400000_i64; // 2026-04-26T08:00:00Z
        for i in 0..1800 {
            btc.timestamps_ms.push(base_ms + i * 1000);
            btc.prices.push(50000.0 + (i as f64).sin() * 10.0);
        }

        let variants = vec![
            StrategyVariant::baseline(),
            StrategyVariant::loose_smoke(),
            StrategyVariant::loose_maker(),
        ];

        let cfg = HarnessConfig {
            hours: vec![],  // empty hours -> the loop is a no-op, but the parallel
                            // setup code still runs (pool build, universe prep).
            universe,
            btc_history: Arc::new(btc),
            bankroll_usd: 100.0,
            cache_dir: PathBuf::from("/tmp"),
            latency: StaticLatencyConfig::default(),
            shared_distilled_dir: None,
            threads: None,
        };
        (cfg, variants)
    }

    #[tokio::test]
    async fn empty_hours_returns_empty_state_per_variant() {
        let (cfg, variants) = synthetic_cfg();
        let runs = run_harness(&cfg, &variants).await.unwrap();
        assert_eq!(runs.len(), variants.len());
        // Order preserved: variant[i] in == variant[i] out.
        for (run, v) in runs.iter().zip(&variants) {
            assert_eq!(run.variant.name, v.name);
            assert_eq!(run.results.n_trades(), 0);
        }
    }

    #[tokio::test]
    async fn thread_count_does_not_change_output_order() {
        // Same synthetic cfg; verify result order is variant-stable for both
        // serial (threads=1) and parallel (threads=4).
        let (mut cfg, variants) = synthetic_cfg();
        cfg.threads = Some(1);
        let serial = run_harness(&cfg, &variants).await.unwrap();
        cfg.threads = Some(4);
        let parallel = run_harness(&cfg, &variants).await.unwrap();
        assert_eq!(serial.len(), parallel.len());
        for (s, p) in serial.iter().zip(&parallel) {
            assert_eq!(s.variant.name, p.variant.name);
            assert_eq!(s.results.n_trades(), p.results.n_trades());
        }
    }
}
