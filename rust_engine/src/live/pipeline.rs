//! Live (paper or live) candle trading pipeline.
//!
//! Translates `src/polymomentum/crypto/candle_pipeline.py::CandlePipeline`
//! to async Rust:
//!
//! - 8-exchange BTC + ETH/SOL spot WS aggregator (already in `exchange.rs`)
//! - Polymarket WS L2 books (already in `polymarket_ws.rs`)
//! - Gamma REST contract refresh (every 2 min)
//! - 10 Hz cycle loop: per-contract evaluation + decision
//! - Paper resolution loop (BTC tape vs window close)
//! - CTF oracle verification loop
//! - Risk + monitoring + breaker

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::json;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::time::sleep;

use crate::clob::{create_shared_client, SharedClobClient};
use crate::config::Settings;
use crate::data::ctf::{CtfReader, Resolution};
use crate::data::gamma::GammaClient;
use crate::data::scanner::{scan_candle_markets, CandleContract};
use crate::execution::fees::polymarket_fee;
use crate::live::breaker::{BreakerConfig, BreakerState};
use crate::live::paper_fill::{simulate_paper_fill, PaperFillCfg};
use crate::live::window::estimate_window_minutes;
use crate::monitoring::alerter::Alerter;
use crate::monitoring::session::{SessionMonitor, SignalEvaluation};
use crate::polymarket_ws::{
    new_shared_book, new_subscription_notify, polymarket_book_feed, SharedBookState,
};
use crate::price_state::PriceState;
use crate::risk::manager::{RiskConfig, RiskManager, TradeRecord};
use crate::strategy::decision::{
    decide_candle_trade, DecisionResult, DEFAULT_MIN_CONFIDENCE, DEFAULT_MIN_EDGE,
    ZoneConfig,
};
use crate::strategy::momentum::{MomentumConfig, MomentumDetector};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Paper,
    Live,
}

impl Mode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::Paper => "paper",
            Mode::Live => "live",
        }
    }
}

pub struct PipelineHandles {
    pub stop: Arc<Notify>,
    pub price_state: Arc<RwLock<PriceState>>,
    pub book_state: SharedBookState,
}

#[derive(Debug, Clone)]
struct PaperPosition {
    direction: String,
    entry_price: f64,
    fee: f64,
    size: f64,
    open_btc: f64,
    end_time: f64,
    asset: String,
    contract_id: String,
}

impl PaperPosition {
    fn to_json(&self) -> serde_json::Value {
        json!({
            "direction": self.direction,
            "entry_price": self.entry_price,
            "fee": self.fee,
            "size": self.size,
            "open_btc": self.open_btc,
            "end_time": self.end_time,
            "asset": self.asset,
            "contract_id": self.contract_id,
        })
    }

    fn from_json(cid: String, v: &serde_json::Value) -> Option<Self> {
        Some(Self {
            direction: v.get("direction")?.as_str()?.to_string(),
            entry_price: v.get("entry_price")?.as_f64()?,
            fee: v.get("fee").and_then(|x| x.as_f64()).unwrap_or(0.0),
            size: v.get("size")?.as_f64()?,
            open_btc: v.get("open_btc")?.as_f64()?,
            end_time: v.get("end_time")?.as_f64()?,
            asset: v
                .get("asset")
                .and_then(|x| x.as_str())
                .unwrap_or("BTC")
                .to_string(),
            contract_id: cid,
        })
    }
}

#[derive(Debug, Clone)]
struct OraclePending {
    our_actual: String,
    our_open_btc: f64,
    our_close_btc: f64,
    end_time: f64,
    attempts: u32,
}

impl OraclePending {
    fn to_json(&self) -> serde_json::Value {
        json!({
            "our_actual": self.our_actual,
            "our_open_btc": self.our_open_btc,
            "our_close_btc": self.our_close_btc,
            "end_time": self.end_time,
            "attempts": self.attempts,
        })
    }

    fn from_json(v: &serde_json::Value) -> Option<Self> {
        Some(Self {
            our_actual: v.get("our_actual")?.as_str()?.to_string(),
            our_open_btc: v.get("our_open_btc")?.as_f64()?,
            our_close_btc: v.get("our_close_btc")?.as_f64()?,
            end_time: v.get("end_time")?.as_f64()?,
            attempts: v
                .get("attempts")
                .and_then(|x| x.as_u64())
                .unwrap_or(0) as u32,
        })
    }
}

pub struct Pipeline {
    settings: Settings,
    mode: Mode,
    risk: RiskManager,
    clob: Option<SharedClobClient>,
    monitor: Arc<SessionMonitor>,
    alerter: Alerter,
    gamma: GammaClient,
    ctf: CtfReader,
    zone_config: ZoneConfig,
    breaker_cfg: BreakerConfig,
    momentum: Mutex<HashMap<String, MomentumDetector>>,
    contracts: RwLock<Vec<CandleContract>>,
    traded: Mutex<HashSet<String>>,
    paper_positions: Mutex<HashMap<String, PaperPosition>>,
    oracle_pending: Mutex<HashMap<String, OraclePending>>,
    breaker: Mutex<BreakerState>,
    breaker_tripped: Mutex<bool>,
    price_state: Arc<RwLock<PriceState>>,
    book_state: SharedBookState,
    tracked_tokens: Arc<RwLock<Vec<String>>>,
    resub_notify: Arc<Notify>,
    stop: Arc<Notify>,
    kill_switch_path: PathBuf,
    cycle_count: Mutex<u64>,
}

impl Pipeline {
    pub async fn new(settings: Settings, mode: Mode) -> Result<Arc<Self>> {
        let bankroll = if settings.bankroll_usd > 0.0 {
            settings.bankroll_usd
        } else {
            // Fall back to wallet detection if private key set
            try_wallet_bankroll(&settings).await.unwrap_or(0.0)
        };
        let risk_cfg = RiskConfig {
            initial_bankroll: bankroll,
            cooldown_seconds: settings.cooldown_seconds,
            max_per_market_override: settings.max_position_per_market_usd,
            ..Default::default()
        };
        let risk = RiskManager::open(&settings.state_db_path, risk_cfg).await?;

        let monitor = Arc::new(SessionMonitor::open(&settings.session_log_dir)?);
        let alerter = Alerter::new(std::env::var("SLACK_WEBHOOK_URL").ok());
        let gamma = GammaClient::new(&settings.poly_gamma_url, &settings.poly_base_url);
        let ctf = CtfReader::new(&settings.polygon_rpc_url);
        let zone_config = ZoneConfig::from_settings(&settings);
        let breaker_cfg = BreakerConfig {
            min_trades: settings.candle_breaker_min_trades.max(1) as u32,
            min_win_rate: settings.candle_breaker_min_win_rate,
            max_drawdown_pct: settings.candle_breaker_max_drawdown_pct,
        };

        // Restore breaker + paper positions + oracle pending
        let breaker_tripped = matches!(
            risk.get_meta("candle_breaker_tripped").await?.as_deref(),
            Some("1")
        );
        let mut paper_positions = HashMap::new();
        for (cid, payload) in risk.load_paper_positions().await.unwrap_or_default() {
            if let Some(pp) = PaperPosition::from_json(cid.clone(), &payload) {
                paper_positions.insert(cid, pp);
            }
        }
        let mut oracle_pending = HashMap::new();
        for (cid, payload) in risk.load_oracle_pending().await.unwrap_or_default() {
            if let Some(op) = OraclePending::from_json(&payload) {
                oracle_pending.insert(cid, op);
            }
        }
        if !paper_positions.is_empty() {
            tracing::info!(n = paper_positions.len(), "restored paper positions");
        }
        if !oracle_pending.is_empty() {
            tracing::info!(n = oracle_pending.len(), "restored oracle-pending");
        }

        let mut momentum_map = HashMap::new();
        let mom_cfg = MomentumConfig {
            noise_z_threshold: settings.candle_noise_z_threshold,
            ..Default::default()
        };
        momentum_map.insert("BTC".to_string(), MomentumDetector::new(None, mom_cfg));

        // Initialize CLOB client only in live mode and only if API creds present.
        let clob = if matches!(mode, Mode::Live)
            && !settings.poly_api_key.is_empty()
            && !settings.private_key.is_empty()
        {
            let client = create_shared_client(
                &settings.poly_base_url,
                &settings.poly_api_key,
                &settings.poly_api_secret,
                &settings.poly_api_passphrase,
            );
            client.write().await.set_signing_key(&settings.private_key);
            client.write().await.warm_connection().await;
            tracing::info!("CLOB direct order placement ENABLED (live mode)");
            Some(client)
        } else {
            None
        };

        let p = Arc::new(Self {
            kill_switch_path: PathBuf::from(&settings.kill_switch_path),
            settings,
            mode,
            risk,
            clob,
            monitor,
            alerter,
            gamma,
            ctf,
            zone_config,
            breaker_cfg,
            momentum: Mutex::new(momentum_map),
            contracts: RwLock::new(Vec::new()),
            traded: Mutex::new(HashSet::new()),
            paper_positions: Mutex::new(paper_positions),
            oracle_pending: Mutex::new(oracle_pending),
            breaker: Mutex::new(BreakerState::default()),
            breaker_tripped: Mutex::new(breaker_tripped),
            price_state: Arc::new(RwLock::new(PriceState::new())),
            book_state: new_shared_book(),
            tracked_tokens: Arc::new(RwLock::new(Vec::new())),
            resub_notify: new_subscription_notify(),
            stop: Arc::new(Notify::new()),
            cycle_count: Mutex::new(0),
        });

        Ok(p)
    }

    pub fn handles(&self) -> PipelineHandles {
        PipelineHandles {
            stop: self.stop.clone(),
            price_state: self.price_state.clone(),
            book_state: self.book_state.clone(),
        }
    }

    pub async fn run(self: &Arc<Self>) -> Result<()> {
        tracing::info!(mode = self.mode.as_str(), "candle.start");
        if self.alerter.enabled() {
            let _ = self
                .alerter
                .send("info", "PolyMomentum Rust starting", &format!("mode={}", self.mode.as_str()))
                .await;
        }

        // Spawn exchange feeds (BTC: binance/bybit/okx; ETH+SOL: alts; Deribit IV)
        spawn_exchange_feeds(self.price_state.clone());

        // Polymarket WS book feed
        {
            let bs = self.book_state.clone();
            let tt = self.tracked_tokens.clone();
            let nt = self.resub_notify.clone();
            tokio::spawn(async move {
                polymarket_book_feed(bs, tt, nt).await;
            });
        }

        // First contract refresh
        if let Err(e) = self.refresh_contracts().await {
            tracing::warn!(error = %e, "initial contract refresh failed");
        }

        // Wait for first BTC price
        loop {
            if self.price_state.read().await.mid_price > 0.0 {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        let scan = {
            let p = self.clone();
            tokio::spawn(async move { p.scan_loop().await })
        };
        let refresh = {
            let p = self.clone();
            tokio::spawn(async move { p.contract_refresh_loop().await })
        };
        let resolve = {
            let p = self.clone();
            tokio::spawn(async move { p.paper_resolution_loop().await })
        };
        let oracle = {
            let p = self.clone();
            tokio::spawn(async move { p.oracle_verification_loop().await })
        };
        let monitor = {
            let p = self.clone();
            tokio::spawn(async move { p.monitoring_loop().await })
        };

        let stop = self.stop.clone();
        stop.notified().await;
        scan.abort();
        refresh.abort();
        resolve.abort();
        oracle.abort();
        monitor.abort();

        if let Err(e) = self.monitor.save_summary() {
            tracing::warn!(error = %e, "save summary failed");
        }
        if self.alerter.enabled() {
            let bs = self.breaker.lock().await;
            let _ = self
                .alerter
                .send(
                    "warning",
                    "PolyMomentum Rust stopped",
                    &format!(
                        "wins={} losses={} pnl=${:.2}",
                        bs.wins, bs.losses, bs.realized_pnl
                    ),
                )
                .await;
        }
        Ok(())
    }

    pub async fn refresh_contracts(&self) -> Result<()> {
        let markets = self
            .gamma
            .fetch_markets_by_end_date(3.0, 0.0)
            .await?;
        let contracts = scan_candle_markets(&markets, 1.0, 50.0);

        let active_cids: HashSet<String> =
            contracts.iter().map(|c| c.market.condition_id.clone()).collect();
        {
            let mut traded = self.traded.lock().await;
            traded.retain(|c| active_cids.contains(c));
        }
        {
            let mut moms = self.momentum.lock().await;
            for det in moms.values_mut() {
                det.evict_stale_windows(&active_cids);
            }
        }

        // Update token subscriptions
        let token_ids: Vec<String> = contracts
            .iter()
            .flat_map(|c| {
                vec![c.up_token_id.clone(), c.down_token_id.clone()]
                    .into_iter()
                    .filter(|s| !s.is_empty())
            })
            .collect();
        {
            let mut tt = self.tracked_tokens.write().await;
            *tt = token_ids;
        }
        self.resub_notify.notify_one();

        let n = contracts.len();
        *self.contracts.write().await = contracts;
        tracing::info!(contracts = n, "candle.scan");
        Ok(())
    }

    async fn contract_refresh_loop(self: Arc<Self>) {
        loop {
            sleep(Duration::from_secs(120)).await;
            if let Err(e) = self.refresh_contracts().await {
                tracing::warn!(error = %e, "refresh failed");
                self.monitor.record_error("contract_refresh", &e.to_string(), true);
            }
        }
    }

    async fn scan_loop(self: Arc<Self>) {
        let mut last_btc = 0.0;
        let mut unchanged = 0u32;
        loop {
            let cycle_start = Instant::now();
            {
                let mut c = self.cycle_count.lock().await;
                *c += 1;
            }

            let ps = self.price_state.read().await.clone();
            let btc = ps.mid_price;
            if btc <= 0.0 {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            // Skip evaluation if BTC unchanged (with periodic forced refresh
            // every 10 cycles to catch zone transitions).
            if (btc - last_btc).abs() < 1e-9 {
                unchanged += 1;
                if unchanged < 10 {
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
                unchanged = 0;
            } else {
                unchanged = 0;
            }
            last_btc = btc;

            // Tick the BTC momentum detector
            {
                let mut moms = self.momentum.lock().await;
                let det = moms.entry("BTC".to_string()).or_insert_with(|| {
                    MomentumDetector::new(
                        Some(ps.implied_vol),
                        MomentumConfig {
                            noise_z_threshold: self.settings.candle_noise_z_threshold,
                            ..Default::default()
                        },
                    )
                });
                det.add_tick(btc, None);
                det.set_realized_vol(ps.implied_vol);
            }

            // Tick alts (ETH/SOL) if cross-asset is enabled — feed their WS
            // prices (`ps.alt_mid`) into per-asset momentum detectors.
            if self.settings.candle_cross_asset_enabled {
                let mut moms = self.momentum.lock().await;
                for asset in ["ETH", "SOL"] {
                    if let Some(&alt_price) = ps.alt_mid.get(asset) {
                        if alt_price > 0.0 {
                            let det = moms.entry(asset.to_string()).or_insert_with(|| {
                                MomentumDetector::new(
                                    Some(ps.implied_vol),
                                    MomentumConfig {
                                        noise_z_threshold: self
                                            .settings
                                            .candle_noise_z_threshold,
                                        ..Default::default()
                                    },
                                )
                            });
                            det.add_tick(alt_price, None);
                            det.set_realized_vol(ps.implied_vol);
                        }
                    }
                }
            }

            // Kill switch
            if self.kill_switch_active() {
                self.trip_breaker("kill_switch").await;
                self.stop.notify_one();
                return;
            }

            // Eager breaker check (every cycle)
            {
                let bs = *self.breaker.lock().await;
                let open_exposure: f64 = self
                    .paper_positions
                    .lock()
                    .await
                    .values()
                    .map(|p| p.entry_price * p.size)
                    .sum();
                if let Some(reason) = bs.should_trip(
                    &self.breaker_cfg,
                    open_exposure,
                    self.settings.bankroll_usd.max(1.0),
                ) {
                    self.trip_breaker(reason).await;
                }
            }
            if *self.breaker_tripped.lock().await {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            let now = Utc::now();
            let now_ts = now.timestamp() as f64;

            let books = self.book_state.read().await.clone();
            let contracts = self.contracts.read().await.clone();
            let mut traded_windows: HashSet<String> = HashSet::new();
            let traded_set = self.traded.lock().await.clone();

            for c in contracts.iter() {
                let cid = c.market.condition_id.clone();
                if traded_set.contains(&cid) {
                    continue;
                }

                let Ok(end) = parse_end(&c.end_date) else { continue };
                let minutes_left = (end - now).num_seconds() as f64 / 60.0;
                if minutes_left <= 0.083 || minutes_left > 30.0 {
                    continue;
                }
                if traded_windows.contains(&c.end_date) {
                    continue;
                }
                let window_minutes = estimate_window_minutes(&c.window_description);
                if window_minutes <= 0.0 {
                    self.monitor.record_signal_skip(&cid, "window_parse_failed");
                    continue;
                }
                let minutes_elapsed = (window_minutes - minutes_left).max(0.0);
                if minutes_elapsed < 0.5 {
                    continue;
                }

                let asset_price = if c.asset == "BTC" {
                    btc
                } else {
                    ps.alt_mid.get(&c.asset).copied().unwrap_or(0.0)
                };
                if asset_price <= 0.0 {
                    continue;
                }

                // Pull real-time best up/down from the WS book if fresh,
                // otherwise fall back to the scanner snapshot.
                let (up_price, down_price) = pick_book_prices(c, &books, now_ts);

                // Detect momentum for the contract's own asset
                let signal = {
                    let mut moms = self.momentum.lock().await;
                    let det = moms.entry(c.asset.clone()).or_insert_with(|| {
                        MomentumDetector::new(
                            Some(ps.implied_vol),
                            MomentumConfig {
                                noise_z_threshold: self.settings.candle_noise_z_threshold,
                                ..Default::default()
                            },
                        )
                    });
                    if det.get_open_price(&cid).is_none() {
                        det.set_window_open(&cid, asset_price);
                    }
                    det.detect(&cid, minutes_elapsed, minutes_left, asset_price, None)
                };
                let Some(signal) = signal else { continue };

                let decision = decide_candle_trade(
                    &signal,
                    minutes_elapsed,
                    minutes_left,
                    window_minutes,
                    up_price,
                    down_price,
                    asset_price,
                    signal.open_price,
                    ps.implied_vol,
                    DEFAULT_MIN_CONFIDENCE,
                    DEFAULT_MIN_EDGE,
                    self.settings.candle_skip_dead_zone,
                    &self.zone_config,
                    0.0, // cross-asset boost not yet wired
                );

                let (vol_fast, vol_slow) = {
                    let moms = self.momentum.lock().await;
                    moms.get(&c.asset)
                        .map(|d| (d.realized_vol(), d.slow_realized_vol()))
                        .unwrap_or((ps.implied_vol, ps.implied_vol))
                };
                let eval_ts_ms = (SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0)) as i64;

                match decision {
                    DecisionResult::Skip(skip) => {
                        let aggregate = format!("{}_{}", skip.reason, skip.zone);
                        self.monitor.record_signal_skip(&cid, &aggregate);
                        self.monitor.record_signal_evaluation(&SignalEvaluation {
                            ts_ms: eval_ts_ms,
                            cid: short_cid(&cid),
                            asset: c.asset.clone(),
                            open: signal.open_price,
                            px: signal.current_price,
                            chg: signal.price_change,
                            chg_pct: signal.price_change_pct,
                            cons: signal.consistency,
                            z: signal.z_score,
                            conf: signal.confidence,
                            elapsed_min: signal.minutes_elapsed,
                            remaining_min: signal.minutes_remaining,
                            dir: signal.direction.clone(),
                            vol_fast,
                            vol_slow,
                            implied_vol: ps.implied_vol,
                            cross_boost: 0.0,
                            up_price,
                            down_price,
                            zone: skip.zone.clone(),
                            fair: 0.0,
                            edge: 0.0,
                            traded: false,
                            skip_reason: Some(skip.reason),
                            skip_detail: Some(skip.detail),
                        });
                    }
                    DecisionResult::Trade(decision) => {
                        traded_windows.insert(c.end_date.clone());
                        self.monitor.record_signal_evaluation(&SignalEvaluation {
                            ts_ms: eval_ts_ms,
                            cid: short_cid(&cid),
                            asset: c.asset.clone(),
                            open: signal.open_price,
                            px: signal.current_price,
                            chg: signal.price_change,
                            chg_pct: signal.price_change_pct,
                            cons: signal.consistency,
                            z: signal.z_score,
                            conf: signal.confidence,
                            elapsed_min: signal.minutes_elapsed,
                            remaining_min: signal.minutes_remaining,
                            dir: signal.direction.clone(),
                            vol_fast,
                            vol_slow,
                            implied_vol: ps.implied_vol,
                            cross_boost: 0.0,
                            up_price,
                            down_price,
                            zone: decision.zone.clone(),
                            fair: decision.fair_value,
                            edge: decision.edge,
                            traded: true,
                            skip_reason: None,
                            skip_detail: None,
                        });
                        if let Err(e) = self.execute_trade(c, &signal, &decision, &ps).await {
                            tracing::warn!(error = %e, "execute_trade failed");
                            self.monitor
                                .record_error("execute_trade", &e.to_string(), true);
                        }
                    }
                }
            }

            let cycle_ms = cycle_start.elapsed().as_secs_f64() * 1000.0;
            let cycle = *self.cycle_count.lock().await;
            if cycle % 30 == 0 {
                let top = self.monitor.top_skip_reasons(5);
                tracing::info!(
                    cycle,
                    btc,
                    cycle_ms = cycle_ms,
                    contracts = contracts.len(),
                    top_skips = ?top,
                    "candle.cycle"
                );
            }

            let elapsed_ms = cycle_start.elapsed().as_millis() as u64;
            if elapsed_ms < 100 {
                sleep(Duration::from_millis(100 - elapsed_ms)).await;
            }
        }
    }

    async fn execute_trade(
        self: &Arc<Self>,
        contract: &CandleContract,
        signal: &crate::strategy::momentum::MomentumSignal,
        decision: &crate::strategy::decision::CandleDecision,
        ps: &PriceState,
    ) -> Result<()> {
        let bankroll = self.risk.effective_bankroll().await;
        let mut position = bankroll * self.settings.candle_position_pct;

        // Volatility regime sizing
        let vol_ratio = if ps.implied_vol > 0.0 {
            ps.implied_vol / 0.50
        } else {
            1.0
        };
        if vol_ratio > 2.5 {
            position *= self.settings.candle_vol_extreme_multiplier;
        } else if vol_ratio > 1.5 {
            position *= self.settings.candle_vol_high_multiplier;
        }

        let max_per_market = self.risk.max_per_market().await;
        let avail = self.risk.available_capital().await;
        position = position.min(max_per_market).min(avail);
        if 0.0 < position && position < 1.0 && avail >= 1.0 {
            position = 1.0;
        }
        if position < 1.0 {
            return Ok(());
        }

        let token_id = if signal.direction == "up" {
            &contract.up_token_id
        } else {
            &contract.down_token_id
        };
        let market_price = decision.market_price;

        match self.mode {
            Mode::Paper => {
                let cfg = PaperFillCfg {
                    prefer_maker: self.settings.candle_prefer_maker,
                    ..Default::default()
                };
                let Some(fill) = simulate_paper_fill(market_price, position, &cfg) else {
                    return Ok(());
                };
                let expected_profit = fill.shares * (decision.fair_value - fill.fill_price) - fill.fee;
                tracing::info!(
                    direction = %signal.direction,
                    cost = position,
                    fee = fill.fee,
                    profit = expected_profit,
                    edge = decision.edge,
                    minutes_left = signal.minutes_remaining,
                    "candle.trade.paper"
                );
                self.traded.lock().await.insert(contract.market.condition_id.clone());
                let now_ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);
                self.risk
                    .record_trade(TradeRecord {
                        timestamp: now_ts,
                        market_condition_id: contract.market.condition_id.clone(),
                        outcome_idx: 0,
                        side: "buy".into(),
                        size: fill.shares,
                        price: fill.fill_price,
                        cost: position,
                        event_id: contract.market.event_id.clone(),
                        pnl: 0.0,
                        paper: true,
                    })
                    .await?;

                let end_ts = parse_end(&contract.end_date)?.timestamp() as f64;
                let pp = PaperPosition {
                    direction: signal.direction.clone(),
                    entry_price: fill.fill_price,
                    fee: fill.fee,
                    size: fill.shares,
                    open_btc: signal.open_price,
                    end_time: end_ts,
                    asset: contract.asset.clone(),
                    contract_id: contract.market.condition_id.clone(),
                };
                self.paper_positions
                    .lock()
                    .await
                    .insert(contract.market.condition_id.clone(), pp);
                self.persist_paper_positions().await;
                Ok(())
            }
            Mode::Live => {
                let Some(clob) = self.clob.clone() else {
                    tracing::error!("live mode but no CLOB client (missing api keys / private key)");
                    return Ok(());
                };
                // Round to tick (0.01) and ensure we don't crash on degenerate inputs.
                let tick = 0.01_f64;
                let limit_price = ((market_price / tick).round() * tick).clamp(0.01, 0.99);
                let shares = (position / limit_price).round().max(1.0);
                let zone = decision.zone.as_str();
                let prefer_maker = self.settings.candle_prefer_maker && zone != "terminal";

                let t_start = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);
                let result = if prefer_maker {
                    let maker = clob
                        .write()
                        .await
                        .place_maker_order(token_id, limit_price, shares, "BUY", false)
                        .await;
                    if maker.is_err() {
                        tracing::warn!("CLOB maker rejected; taker fallback");
                        clob.write()
                            .await
                            .place_taker_order(token_id, limit_price, shares, "BUY", false)
                            .await
                    } else {
                        maker
                    }
                } else {
                    clob.write()
                        .await
                        .place_taker_order(token_id, limit_price, shares, "BUY", false)
                        .await
                };
                let fill_time_s = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0)
                    - t_start;

                match result {
                    Ok(order_id) => {
                        let order_value = limit_price * shares;
                        let approx_fee = polymarket_fee(shares, limit_price, 0.072);
                        self.monitor.record_order_placed(&crate::monitoring::session::OrderPlaced {
                            token_id: short_cid(token_id),
                            side: "BUY".into(),
                            price: limit_price,
                            live_price: market_price,
                            size: shares,
                            order_value,
                            order_id: short_cid(&order_id),
                            book_best_ask: market_price,
                            book_ask_depth: 0.0,
                            book_bid_depth: 0.0,
                            balance_usd: self.risk.effective_bankroll().await,
                        });
                        self.monitor.record_order_filled(&crate::monitoring::session::OrderFilled {
                            order_id: short_cid(&order_id),
                            filled: shares,
                            requested: shares,
                            fill_pct: 1.0,
                            fill_price: limit_price,
                            limit_price,
                            slippage: 0.0,
                            slippage_bps: 0.0,
                            fill_time_s,
                            fee: approx_fee,
                            n_trades: 1,
                        });
                        self.traded.lock().await.insert(contract.market.condition_id.clone());
                        let now_ts = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs_f64())
                            .unwrap_or(0.0);
                        self.risk
                            .record_trade(TradeRecord {
                                timestamp: now_ts,
                                market_condition_id: contract.market.condition_id.clone(),
                                outcome_idx: 0,
                                side: "buy".into(),
                                size: shares,
                                price: limit_price,
                                cost: order_value,
                                event_id: contract.market.event_id.clone(),
                                pnl: 0.0,
                                paper: false,
                            })
                            .await?;
                        let end_ts = parse_end(&contract.end_date)?.timestamp() as f64;
                        let pp = PaperPosition {
                            direction: signal.direction.clone(),
                            entry_price: limit_price,
                            fee: approx_fee,
                            size: shares,
                            open_btc: signal.open_price,
                            end_time: end_ts,
                            asset: contract.asset.clone(),
                            contract_id: contract.market.condition_id.clone(),
                        };
                        self.paper_positions
                            .lock()
                            .await
                            .insert(contract.market.condition_id.clone(), pp);
                        self.persist_paper_positions().await;
                        tracing::info!(
                            order_id = short_cid(&order_id),
                            cost = order_value,
                            fill_time_s,
                            "candle.trade.live.filled"
                        );
                    }
                    Err(e) => {
                        let truncated = if e.len() > 200 { &e[..200] } else { e.as_str() };
                        self.monitor
                            .record_order_rejected(token_id, truncated, limit_price, shares);
                        tracing::warn!(error = %truncated, "candle.trade.live.failed");
                    }
                }
                Ok(())
            }
        }
    }

    async fn paper_resolution_loop(self: Arc<Self>) {
        loop {
            let near_resolution = {
                let pp = self.paper_positions.lock().await;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);
                pp.values().any(|p| (p.end_time - now) > 0.0 && (p.end_time - now) < 15.0)
            };
            sleep(Duration::from_secs(if near_resolution { 1 } else { 5 })).await;

            let positions = self.paper_positions.lock().await.clone();
            if positions.is_empty() {
                continue;
            }

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);

            let ps = self.price_state.read().await.clone();
            let btc = ps.mid_price;
            if btc <= 0.0 {
                continue;
            }

            let mut resolved: Vec<String> = Vec::new();
            for (cid, pos) in positions.iter() {
                if now < pos.end_time {
                    continue;
                }
                let close_price = if pos.asset == "BTC" {
                    btc
                } else {
                    ps.alt_mid.get(&pos.asset).copied().unwrap_or(btc)
                };
                let actual = if close_price >= pos.open_btc { "up" } else { "down" };
                let won = actual == pos.direction;
                let pnl = if won {
                    (1.0 - pos.entry_price) * pos.size - pos.fee
                } else {
                    -pos.entry_price * pos.size - pos.fee
                };
                self.risk.record_pnl(pnl).await.ok();
                self.risk.record_fees(pos.fee).await;
                let mut bs = self.breaker.lock().await;
                bs.record_resolution(won, pnl);
                drop(bs);

                self.monitor.record_resolution(
                    cid,
                    &pos.direction,
                    actual,
                    won,
                    pnl,
                    pos.entry_price,
                    pos.open_btc,
                    close_price,
                );

                self.oracle_pending.lock().await.insert(
                    cid.clone(),
                    OraclePending {
                        our_actual: actual.to_string(),
                        our_open_btc: pos.open_btc,
                        our_close_btc: close_price,
                        end_time: pos.end_time,
                        attempts: 0,
                    },
                );

                tracing::info!(
                    cid = short_cid(cid),
                    predicted = %pos.direction,
                    actual,
                    won,
                    pnl,
                    "candle.resolved"
                );
                resolved.push(cid.clone());
            }

            if !resolved.is_empty() {
                let mut pp = self.paper_positions.lock().await;
                for cid in &resolved {
                    pp.remove(cid);
                }
                drop(pp);
                self.persist_paper_positions().await;
                self.persist_oracle_pending().await;

                // Post-resolution breaker check
                let bs = *self.breaker.lock().await;
                let open_exp: f64 = self
                    .paper_positions
                    .lock()
                    .await
                    .values()
                    .map(|p| p.entry_price * p.size)
                    .sum();
                if let Some(reason) = bs.should_trip(
                    &self.breaker_cfg,
                    open_exp,
                    self.settings.bankroll_usd.max(1.0),
                ) {
                    self.trip_breaker(reason).await;
                    self.stop.notify_one();
                }
            }
        }
    }

    async fn oracle_verification_loop(self: Arc<Self>) {
        const MAX_ATTEMPTS: u32 = 120;
        loop {
            sleep(Duration::from_secs(60)).await;
            let pending = self.oracle_pending.lock().await.clone();
            if pending.is_empty() {
                continue;
            }
            let mut to_remove: Vec<String> = Vec::new();
            for (cid, mut entry) in pending {
                entry.attempts += 1;
                let result = self.ctf.get_resolution(&cid).await;
                match result {
                    Ok((Resolution::NotResolved, _)) => {
                        if entry.attempts >= MAX_ATTEMPTS {
                            to_remove.push(cid.clone());
                        } else {
                            self.oracle_pending.lock().await.insert(cid, entry);
                        }
                    }
                    Ok((res, [n0, n1])) => {
                        let res_str = res.as_str();
                        let agreed = res_str == entry.our_actual;
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs_f64())
                            .unwrap_or(0.0);
                        let delay = now - entry.end_time;
                        self.monitor.record_oracle_resolution(
                            &cid,
                            &entry.our_actual,
                            entry.our_open_btc,
                            entry.our_close_btc,
                            res_str,
                            &[n0 as f64, n1 as f64],
                            true,
                            agreed,
                            delay,
                        );
                        if !agreed {
                            tracing::warn!(
                                cid = short_cid(&cid),
                                ours = %entry.our_actual,
                                polymarket = res_str,
                                "candle.oracle.disagreement"
                            );
                        } else {
                            tracing::info!(cid = short_cid(&cid), "candle.oracle.agreed");
                        }
                        to_remove.push(cid);
                    }
                    Err(e) => {
                        tracing::warn!(cid = short_cid(&cid), error = %e, "ctf read failed");
                        if entry.attempts >= MAX_ATTEMPTS {
                            to_remove.push(cid.clone());
                        } else {
                            self.oracle_pending.lock().await.insert(cid, entry);
                        }
                    }
                }
            }
            if !to_remove.is_empty() {
                let mut op = self.oracle_pending.lock().await;
                for cid in &to_remove {
                    op.remove(cid);
                }
                drop(op);
                self.persist_oracle_pending().await;
            }
        }
    }

    async fn monitoring_loop(self: Arc<Self>) {
        let mut prev_sources: HashSet<String> = HashSet::new();
        loop {
            sleep(Duration::from_secs(15)).await;
            let ps = self.price_state.read().await.clone();
            let n_sources = ps.n_live_sources();
            let staleness_ms = if let Some(t) = ps.source_timestamps.values().max() {
                t.elapsed().as_millis() as f64
            } else {
                0.0
            };
            let sources: HashMap<String, f64> = ps
                .prices
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            self.monitor.record_price_snapshot(
                ps.mid_price,
                n_sources,
                ps.spread,
                staleness_ms,
                &sources,
            );
            let current: HashSet<String> = sources.keys().cloned().collect();
            for src in prev_sources.difference(&current) {
                self.monitor.record_error("source_dropout", src, true);
            }
            prev_sources = current;

            let bs = *self.breaker.lock().await;
            let bankroll = self.risk.effective_bankroll().await;
            let exposure = self.risk.total_exposure().await;
            let avail = self.risk.available_capital().await;
            let n_paper = self.paper_positions.lock().await.len() as u64;
            self.monitor.record_risk_state(
                bankroll,
                exposure,
                avail,
                n_paper,
                bs.realized_pnl,
                bs.wins,
                bs.losses,
            );
        }
    }

    async fn trip_breaker(&self, reason: &str) {
        let mut tripped = self.breaker_tripped.lock().await;
        if *tripped {
            return;
        }
        *tripped = true;
        let _ = self.risk.set_meta("candle_breaker_tripped", "1").await;
        let bs = *self.breaker.lock().await;
        tracing::warn!(reason, wins = bs.wins, losses = bs.losses, pnl = bs.realized_pnl, "candle.circuit_breaker.tripped");
        let _ = self
            .alerter
            .send(
                "critical",
                "PolyMomentum circuit breaker",
                &format!("reason={reason} wins={} losses={} pnl=${:.2}", bs.wins, bs.losses, bs.realized_pnl),
            )
            .await;
    }

    fn kill_switch_active(&self) -> bool {
        self.kill_switch_path.exists()
    }

    async fn persist_paper_positions(&self) {
        let pp = self.paper_positions.lock().await.clone();
        let entries: Vec<(String, serde_json::Value)> = pp
            .into_iter()
            .map(|(k, v)| (k, v.to_json()))
            .collect();
        if let Err(e) = self.risk.save_paper_positions(&entries).await {
            tracing::warn!(error = %e, "persist paper positions failed");
        }
    }

    async fn persist_oracle_pending(&self) {
        let op = self.oracle_pending.lock().await.clone();
        let entries: Vec<(String, serde_json::Value)> = op
            .into_iter()
            .map(|(k, v)| (k, v.to_json()))
            .collect();
        if let Err(e) = self.risk.save_oracle_pending(&entries).await {
            tracing::warn!(error = %e, "persist oracle pending failed");
        }
    }
}

fn pick_book_prices(
    contract: &CandleContract,
    books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
    now_ts: f64,
) -> (f64, f64) {
    let up = books
        .get(&contract.up_token_id)
        .and_then(|b| {
            let age = now_ts - b.last_update_us as f64 / 1_000_000.0;
            if age < 30.0 && b.best_ask > 0.0 {
                Some(b.best_ask)
            } else {
                None
            }
        })
        .unwrap_or(contract.up_price);
    let down = books
        .get(&contract.down_token_id)
        .and_then(|b| {
            let age = now_ts - b.last_update_us as f64 / 1_000_000.0;
            if age < 30.0 && b.best_ask > 0.0 {
                Some(b.best_ask)
            } else {
                None
            }
        })
        .unwrap_or(contract.down_price);
    (up, down)
}

fn parse_end(s: &str) -> Result<DateTime<Utc>> {
    let normalized = s.replace('Z', "+00:00");
    Ok(DateTime::parse_from_rfc3339(&normalized)?.with_timezone(&Utc))
}

fn short_cid(s: &str) -> String {
    if s.len() <= 16 {
        s.to_string()
    } else {
        s[..16].to_string()
    }
}

async fn try_wallet_bankroll(settings: &Settings) -> Option<f64> {
    if settings.private_key.is_empty() {
        return None;
    }
    let r = crate::data::wallet::WalletReader::new(&settings.polygon_rpc_url, &settings.private_key)
        .ok()?;
    let b = r.fetch_balances().await.ok()?;
    if b.usdc_e > 0.0 {
        tracing::info!(
            address = b.address,
            usdc_e = b.usdc_e,
            "auto-detected bankroll"
        );
        Some(b.usdc_e)
    } else {
        None
    }
}

fn spawn_exchange_feeds(state: Arc<RwLock<PriceState>>) {
    use crate::exchange;

    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::binance_feed(s.clone()).await;
                sleep(Duration::from_secs(3)).await;
            }
        });
    }
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::bybit_feed(s.clone()).await;
                sleep(Duration::from_secs(3)).await;
            }
        });
    }
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::okx_feed(s.clone()).await;
                sleep(Duration::from_secs(3)).await;
            }
        });
    }
    // Alts
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::binance_alt_feed(s.clone()).await;
                sleep(Duration::from_secs(3)).await;
            }
        });
    }
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::bybit_alt_feed(s.clone()).await;
                sleep(Duration::from_secs(3)).await;
            }
        });
    }
    // Deribit IV
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                if let Some(iv) = exchange::fetch_deribit_iv().await {
                    s.write().await.implied_vol = iv;
                }
                sleep(Duration::from_secs(60)).await;
            }
        });
    }
}
