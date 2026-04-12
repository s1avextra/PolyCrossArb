//! PolyCrossArb Rust Latency Engine v0.2
//!
//! Ultra-low-latency candle trading pipeline:
//! 1. WebSocket price feeds from 4 exchanges (~100ms updates)
//! 2. Polymarket contract prices from Python orchestrator (stdin)
//! 3. Edge detection: our BS fair value vs stale MM price
//! 4. Edge accumulation: scale into positions as edge grows
//! 5. Trade signals output as JSON lines to stdout
//! 6. Full latency instrumentation on stderr
//!
//! Protocol:
//!   Python → stdin:  JSON lines with contract updates
//!     {"type":"contracts", "data": [{contract_id, token_id, up_price, down_price, end_time_s, window_minutes}, ...]}
//!     {"type":"config", "data": {min_btc_move, min_edge, ...}}
//!   Rust → stdout:  JSON lines with trade signals (LatencySignal)
//!   Rust → stderr:  Latency reports + diagnostics

mod clob;
mod exchange;
mod fair_value;
mod ipc;
mod latency;
mod edge;
mod debug;
mod polymarket_ws;
mod signing;

use std::collections::HashMap;
use std::io::BufRead;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

pub use fair_value::norm_cdf;

#[derive(Debug, Clone)]
pub struct PriceState {
    pub prices: HashMap<String, f64>,
    pub last_update: Instant,
    pub mid_price: f64,
    pub spread: f64,
    pub implied_vol: f64,
    // Per-source timestamps for staleness tracking
    pub source_timestamps: HashMap<String, Instant>,
    // Multi-asset price tracking: "ETH" -> {source -> price}, "SOL" -> {source -> price}
    pub alt_prices: HashMap<String, HashMap<String, f64>>,
    pub alt_mid: HashMap<String, f64>,
    pub alt_timestamps: HashMap<String, Instant>,
}

impl PriceState {
    pub fn new() -> Self {
        Self {
            prices: HashMap::new(),
            last_update: Instant::now(),
            mid_price: 0.0,
            spread: 0.0,
            implied_vol: 0.50,
            source_timestamps: HashMap::new(),
            alt_prices: HashMap::new(),
            alt_mid: HashMap::new(),
            alt_timestamps: HashMap::new(),
        }
    }

    pub fn update(&mut self, source: &str, price: f64) {
        if price <= 0.0 { return; }
        self.prices.insert(source.to_string(), price);
        self.source_timestamps.insert(source.to_string(), Instant::now());
        self.last_update = Instant::now();

        // Filter stale sources (>10s old)
        let now = Instant::now();
        let live: Vec<f64> = self.prices.iter()
            .filter(|(src, _)| {
                self.source_timestamps.get(*src)
                    .map(|t| now.duration_since(*t).as_secs() < 10)
                    .unwrap_or(false)
            })
            .map(|(_, p)| *p)
            .collect();

        if !live.is_empty() {
            self.mid_price = live.iter().sum::<f64>() / live.len() as f64;
            let min = live.iter().cloned().fold(f64::MAX, f64::min);
            let max = live.iter().cloned().fold(f64::MIN, f64::max);
            self.spread = max - min;
        }
    }

    /// Update price for an alt asset (ETH, SOL).
    pub fn update_alt(&mut self, asset: &str, source: &str, price: f64) {
        if price <= 0.0 { return; }
        let key = format!("{}:{}", asset, source);
        self.alt_timestamps.insert(key, Instant::now());

        let sources = self.alt_prices.entry(asset.to_string()).or_default();
        sources.insert(source.to_string(), price);

        // Compute mid from live sources
        let now = Instant::now();
        let live: Vec<f64> = sources.iter()
            .filter(|(src, _)| {
                let key = format!("{}:{}", asset, src);
                self.alt_timestamps.get(&key)
                    .map(|t| now.duration_since(*t).as_secs() < 10)
                    .unwrap_or(false)
            })
            .map(|(_, p)| *p)
            .collect();

        if !live.is_empty() {
            self.alt_mid.insert(
                asset.to_string(),
                live.iter().sum::<f64>() / live.len() as f64,
            );
        }
    }

    pub fn n_live_sources(&self) -> usize {
        let now = Instant::now();
        self.source_timestamps.values()
            .filter(|t| now.duration_since(**t).as_secs() < 10)
            .count()
    }
}

/// Contract info received from Python orchestrator
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ContractUpdate {
    pub contract_id: String,
    pub token_id: String,
    pub up_price: f64,
    pub down_price: f64,
    pub end_time_s: f64,
    pub window_minutes: f64,
}

/// Input message from Python
#[derive(Debug, serde::Deserialize)]
struct InputMessage {
    #[serde(rename = "type")]
    msg_type: String,
    data: serde_json::Value,
}

#[tokio::main]
async fn main() {
    eprintln!("PolyCrossArb Latency Engine v0.2.0");
    eprintln!("Strategy: detect stale MM prices, accumulate edge, scale in");

    let state = Arc::new(RwLock::new(PriceState::new()));
    let monitor = Arc::new(RwLock::new(latency::LatencyMonitor::new()));

    // Start exchange feeds with latency tracking
    {
        let (s, m) = (state.clone(), monitor.clone());
        tokio::spawn(async move {
            loop {
                let t0 = Instant::now();
                exchange::binance_feed(s.clone()).await;
                // If we get here, connection dropped — reconnect
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::bybit_feed(s.clone()).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::okx_feed(s.clone()).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::mexc_feed(s.clone()).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }

    // ETH + SOL price feeds (for cross-asset lead-lag)
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::binance_alt_feed(s.clone()).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::bybit_alt_feed(s.clone()).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }

    // Deribit IV loop
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                if let Some(iv) = exchange::fetch_deribit_iv().await {
                    s.write().await.implied_vol = iv;
                    eprintln!("IV: {:.1}%", iv * 100.0);
                }
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
    }

    // Wait for first price
    loop {
        if state.read().await.mid_price > 0.0 { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    {
        let ps = state.read().await;
        eprintln!("Ready: BTC ${:.2}, {} sources, spread ${:.2}",
            ps.mid_price, ps.n_live_sources(), ps.spread);
    }

    // CLOB client for direct order placement (hot path)
    let clob_enabled = std::env::var("CLOB_DIRECT").unwrap_or_default() == "1";
    let clob_client = if clob_enabled {
        let base_url = std::env::var("POLY_BASE_URL")
            .unwrap_or_else(|_| "https://clob.polymarket.com".to_string());
        let api_key = std::env::var("POLY_API_KEY").unwrap_or_default();
        let api_secret = std::env::var("POLY_API_SECRET").unwrap_or_default();
        let api_passphrase = std::env::var("POLY_API_PASSPHRASE").unwrap_or_default();
        if api_key.is_empty() {
            eprintln!("CLOB_DIRECT=1 but POLY_API_KEY not set — falling back to signal-only mode");
            None
        } else {
            let client = clob::create_shared_client(&base_url, &api_key, &api_secret, &api_passphrase);
            // Set signing key for EIP-712 order signing
            if let Ok(pk) = std::env::var("PRIVATE_KEY") {
                client.write().await.set_signing_key(&pk);
            } else {
                eprintln!("Warning: PRIVATE_KEY not set — orders will fail EIP-712 signing");
            }
            // Pre-warm connection pool
            client.write().await.warm_connection().await;
            eprintln!("CLOB direct order placement ENABLED (EIP-712 signed, 0% maker fees)");
            Some(client)
        }
    } else {
        eprintln!("CLOB direct placement disabled (set CLOB_DIRECT=1 to enable)");
        None
    };

    // Edge accumulator with default config
    let mut accumulator = edge::EdgeAccumulator::new(edge::EdgeConfig::default());

    // Debug mode
    let debug_mode = std::env::var("DEBUG").unwrap_or_default() == "1";
    let mut debug_stats = debug::DebugStats::new();
    let mut btc_tracker = debug::BtcTracker::new();
    let debug_start = Instant::now();
    if debug_mode {
        eprintln!("*** DEBUG MODE ENABLED — full diagnostics every 60s ***");
    }

    // Contract state (updated from stdin)
    let mut contracts: Vec<ContractUpdate> = Vec::new();

    // Read stdin in a background thread (non-blocking)
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(100);
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            if let Ok(line) = line {
                if tx.blocking_send(line).is_err() { break; }
            }
        }
    });

    eprintln!("Listening for contracts on stdin...");

    // ── Event-driven main loop ──────────────────────────────────
    // Instead of polling at 50ms (20Hz), we wake on:
    //   1. Price state change (WS tick from any exchange)
    //   2. stdin/IPC message (contract update from Python)
    //   3. Timer fallback every 200ms (housekeeping)
    //
    // This eliminates the 25ms average polling latency. On a Dublin
    // VPS, the WS tick-to-evaluation path is now <1ms.
    let (tick_tx, mut tick_rx) = tokio::sync::mpsc::channel::<()>(64);
    // Give the price state a notifier so WS feeds wake the main loop
    {
        let tx = tick_tx.clone();
        let s = state.clone();
        tokio::spawn(async move {
            let mut last_price = 0.0_f64;
            loop {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let current = s.read().await.mid_price;
                if (current - last_price).abs() > 0.01 {
                    last_price = current;
                    let _ = tx.try_send(());
                }
            }
        });
    }

    let mut cycle: u64 = 0;
    let report_interval_s = 30.0_f64;
    let cleanup_interval_s = 5.0_f64;
    let mut last_report = Instant::now();
    let mut last_cleanup = Instant::now();
    let mut fallback_timer = tokio::time::interval(Duration::from_millis(200));

    loop {
        // Wait for any event: price tick, stdin message, or 200ms fallback
        tokio::select! {
            _ = tick_rx.recv() => {}
            _ = fallback_timer.tick() => {}
        }

        cycle += 1;
        let tick_start = Instant::now();

        // Check for stdin updates (non-blocking)
        while let Ok(line) = rx.try_recv() {
            if let Ok(msg) = serde_json::from_str::<InputMessage>(&line) {
                match msg.msg_type.as_str() {
                    "contracts" => {
                        if let Ok(c) = serde_json::from_value::<Vec<ContractUpdate>>(msg.data) {
                            contracts = c;
                            eprintln!("Updated: {} contracts", contracts.len());
                        }
                    }
                    "config" => {
                        // Partial config: just update bankroll if that's all we got
                        if let Some(bankroll) = msg.data.get("bankroll_usd").and_then(|v| v.as_f64()) {
                            let old = accumulator.config.bankroll_usd;
                            accumulator.config.bankroll_usd = bankroll;
                            eprintln!("Bankroll: ${:.2} → ${:.2} (locked=${:.2} avail=${:.2})",
                                old, bankroll, accumulator.capital_locked, accumulator.available_capital());
                        } else if let Ok(cfg) = serde_json::from_value::<edge::EdgeConfig>(msg.data) {
                            eprintln!("Config updated: min_move=${} min_edge={} entries={}",
                                cfg.min_btc_move, cfg.min_edge, cfg.max_entries_per_window);
                            accumulator.config = cfg;
                        }
                    }
                    _ => {}
                }
            }
        }

        // Read price state
        let agg_start = Instant::now();
        let ps = state.read().await.clone();
        let btc = ps.mid_price;
        let vol = ps.implied_vol;
        let n_sources = ps.n_live_sources();
        let spread = ps.spread;
        let agg_ns = agg_start.elapsed().as_nanos() as u64;
        monitor.write().await.record("price_aggregation", agg_ns);

        if btc <= 0.0 || contracts.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }

        let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();

        // Record BTC for momentum lookback (reversal filter)
        accumulator.record_btc(now_s, btc);

        // Track BTC for debug
        if debug_mode {
            btc_tracker.record(now_s, btc);
            debug_stats.tick_count += 1;
            // Record BTC moves every second (not every tick)
            if cycle % 20 == 0 {
                debug_stats.btc_moves_5s.push(btc_tracker.move_over(5.0).abs());
                debug_stats.btc_moves_15s.push(btc_tracker.move_over(15.0).abs());
            }
        }

        // Evaluate every contract
        let signal_start = Instant::now();
        let mut tick_signals = 0_usize;

        for c in &contracts {
            let minutes_remaining = (c.end_time_s - now_s) / 60.0;
            if minutes_remaining <= 0.0 { continue; }

            // Track MM staleness
            {
                let mut mon = monitor.write().await;
                mon.record_mm_price(&c.contract_id, c.up_price);
            }
            let mm_stale_s = monitor.read().await.mm_staleness_s(&c.contract_id);

            let tick_ns = tick_start.elapsed().as_nanos() as u64;

            if debug_mode {
                debug_stats.total_contracts_evaluated += 1;
            }

            let ds = if debug_mode { Some(&mut debug_stats) } else { None };

            if let Some(signal) = accumulator.evaluate(
                &c.contract_id,
                &c.token_id,
                c.up_price,
                c.down_price,
                btc,
                c.end_time_s,
                c.window_minutes,
                minutes_remaining,
                mm_stale_s,
                vol,
                n_sources,
                spread,
                tick_ns,
                ds,
            ) {
                // Output signal as JSON line to stdout (always, for Python monitoring)
                if let Ok(json) = serde_json::to_string(&signal) {
                    println!("{}", json);
                }

                // Direct CLOB order placement (hot path — bypasses Python)
                if let Some(ref clob) = clob_client {
                    let clob = clob.clone();
                    let sig = signal.clone();
                    tokio::spawn(async move {
                        let mut client = clob.write().await;
                        // Place maker order (GTC = 0% fee + rebate)
                        // Price: use MM's stale ask (we're buying at their old price)
                        let tick = 0.01_f64;
                        let price = (sig.mm_price / tick).round() * tick;
                        let size = (sig.size_usd / price).round().max(1.0);
                        match client.place_maker_order(
                            &sig.token_id,
                            price,
                            size,
                            "BUY",
                            false, // neg_risk — candle markets are standard CTF
                        ).await {
                            Ok(oid) => eprintln!("CLOB order OK: {} edge={:.1}% id={}",
                                sig.direction, sig.edge * 100.0, &oid[..16.min(oid.len())]),
                            Err(e) => eprintln!("CLOB order FAIL: {}", &e[..80.min(e.len())]),
                        }
                    });
                }

                tick_signals += 1;
                if debug_mode {
                    debug_stats.signals_emitted += 1;
                }
            }
        }
        let signal_ns = signal_start.elapsed().as_nanos() as u64;
        {
            let mut mon = monitor.write().await;
            mon.record("signal_generation", signal_ns);
            mon.record("tick_to_signal", tick_start.elapsed().as_nanos() as u64);
        }

        // Cleanup expired positions (every 5s)
        if last_cleanup.elapsed().as_secs_f64() >= cleanup_interval_s {
            accumulator.cleanup_expired(now_s);
            last_cleanup = Instant::now();
        }

        // Periodic latency report (every 30s)
        if last_report.elapsed().as_secs_f64() >= report_interval_s {
            let mon = monitor.read().await;
            eprintln!("{}", mon.report());
            eprintln!(
                "  positions={} signals={} btc=${:.0} sources={} spread=${:.2} vol={:.1}% locked=${:.2} avail=${:.2}",
                accumulator.positions.len(),
                accumulator.signal_count,
                btc, n_sources, spread, vol * 100.0,
                accumulator.capital_locked,
                accumulator.available_capital(),
            );
            for (cid, pos) in &accumulator.positions {
                eprintln!(
                    "  POS {}: {} entries=${:.2} avg_entry={:.3} avg_edge={:.1}% age={:.0}s",
                    &cid[..16.min(cid.len())], pos.direction, pos.total_size_usd,
                    pos.avg_entry_price, pos.avg_edge() * 100.0, pos.time_in_position_s(),
                );
            }
            last_report = Instant::now();
        }

        // Debug report every 60s
        if debug_mode && last_report.elapsed().as_secs_f64() >= 60.0 {
            let elapsed = debug_start.elapsed().as_secs_f64();
            eprintln!("{}", debug_stats.report(elapsed));
            debug_stats.reset();
        }

        // No sleep — loop returns to tokio::select! which blocks
        // until next WS tick, stdin message, or 200ms fallback.
    }
}
