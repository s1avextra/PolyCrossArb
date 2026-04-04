//! PolyCrossArb Rust Hot-Path Engine
//!
//! High-performance core:
//! 1. WebSocket price feeds from 4 exchanges
//! 2. Black-Scholes fair value computation
//! 3. Edge detection at 100ms intervals
//! 4. Trade signals output as JSON lines to stdout
//!
//! Python orchestrator feeds contracts via stdin, reads signals from stdout.

mod exchange;
mod fair_value;

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct PriceState {
    pub prices: std::collections::HashMap<String, f64>,
    pub last_update: Instant,
    pub mid_price: f64,
    pub spread: f64,
    pub implied_vol: f64,
}

impl PriceState {
    pub fn new() -> Self {
        Self {
            prices: std::collections::HashMap::new(),
            last_update: Instant::now(),
            mid_price: 0.0,
            spread: 0.0,
            implied_vol: 0.50,
        }
    }

    pub fn update(&mut self, source: &str, price: f64) {
        if price <= 0.0 { return; }
        self.prices.insert(source.to_string(), price);
        self.last_update = Instant::now();
        let vals: Vec<f64> = self.prices.values().copied().collect();
        if !vals.is_empty() {
            self.mid_price = vals.iter().sum::<f64>() / vals.len() as f64;
            let min = vals.iter().cloned().fold(f64::MAX, f64::min);
            let max = vals.iter().cloned().fold(f64::MIN, f64::max);
            self.spread = max - min;
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Contract {
    pub token_id: String,
    pub question: String,
    pub strike: f64,
    pub yes_price: f64,
    pub end_date: String,
    pub volume: f64,
}

#[derive(Debug, serde::Serialize)]
struct TradeSignal {
    action: String,
    token_id: String,
    price: f64,
    fair_value: f64,
    edge_pct: f64,
    btc_price: f64,
    strike: f64,
    question: String,
    latency_us: u64,
    timestamp: f64,
    sources: usize,
    spread: f64,
}

#[tokio::main]
async fn main() {
    eprintln!("PolyCrossArb Rust Engine v0.1.0");

    let state = Arc::new(RwLock::new(PriceState::new()));

    // Start exchange feeds
    let (s1, s2, s3, s4, s5) = (
        state.clone(), state.clone(), state.clone(), state.clone(), state.clone(),
    );
    tokio::spawn(async move { exchange::binance_feed(s1).await });
    tokio::spawn(async move { exchange::bybit_feed(s2).await });
    tokio::spawn(async move { exchange::okx_feed(s3).await });
    tokio::spawn(async move { exchange::mexc_feed(s4).await });

    // Wait for first price
    loop {
        if state.read().await.mid_price > 0.0 { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ps = state.read().await;
    eprintln!("Ready: BTC ${:.2}, {} sources", ps.mid_price, ps.prices.len());
    drop(ps);

    // Deribit IV loop
    tokio::spawn(async move {
        loop {
            if let Some(iv) = exchange::fetch_deribit_iv().await {
                s5.write().await.implied_vol = iv;
                eprintln!("IV: {:.1}%", iv * 100.0);
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });

    // Read contracts from stdin
    let mut input = String::new();
    eprintln!("Waiting for contracts on stdin...");
    std::io::stdin().read_line(&mut input).unwrap();
    let contracts: Vec<Contract> = serde_json::from_str(&input).unwrap_or_default();
    eprintln!("Loaded {} contracts", contracts.len());

    // Main loop — 100ms scan interval
    let mut cycle: u64 = 0;
    loop {
        cycle += 1;
        let t0 = Instant::now();

        let ps = state.read().await.clone();
        let btc = ps.mid_price;
        let vol = ps.implied_vol;

        if btc <= 0.0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let now_utc = chrono::Utc::now();
        let mut best_edge: f64 = 0.0;
        let mut best_q = String::new();

        for c in &contracts {
            if c.yes_price < 0.02 || c.yes_price > 0.98 || c.volume < 100.0 {
                continue;
            }

            let hours = match chrono::DateTime::parse_from_rfc3339(&c.end_date) {
                Ok(end) => {
                    let diff = end.signed_duration_since(now_utc);
                    diff.num_seconds() as f64 / 3600.0
                }
                Err(_) => continue,
            };
            if hours > 24.0 || hours < 0.1 { continue; }

            let ratio = c.strike / btc;
            if (0.95..1.05).contains(&ratio) { continue; }
            if ratio > 1.20 || ratio < 0.80 { continue; }

            let fv = fair_value::binary_option_price(btc, c.strike, hours / 24.0, vol);
            if fv > 0.30 && fv < 0.70 { continue; }

            let edge = fv - c.yes_price;
            let edge_pct = edge / c.yes_price.max(0.01);

            if edge_pct.abs() > best_edge.abs() {
                best_edge = edge_pct;
                best_q = c.question.chars().take(45).collect();

                if edge_pct.abs() >= 0.02 {
                    let signal = TradeSignal {
                        action: if edge > 0.0 { "buy_yes" } else { "buy_no" }.into(),
                        token_id: c.token_id.clone(),
                        price: c.yes_price,
                        fair_value: fv,
                        edge_pct,
                        btc_price: btc,
                        strike: c.strike,
                        question: c.question.clone(),
                        latency_us: t0.elapsed().as_micros() as u64,
                        timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64(),
                        sources: ps.prices.len(),
                        spread: ps.spread,
                    };
                    println!("{}", serde_json::to_string(&signal).unwrap());
                }
            }
        }

        if cycle % 100 == 0 {
            let ms = t0.elapsed().as_millis();
            eprintln!(
                "cycle={} btc=${:.0} src={} spread=${:.2} vol={:.1}% edge={:+.2}% scan={}ms {}",
                cycle, btc, ps.prices.len(), ps.spread, vol * 100.0,
                best_edge * 100.0, ms, best_q
            );
        }

        let elapsed = t0.elapsed();
        if elapsed < Duration::from_millis(100) {
            tokio::time::sleep(Duration::from_millis(100) - elapsed).await;
        }
    }
}
