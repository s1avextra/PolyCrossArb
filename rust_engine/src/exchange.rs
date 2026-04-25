//! WebSocket price feeds from 4 exchanges + Deribit IV.
//!
//! Connection lifecycle (subscribe, keepalive ping, frame-staleness watchdog,
//! exponential backoff with reconnect rate cap) is centralized in `run_ws_feed`
//! to harden against MEXC-style quirks: idle disconnect at 60s, silent stalls
//! after rapid reconnects, and TCP-reads-OK-but-no-frames behavior.

use crate::PriceState;
use futures_util::{SinkExt, StreamExt};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const STALE_AFTER: Duration = Duration::from_secs(90);
const WATCHDOG_TICK: Duration = Duration::from_secs(5);
const BACKOFF_INIT: Duration = Duration::from_millis(100);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const RECONNECT_WINDOW: Duration = Duration::from_secs(2 * 3600);
const RECONNECT_RATE_LIMIT_AFTER: usize = 3;
const RECONNECT_RATE_FLOOR: Duration = Duration::from_secs(15 * 60);

struct Backoff {
    history: Vec<Instant>,
    next: Duration,
}

impl Backoff {
    fn new() -> Self {
        Self { history: Vec::new(), next: BACKOFF_INIT }
    }

    /// Compute delay before next reconnect; records this attempt in history.
    fn delay(&mut self) -> Duration {
        let now = Instant::now();
        self.history.retain(|t| now.duration_since(*t) < RECONNECT_WINDOW);
        self.history.push(now);
        let floor = if self.history.len() > RECONNECT_RATE_LIMIT_AFTER {
            RECONNECT_RATE_FLOOR
        } else {
            Duration::ZERO
        };
        let exp = self.next.min(BACKOFF_MAX);
        self.next = self.next.saturating_mul(2).min(BACKOFF_MAX);
        exp.max(floor)
    }

    fn reset(&mut self) {
        self.next = BACKOFF_INIT;
    }
}

struct WsCfg {
    name: &'static str,
    url: &'static str,
    subscribe: Option<&'static str>,
    ping: Option<(Duration, &'static str)>,
}

async fn run_ws_feed<F, Fut>(cfg: WsCfg, mut on_text: F)
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut backoff = Backoff::new();
    loop {
        match connect_async(cfg.url).await {
            Ok((ws, _)) => {
                eprintln!("{} connected", cfg.name);
                let (mut write, mut read) = ws.split();

                if let Some(sub) = cfg.subscribe {
                    if write.send(Message::Text(sub.to_string().into())).await.is_err() {
                        eprintln!("{} subscribe send failed", cfg.name);
                        tokio::time::sleep(backoff.delay()).await;
                        continue;
                    }
                }

                let mut watchdog = tokio::time::interval(WATCHDOG_TICK);
                watchdog.set_missed_tick_behavior(MissedTickBehavior::Delay);
                // No-op tick for feeds without app-level ping; just fires unused.
                let ping_dur = cfg.ping.map(|(d, _)| d).unwrap_or(Duration::from_secs(3600));
                let mut ping_timer = tokio::time::interval(ping_dur);
                ping_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
                let mut last_frame = Instant::now();
                let mut got_frame = false;

                loop {
                    tokio::select! {
                        msg = read.next() => {
                            match msg {
                                Some(Ok(m)) => {
                                    last_frame = Instant::now();
                                    if !got_frame {
                                        got_frame = true;
                                        backoff.reset();
                                    }
                                    if let Ok(text) = m.into_text() {
                                        on_text(text.to_string()).await;
                                    }
                                }
                                _ => break,
                            }
                        }
                        _ = watchdog.tick() => {
                            if last_frame.elapsed() > STALE_AFTER {
                                eprintln!(
                                    "{} stale (no frame in {}s), reconnecting",
                                    cfg.name, STALE_AFTER.as_secs()
                                );
                                break;
                            }
                        }
                        _ = ping_timer.tick() => {
                            if let Some((_, payload)) = cfg.ping {
                                if write
                                    .send(Message::Text(payload.to_string().into()))
                                    .await
                                    .is_err()
                                {
                                    eprintln!("{} ping send failed", cfg.name);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("{} error: {}", cfg.name, e),
        }
        tokio::time::sleep(backoff.delay()).await;
    }
}

pub async fn binance_feed(state: Arc<RwLock<PriceState>>) {
    run_ws_feed(
        WsCfg {
            name: "Binance",
            url: "wss://stream.binance.com:9443/ws/btcusdt@ticker",
            subscribe: None,
            // Binance server pings; tungstenite auto-pongs at protocol layer.
            ping: None,
        },
        move |text| {
            let s = state.clone();
            async move {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(price) = v["c"].as_str().and_then(|s| s.parse::<f64>().ok()) {
                        s.write().await.update("binance", price);
                    }
                }
            }
        },
    )
    .await;
}

pub async fn bybit_feed(state: Arc<RwLock<PriceState>>) {
    run_ws_feed(
        WsCfg {
            name: "Bybit",
            url: "wss://stream.bybit.com/v5/public/spot",
            subscribe: Some(r#"{"op":"subscribe","args":["tickers.BTCUSDT"]}"#),
            ping: Some((Duration::from_secs(20), r#"{"op":"ping"}"#)),
        },
        move |text| {
            let s = state.clone();
            async move {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(price) =
                        v["data"]["lastPrice"].as_str().and_then(|s| s.parse::<f64>().ok())
                    {
                        s.write().await.update("bybit", price);
                    }
                }
            }
        },
    )
    .await;
}

pub async fn okx_feed(state: Arc<RwLock<PriceState>>) {
    run_ws_feed(
        WsCfg {
            name: "OKX",
            url: "wss://ws.okx.com:8443/ws/v5/public",
            subscribe: Some(
                r#"{"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT"}]}"#,
            ),
            // OKX V5 expects a raw text frame "ping" (not JSON); server replies "pong".
            ping: Some((Duration::from_secs(20), "ping")),
        },
        move |text| {
            let s = state.clone();
            async move {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(data) = v["data"].as_array().and_then(|a| a.first()) {
                        if let Some(price) =
                            data["last"].as_str().and_then(|s| s.parse::<f64>().ok())
                        {
                            s.write().await.update("okx", price);
                        }
                    }
                }
            }
        },
    )
    .await;
}

pub async fn mexc_feed(state: Arc<RwLock<PriceState>>) {
    run_ws_feed(
        WsCfg {
            name: "MEXC",
            url: "wss://wbs.mexc.com/ws",
            subscribe: Some(
                r#"{"method":"SUBSCRIPTION","params":["spot@public.miniTicker.v3.api@BTCUSDT@UTC+8"]}"#,
            ),
            // MEXC Spot V3 uses uppercase {"method":"PING"} (different from futures).
            ping: Some((Duration::from_secs(20), r#"{"method":"PING"}"#)),
        },
        move |text| {
            let s = state.clone();
            async move {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(price) = v["d"]["c"].as_str().and_then(|s| s.parse::<f64>().ok()) {
                        s.write().await.update("mexc", price);
                    }
                }
            }
        },
    )
    .await;
}

// ── ETH/SOL multi-asset feeds ───────────────────────────────────
// Binance combined stream for ETH + SOL — single connection, lower overhead.

pub async fn binance_alt_feed(state: Arc<RwLock<PriceState>>) {
    run_ws_feed(
        WsCfg {
            name: "Binance alt (ETH+SOL)",
            url: "wss://stream.binance.com:9443/stream?streams=ethusdt@ticker/solusdt@ticker",
            subscribe: None,
            ping: None,
        },
        move |text| {
            let s = state.clone();
            async move {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(price) =
                        v["data"]["c"].as_str().and_then(|s| s.parse::<f64>().ok())
                    {
                        let stream = v["stream"].as_str().unwrap_or("");
                        let asset = if stream.starts_with("ethusdt") {
                            "ETH"
                        } else if stream.starts_with("solusdt") {
                            "SOL"
                        } else {
                            return;
                        };
                        s.write().await.update_alt(asset, "binance", price);
                    }
                }
            }
        },
    )
    .await;
}

pub async fn bybit_alt_feed(state: Arc<RwLock<PriceState>>) {
    run_ws_feed(
        WsCfg {
            name: "Bybit alt (ETH+SOL)",
            url: "wss://stream.bybit.com/v5/public/spot",
            subscribe: Some(r#"{"op":"subscribe","args":["tickers.ETHUSDT","tickers.SOLUSDT"]}"#),
            ping: Some((Duration::from_secs(20), r#"{"op":"ping"}"#)),
        },
        move |text| {
            let s = state.clone();
            async move {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let (Some(symbol), Some(price)) = (
                        v["data"]["symbol"].as_str(),
                        v["data"]["lastPrice"].as_str().and_then(|s| s.parse::<f64>().ok()),
                    ) {
                        let asset = if symbol.starts_with("ETH") {
                            "ETH"
                        } else if symbol.starts_with("SOL") {
                            "SOL"
                        } else {
                            return;
                        };
                        s.write().await.update_alt(asset, "bybit", price);
                    }
                }
            }
        },
    )
    .await;
}

/// Fetch BTC ATM implied volatility from Deribit (free, no auth)
pub async fn fetch_deribit_iv() -> Option<f64> {
    let client = reqwest::Client::new();

    // Get index price
    let idx_resp = client
        .get("https://www.deribit.com/api/v2/public/get_index_price")
        .query(&[("index_name", "btc_usd")])
        .send()
        .await
        .ok()?;
    let idx_data: serde_json::Value = idx_resp.json().await.ok()?;
    let btc_price = idx_data["result"]["index_price"].as_f64()?;

    // Get option summaries
    let resp = client
        .get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency")
        .query(&[("currency", "BTC"), ("kind", "option")])
        .send()
        .await
        .ok()?;
    let data: serde_json::Value = resp.json().await.ok()?;
    let results = data["result"].as_array()?;

    let mut ivs = Vec::new();
    for opt in results {
        let iv = opt["mark_iv"].as_f64().unwrap_or(0.0);
        if iv <= 0.0 { continue; }

        let name = opt["instrument_name"].as_str().unwrap_or("");
        let parts: Vec<&str> = name.split('-').collect();
        if parts.len() < 4 { continue; }

        let strike: f64 = parts[2].parse().unwrap_or(0.0);
        if strike <= 0.0 { continue; }

        let ratio = strike / btc_price;
        if ratio < 0.95 || ratio > 1.05 { continue; }

        ivs.push(iv / 100.0); // Deribit reports as percentage
    }

    if ivs.is_empty() { return None; }
    Some(ivs.iter().sum::<f64>() / ivs.len() as f64)
}
