//! Direct CLOB order placement — bypasses Python for the hot path.
//!
//! When the Rust engine detects an edge, it places orders directly
//! via the Polymarket CLOB API instead of signaling Python.
//!
//! Latency path: signal detection (~1µs) → order build (~100µs) →
//!               HTTP POST (~1-5ms from Dublin) = ~5ms total
//!
//! The Python orchestrator still handles:
//!   - Market scanning / contract discovery
//!   - Risk management
//!   - Position tracking / state persistence
//!   - Monitoring / alerting

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// CLOB order placement client with connection pre-warming
pub struct ClobClient {
    client: Client,
    base_url: String,
    api_key: String,
    api_secret: String,
    api_passphrase: String,
    /// Track order latencies for monitoring
    pub latencies: Vec<u64>,
    /// Pre-warmed: have we sent a test request to prime the connection?
    warmed: bool,
}

#[derive(Debug, Serialize)]
struct OrderRequest {
    token_id: String,
    price: f64,
    size: f64,
    side: String,       // "BUY" or "SELL"
    order_type: String, // "GTC" (maker) or "FOK" (taker)
}

#[derive(Debug, Deserialize)]
pub struct OrderResponse {
    #[serde(rename = "orderID")]
    pub order_id: Option<String>,
    pub id: Option<String>,
    pub error: Option<String>,
}

impl ClobClient {
    pub fn new(
        base_url: &str,
        api_key: &str,
        api_secret: &str,
        api_passphrase: &str,
    ) -> Self {
        // Build client with connection pooling and HTTP/2
        let client = Client::builder()
            .pool_max_idle_per_host(5)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .tcp_nodelay(true)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            base_url: base_url.to_string(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            api_passphrase: api_passphrase.to_string(),
            latencies: Vec::with_capacity(1000),
            warmed: false,
        }
    }

    /// Pre-warm the connection pool by sending a lightweight request.
    /// First request is ~70% slower due to TLS handshake + TCP setup.
    pub async fn warm_connection(&mut self) {
        if self.warmed {
            return;
        }
        let url = format!("{}/time", self.base_url);
        match self.client.get(&url).send().await {
            Ok(_) => {
                self.warmed = true;
                eprintln!("CLOB connection pre-warmed");
            }
            Err(e) => eprintln!("CLOB warm failed: {}", e),
        }
    }

    /// Place a maker limit order (GTC — 0% fee).
    ///
    /// Returns the order ID on success.
    /// This is the hot path — every microsecond matters.
    pub async fn place_maker_order(
        &mut self,
        token_id: &str,
        price: f64,
        size: f64,
        side: &str,
    ) -> Result<String, String> {
        let t0 = Instant::now();

        let order = OrderRequest {
            token_id: token_id.to_string(),
            price,
            size,
            side: side.to_uppercase(),
            order_type: "GTC".to_string(),
        };

        let url = format!("{}/order", self.base_url);
        let result = self.client
            .post(&url)
            .header("POLY-API-KEY", &self.api_key)
            .header("POLY-API-SECRET", &self.api_secret)
            .header("POLY-API-PASSPHRASE", &self.api_passphrase)
            .json(&order)
            .send()
            .await;

        let latency_us = t0.elapsed().as_micros() as u64;
        self.latencies.push(latency_us);

        match result {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();

                if !status.is_success() {
                    return Err(format!("HTTP {}: {}", status, &body[..100.min(body.len())]));
                }

                match serde_json::from_str::<OrderResponse>(&body) {
                    Ok(order_resp) => {
                        if let Some(err) = order_resp.error {
                            return Err(err);
                        }
                        let oid = order_resp.order_id
                            .or(order_resp.id)
                            .unwrap_or_default();
                        eprintln!("Order placed in {}µs: {} {} {:.1}@{:.4} id={}",
                            latency_us, side, token_id.get(..16).unwrap_or(token_id),
                            size, price, oid.get(..16).unwrap_or(&oid));
                        Ok(oid)
                    }
                    Err(e) => Err(format!("Parse error: {}", e)),
                }
            }
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }

    /// Get average order placement latency in microseconds
    pub fn avg_latency_us(&self) -> u64 {
        if self.latencies.is_empty() { return 0; }
        self.latencies.iter().sum::<u64>() / self.latencies.len() as u64
    }

    /// Get p99 order placement latency
    pub fn p99_latency_us(&self) -> u64 {
        if self.latencies.is_empty() { return 0; }
        let mut sorted = self.latencies.clone();
        sorted.sort();
        sorted[sorted.len() * 99 / 100]
    }
}

/// Shared CLOB client wrapped for async access
pub type SharedClobClient = Arc<RwLock<ClobClient>>;

pub fn create_shared_client(
    base_url: &str,
    api_key: &str,
    api_secret: &str,
    api_passphrase: &str,
) -> SharedClobClient {
    Arc::new(RwLock::new(ClobClient::new(base_url, api_key, api_secret, api_passphrase)))
}
