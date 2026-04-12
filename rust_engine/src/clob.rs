//! Direct CLOB order placement — bypasses Python for the hot path.
//!
//! When the Rust engine detects an edge, it places orders directly
//! via the Polymarket CLOB API instead of signaling Python.
//!
//! Latency path: signal detection (~1µs) → order build + sign (~50µs) →
//!               HTTP POST (~1-5ms from Dublin) = ~5ms total
//!
//! The Python orchestrator still handles:
//!   - Market scanning / contract discovery
//!   - Risk management
//!   - Position tracking / state persistence
//!   - Monitoring / alerting

use k256::ecdsa::SigningKey;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

use crate::signing;

/// CLOB order placement client with connection pre-warming and EIP-712 signing.
pub struct ClobClient {
    client: Client,
    base_url: String,
    api_key: String,
    api_secret: String,
    api_passphrase: String,
    signing_key: Option<SigningKey>,
    maker_address: String,
    /// Track order latencies for monitoring
    pub latencies: Vec<u64>,
    /// Pre-warmed: have we sent a test request to prime the connection?
    warmed: bool,
}

/// Signed order body for the CLOB /order endpoint.
#[derive(Debug, Serialize)]
struct SignedOrderRequest {
    order: OrderPayload,
    signature: String,
    owner: String,        // maker address
    #[serde(rename = "orderType")]
    order_type: String,   // "GTC" or "FOK"
}

#[derive(Debug, Serialize)]
struct OrderPayload {
    salt: String,
    maker: String,
    signer: String,
    taker: String,
    #[serde(rename = "tokenId")]
    token_id: String,
    #[serde(rename = "makerAmount")]
    maker_amount: String,
    #[serde(rename = "takerAmount")]
    taker_amount: String,
    expiration: String,
    nonce: String,
    #[serde(rename = "feeRateBps")]
    fee_rate_bps: String,
    side: String,
    #[serde(rename = "signatureType")]
    signature_type: String,
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
            signing_key: None,
            maker_address: String::new(),
            latencies: Vec::with_capacity(1000),
            warmed: false,
        }
    }

    /// Set the private key for EIP-712 order signing.
    pub fn set_signing_key(&mut self, hex_key: &str) {
        if let Some(key) = signing::parse_private_key(hex_key) {
            let addr = signing::address_from_key(&key);
            self.maker_address = format!("0x{}", hex::encode(addr));
            self.signing_key = Some(key);
            eprintln!("CLOB signing key set: {}", self.maker_address);
        } else {
            eprintln!("CLOB signing key parse failed");
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

    /// Build HMAC-SHA256 authenticated headers for a request.
    fn auth_headers(&self, method: &str, path: &str, body: &str) -> Vec<(String, String)> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();

        let signature =
            signing::hmac_sign_request(&self.api_secret, &timestamp, method, path, body);

        vec![
            ("POLY-ADDRESS".into(), self.maker_address.clone()),
            ("POLY-SIGNATURE".into(), signature),
            ("POLY-TIMESTAMP".into(), timestamp),
            ("POLY-API-KEY".into(), self.api_key.clone()),
            ("POLY-PASSPHRASE".into(), self.api_passphrase.clone()),
        ]
    }

    /// Place a GTC maker limit order (0% fee) with EIP-712 signing.
    pub async fn place_maker_order(
        &mut self,
        token_id: &str,
        price: f64,
        size: f64,
        side: &str,
        neg_risk: bool,
    ) -> Result<String, String> {
        self.place_order_internal(token_id, price, size, side, "GTC", neg_risk, 0.01)
            .await
    }

    /// Place a FOK taker order (crosses the spread immediately).
    pub async fn place_taker_order(
        &mut self,
        token_id: &str,
        price: f64,
        size: f64,
        side: &str,
        neg_risk: bool,
    ) -> Result<String, String> {
        self.place_order_internal(token_id, price, size, side, "FOK", neg_risk, 0.01)
            .await
    }

    /// Internal: build, sign, and submit an order.
    async fn place_order_internal(
        &mut self,
        token_id: &str,
        price: f64,
        size: f64,
        side: &str,
        order_type: &str, // "GTC" or "FOK"
        neg_risk: bool,
        tick_size: f64,
    ) -> Result<String, String> {
        let key = self
            .signing_key
            .as_ref()
            .ok_or_else(|| "No signing key set".to_string())?;

        let t0 = Instant::now();

        // Build and sign the order
        let fee_bps = if order_type == "GTC" { 0 } else { 200 }; // 0% maker, 2% taker
        let order = signing::build_order(key, token_id, price, size, side, fee_bps, neg_risk, tick_size);
        let signed = signing::sign_order(&order, key, neg_risk);

        let sign_us = t0.elapsed().as_micros();

        // Serialize to CLOB API format
        let payload = SignedOrderRequest {
            order: OrderPayload {
                salt: signed.order.salt.to_string(),
                maker: format!("0x{}", hex::encode(signed.order.maker)),
                signer: format!("0x{}", hex::encode(signed.order.signer)),
                taker: format!("0x{}", hex::encode(signed.order.taker)),
                token_id: signed.order.token_id.clone(),
                maker_amount: signed.order.maker_amount.to_string(),
                taker_amount: signed.order.taker_amount.to_string(),
                expiration: signed.order.expiration.to_string(),
                nonce: signed.order.nonce.to_string(),
                fee_rate_bps: signed.order.fee_rate_bps.to_string(),
                side: signed.order.side.to_string(),
                signature_type: signed.order.signature_type.to_string(),
            },
            signature: format!("0x{}", signed.signature),
            owner: self.maker_address.clone(),
            order_type: order_type.to_string(),
        };

        let body = serde_json::to_string(&payload).map_err(|e| format!("Serialize: {}", e))?;

        // Build auth headers
        let headers = self.auth_headers("POST", "/order", &body);

        let url = format!("{}/order", self.base_url);
        let mut req = self.client.post(&url);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("Content-Type", "application/json");
        req = req.body(body);

        let result = req.send().await;
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
                        let oid = order_resp
                            .order_id
                            .or(order_resp.id)
                            .unwrap_or_default();
                        eprintln!(
                            "Order {} placed in {}µs (sign: {}µs): {} {} {:.1}@{:.4} id={}",
                            order_type,
                            latency_us,
                            sign_us,
                            side,
                            token_id.get(..16).unwrap_or(token_id),
                            size,
                            price,
                            oid.get(..16).unwrap_or(&oid)
                        );
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
        if self.latencies.is_empty() {
            return 0;
        }
        self.latencies.iter().sum::<u64>() / self.latencies.len() as u64
    }

    /// Get p99 order placement latency
    pub fn p99_latency_us(&self) -> u64 {
        if self.latencies.is_empty() {
            return 0;
        }
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
    Arc::new(RwLock::new(ClobClient::new(
        base_url,
        api_key,
        api_secret,
        api_passphrase,
    )))
}
