//! Polymarket fee calculation.
//!
//! Uses the verified binary fee formula:
//!   fee = qty × fee_rate × p × (1 − p)
//! Maximum at p = 0.5, symmetric around p=0.5.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use once_cell::sync::Lazy;
use reqwest::Client;

const CLOB_BASE: &str = "https://clob.polymarket.com";
const CACHE_TTL_SECS: u64 = 300;

#[derive(Clone, Copy)]
struct CachedFee {
    fee: f64,
    fetched_at: u64,
}

static FEE_CACHE: Lazy<Mutex<HashMap<String, CachedFee>>> = Lazy::new(|| Mutex::new(HashMap::new()));

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Polymarket binary-option fee formula.
/// fee = shares × fee_rate × p × (1 − p)
pub fn polymarket_fee(shares: f64, price: f64, fee_rate: f64) -> f64 {
    if fee_rate <= 0.0 || shares <= 0.0 {
        return 0.0;
    }
    let p = price.clamp(0.0, 1.0);
    let fee = shares * fee_rate * p * (1.0 - p);
    let fee = (fee * 100_000.0).round() / 100_000.0;
    if fee > 0.0 && fee < 0.00001 {
        0.00001
    } else {
        fee
    }
}

/// Hardcoded category fallback (used when live API is unreachable).
pub fn fallback_rate(category: &str) -> f64 {
    match category.to_lowercase().trim() {
        "crypto" => 0.072,
        "sports" => 0.030,
        "finance" => 0.040,
        "politics" => 0.040,
        "economics" => 0.030,
        "culture" => 0.050,
        "weather" => 0.025,
        "tech" => 0.040,
        "geopolitics" => 0.000,
        "mentions" => 0.250,
        _ => 0.200,
    }
}

pub fn fallback_fee(shares: f64, price: f64, category: &str) -> f64 {
    polymarket_fee(shares, price, fallback_rate(category))
}

/// Fetch the live CLOB fee rate for a token (cached for 5 min).
pub async fn get_live_fee_rate(client: &Client, token_id: &str) -> Option<f64> {
    let now = now_secs();
    if let Some(cached) = FEE_CACHE.lock().ok().and_then(|c| c.get(token_id).copied()) {
        if now - cached.fetched_at < CACHE_TTL_SECS {
            return Some(cached.fee);
        }
    }
    let url = format!("{CLOB_BASE}/fee-rate");
    let resp = client
        .get(&url)
        .query(&[("token_id", token_id)])
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: serde_json::Value = resp.json().await.ok()?;
    let fee = parse_fee_response(&v);
    if let Some(f) = fee {
        if let Ok(mut c) = FEE_CACHE.lock() {
            c.insert(token_id.to_string(), CachedFee { fee: f, fetched_at: now });
        }
    }
    fee
}

fn parse_fee_response(v: &serde_json::Value) -> Option<f64> {
    if let Some(b) = v.get("base_fee").and_then(|x| x.as_f64()) {
        return Some(b);
    }
    for key in ["fee_rate", "fee_rate_bps", "taker_fee", "fee"] {
        if let Some(num) = v.get(key).and_then(|x| x.as_f64()) {
            let mut f = num;
            if key.contains("bps") && f > 1.0 {
                f /= 10_000.0;
            }
            return Some(f);
        }
    }
    None
}

pub fn cached_fee_for_token(token_id: &str) -> Option<f64> {
    let now = now_secs();
    let c = FEE_CACHE.lock().ok()?;
    let entry = c.get(token_id).copied()?;
    if now - entry.fetched_at < CACHE_TTL_SECS {
        Some(entry.fee)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_zero_when_no_rate() {
        assert_eq!(polymarket_fee(100.0, 0.5, 0.0), 0.0);
    }

    #[test]
    fn fee_max_at_half() {
        let f50 = polymarket_fee(100.0, 0.5, 0.072);
        let f25 = polymarket_fee(100.0, 0.25, 0.072);
        let f75 = polymarket_fee(100.0, 0.75, 0.072);
        assert!(f50 > f25);
        assert!(f50 > f75);
        // symmetric
        assert!((f25 - f75).abs() < 1e-6);
    }

    #[test]
    fn fee_floor_for_tiny_positive() {
        let f = polymarket_fee(0.001, 0.5, 0.072);
        assert!(f >= 0.00001);
    }
}
