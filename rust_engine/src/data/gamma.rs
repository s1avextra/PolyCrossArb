//! Polymarket Gamma REST client (market discovery).
//!
//! The CLOB REST endpoints (`/book`, `/midpoint`) used to live here, but the
//! pipeline now reads books off the WebSocket feed in `polymarket_ws.rs`,
//! so REST book/midpoint queries were removed during the cleanup audit.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde_json::Value;

use crate::data::models::{Market, Outcome};

const GAMMA_MARKETS: &str = "/markets";

#[derive(Clone)]
pub struct GammaClient {
    gamma_url: String,
    http: Client,
    max_retries: u32,
}

impl GammaClient {
    pub fn new(gamma_url: impl Into<String>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .expect("reqwest client builds");
        Self {
            gamma_url: gamma_url.into().trim_end_matches('/').to_string(),
            http,
            max_retries: 3,
        }
    }

    async fn get_with_retry(&self, path: &str, params: &[(&str, String)]) -> Result<Value> {
        let url = format!("{}{path}", self.gamma_url);
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 0..self.max_retries {
            let resp = self.http.get(&url).query(params).send().await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    return r.json::<Value>().await.context("decode gamma json");
                }
                Ok(r) if r.status().as_u16() == 429 => {
                    let wait = Duration::from_secs(1u64 << attempt);
                    tracing::warn!(attempt, ?wait, "Gamma rate limited");
                    tokio::time::sleep(wait).await;
                }
                Ok(r) => {
                    last_err = Some(anyhow!("HTTP {} from {}", r.status(), url));
                    tokio::time::sleep(Duration::from_millis(500 * (attempt + 1) as u64)).await;
                }
                Err(e) => {
                    last_err = Some(anyhow::Error::new(e));
                    tokio::time::sleep(Duration::from_millis(500 * (attempt + 1) as u64)).await;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("Gamma request failed without specific error")))
    }

    /// Fetch markets sorted by endDate ascending — the fast path for candle
    /// discovery. Stops paginating once the last page's endDate exceeds
    /// `now + max_hours`. Filters out markets with degenerate prices /
    /// missing tokens / liquidity below `min_liquidity`.
    pub async fn fetch_markets_by_end_date(
        &self,
        max_hours: f64,
        min_liquidity: f64,
    ) -> Result<Vec<Market>> {
        let now = chrono::Utc::now().timestamp() as f64;
        let cutoff_ts = now + max_hours * 3600.0;
        let mut all: Vec<Market> = Vec::new();
        let mut offset = 0u32;
        let page_size = 100u32;

        loop {
            let params = vec![
                ("limit", page_size.to_string()),
                ("offset", offset.to_string()),
                ("active", "true".to_string()),
                ("closed", "false".to_string()),
                ("order", "endDate".to_string()),
                ("ascending", "true".to_string()),
            ];
            let v = self.get_with_retry(GAMMA_MARKETS, &params).await?;
            let items = unwrap_market_list(v);
            if items.is_empty() {
                break;
            }

            for raw in &items {
                let Some(m) = parse_gamma_market(raw) else { continue };
                if m.outcomes.is_empty() || m.outcomes.iter().all(|o| o.price == 0.0) {
                    continue;
                }
                if m.outcomes.iter().any(|o| o.token_id.is_empty()) {
                    continue;
                }
                if m.liquidity < min_liquidity {
                    continue;
                }
                all.push(m);
            }

            if let Some(last) = items.last() {
                let end_str = last
                    .get("endDate")
                    .or_else(|| last.get("end_date"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if let Some(end_ts) = parse_iso8601(end_str) {
                    if end_ts > cutoff_ts {
                        break;
                    }
                }
            }

            if items.len() < page_size as usize {
                break;
            }
            offset += page_size;
        }

        tracing::info!(count = all.len(), max_hours, "Gamma markets-by-endDate fetched");
        Ok(all)
    }
}

fn unwrap_market_list(v: Value) -> Vec<Value> {
    if let Value::Array(arr) = v {
        return arr;
    }
    if let Some(arr) = v.get("data").and_then(|x| x.as_array()) {
        return arr.clone();
    }
    if let Some(arr) = v.get("markets").and_then(|x| x.as_array()) {
        return arr.clone();
    }
    Vec::new()
}

fn parse_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_json_or_csv(v: Option<&Value>) -> Vec<String> {
    let Some(v) = v else { return Vec::new() };
    if let Value::Array(arr) = v {
        return arr
            .iter()
            .map(|x| match x {
                Value::String(s) => s.clone(),
                _ => x.to_string(),
            })
            .collect();
    }
    let Some(s) = v.as_str() else { return Vec::new() };
    let s = s.trim();
    if s.is_empty() {
        return Vec::new();
    }
    if s.starts_with('[') {
        if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(s) {
            return arr
                .into_iter()
                .map(|x| match x {
                    Value::String(s) => s,
                    other => other.to_string(),
                })
                .collect();
        }
    }
    s.split(',')
        .map(|p| p.trim().trim_matches('"').to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

pub fn parse_gamma_market(raw: &Value) -> Option<Market> {
    let condition_id = raw
        .get("conditionId")
        .or_else(|| raw.get("condition_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let question = raw.get("question").and_then(|v| v.as_str()).unwrap_or("").to_string();
    if condition_id.is_empty() || question.is_empty() {
        return None;
    }

    let outcome_names = parse_json_or_csv(raw.get("outcomes"));
    let outcome_prices_raw =
        parse_json_or_csv(raw.get("outcomePrices").or_else(|| raw.get("outcome_prices")));
    let outcome_prices: Vec<f64> = outcome_prices_raw
        .iter()
        .map(|s| s.parse::<f64>().unwrap_or(0.0))
        .collect();
    let token_ids = parse_json_or_csv(raw.get("clobTokenIds").or_else(|| raw.get("clob_token_ids")));

    let outcomes: Vec<Outcome> = outcome_names
        .iter()
        .enumerate()
        .map(|(i, name)| Outcome {
            token_id: token_ids.get(i).cloned().unwrap_or_default(),
            name: name.clone(),
            price: outcome_prices.get(i).copied().unwrap_or(0.0),
        })
        .collect();

    let tags_raw = raw.get("tags");
    let tags: Vec<String> = match tags_raw {
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|x| x.as_str().map(str::to_string))
            .collect(),
        Some(Value::String(s)) => s
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect(),
        _ => Vec::new(),
    };

    let mut event_slug = String::new();
    let mut event_id = String::new();
    let mut event_title = String::new();
    let mut neg_risk_augmented = false;
    if let Some(Value::Array(events)) = raw.get("events") {
        if let Some(ev) = events.first() {
            event_slug = ev.get("slug").and_then(|v| v.as_str()).unwrap_or("").to_string();
            event_id = ev
                .get("id")
                .map(|v| match v {
                    Value::Number(n) => n.to_string(),
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default();
            event_title = ev.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
            neg_risk_augmented = ev
                .get("negRiskAugmented")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
        }
    }

    Some(Market {
        condition_id,
        question,
        slug: raw.get("slug").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        outcomes,
        tags,
        category: raw.get("category").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        active: raw.get("active").and_then(|v| v.as_bool()).unwrap_or(true),
        closed: raw.get("closed").and_then(|v| v.as_bool()).unwrap_or(false),
        volume: raw.get("volume").and_then(parse_f64).unwrap_or(0.0),
        liquidity: raw.get("liquidity").and_then(parse_f64).unwrap_or(0.0),
        end_date: raw
            .get("endDate")
            .or_else(|| raw.get("end_date"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        event_slug,
        event_id,
        event_title,
        group_slug: raw
            .get("groupSlug")
            .or_else(|| raw.get("group_slug"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        neg_risk: raw.get("negRisk").and_then(|v| v.as_bool()).unwrap_or(false),
        neg_risk_augmented,
    })
}

fn parse_iso8601(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp() as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_csv_and_json() {
        let v = serde_json::json!(["Yes", "No"]);
        assert_eq!(parse_json_or_csv(Some(&v)), vec!["Yes", "No"]);

        let v = serde_json::json!("[\"Yes\",\"No\"]");
        assert_eq!(parse_json_or_csv(Some(&v)), vec!["Yes", "No"]);

        let v = serde_json::json!("Yes,No");
        assert_eq!(parse_json_or_csv(Some(&v)), vec!["Yes", "No"]);

        assert!(parse_json_or_csv(None).is_empty());
    }

    #[test]
    fn parses_gamma_market_skeleton() {
        let raw = serde_json::json!({
            "conditionId": "0xabc",
            "question": "Bitcoin Up or Down - April 4, 3AM ET?",
            "outcomes": "[\"Up\",\"Down\"]",
            "outcomePrices": "[\"0.5\",\"0.5\"]",
            "clobTokenIds": "[\"t1\",\"t2\"]",
            "active": true,
            "closed": false,
            "endDate": "2026-04-04T07:00:00Z",
        });
        let m = parse_gamma_market(&raw).unwrap();
        assert_eq!(m.condition_id, "0xabc");
        assert_eq!(m.outcomes.len(), 2);
        assert_eq!(m.outcomes[0].token_id, "t1");
        assert_eq!(m.outcomes[0].name, "Up");
        assert!((m.outcomes[0].price - 0.5).abs() < 1e-9);
    }
}
