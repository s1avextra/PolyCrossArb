//! Crypto Up/Down candle market scanner.
//!
//! Finds markets like "Bitcoin Up or Down - April 4, 3:45AM-4:00AM ET" and
//! groups them by resolution time.

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::data::models::Market;

const SUPPORTED: &[(&str, &str)] = &[
    ("Bitcoin", "BTC"),
    ("Ethereum", "ETH"),
    ("Solana", "SOL"),
    ("BNB", "BNB"),
    ("XRP", "XRP"),
    ("Dogecoin", "DOGE"),
    ("Hyperliquid", "HYPE"),
    ("Monero", "XMR"),
    ("Cardano", "ADA"),
    ("Avalanche", "AVAX"),
    ("Chainlink", "LINK"),
];

static CANDLE_RE: Lazy<Regex> = Lazy::new(|| {
    let alts: Vec<String> = SUPPORTED
        .iter()
        .flat_map(|(long, short)| [regex::escape(long), regex::escape(short)])
        .collect();
    let pat = format!(
        r"(?i)({})\s+Up or Down\s*[-–—]\s*(.+?)(?:\?|$)",
        alts.join("|")
    );
    Regex::new(&pat).expect("valid regex")
});

fn prefix_to_asset(prefix: &str) -> &'static str {
    let l = prefix.to_lowercase();
    for (long, short) in SUPPORTED {
        if l == long.to_lowercase() || l == short.to_lowercase() {
            return short;
        }
    }
    "BTC"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleContract {
    pub market: Market,
    pub up_token_id: String,
    pub down_token_id: String,
    pub up_price: f64,
    pub down_price: f64,
    pub end_date: String,
    pub hours_left: f64,
    pub volume: f64,
    pub liquidity: f64,
    pub window_description: String,
    pub asset: String,
}

impl CandleContract {
    pub fn spread(&self) -> f64 {
        (self.up_price + self.down_price - 1.0).abs()
    }

    pub fn minutes_left(&self) -> f64 {
        self.hours_left * 60.0
    }
}

pub fn scan_candle_markets(
    markets: &[Market],
    max_hours: f64,
    min_liquidity: f64,
) -> Vec<CandleContract> {
    let now = Utc::now();
    let mut contracts = Vec::new();

    for m in markets {
        if !m.active || m.closed {
            continue;
        }
        let Some(caps) = CANDLE_RE.captures(&m.question) else { continue };
        let asset = prefix_to_asset(caps.get(1).map(|m| m.as_str()).unwrap_or(""));
        let window_desc = caps.get(2).map(|m| m.as_str().trim().to_string()).unwrap_or_default();

        if m.outcomes.len() != 2 {
            continue;
        }

        let mut up_idx = None;
        let mut down_idx = None;
        for (i, o) in m.outcomes.iter().enumerate() {
            let name = o.name.to_lowercase();
            if name.contains("up") {
                up_idx = Some(i);
            } else if name.contains("down") {
                down_idx = Some(i);
            }
        }
        let (Some(up_idx), Some(down_idx)) = (up_idx, down_idx) else {
            continue;
        };

        if m.end_date.is_empty() {
            continue;
        }
        let Some(end) = parse_end_date(&m.end_date) else {
            continue;
        };
        let hours_left = (end - now).num_seconds() as f64 / 3600.0;
        if hours_left <= 0.0 || hours_left > max_hours {
            continue;
        }

        if m.liquidity < min_liquidity {
            continue;
        }

        contracts.push(CandleContract {
            market: m.clone(),
            up_token_id: m.outcomes[up_idx].token_id.clone(),
            down_token_id: m.outcomes[down_idx].token_id.clone(),
            up_price: m.outcomes[up_idx].price,
            down_price: m.outcomes[down_idx].price,
            end_date: m.end_date.clone(),
            hours_left,
            volume: m.volume,
            liquidity: m.liquidity,
            window_description: window_desc,
            asset: asset.to_string(),
        });
    }

    contracts.sort_by(|a, b| {
        a.hours_left.partial_cmp(&b.hours_left).unwrap_or(std::cmp::Ordering::Equal)
    });

    tracing::info!(count = contracts.len(), max_hours, "candle scanner");
    contracts
}

fn parse_end_date(s: &str) -> Option<DateTime<Utc>> {
    let normalized = s.replace('Z', "+00:00");
    DateTime::parse_from_rfc3339(&normalized)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::models::{Market, Outcome};

    fn mk_market(question: &str, end_date: &str, liquidity: f64) -> Market {
        Market {
            condition_id: "0xabc".into(),
            question: question.into(),
            slug: "x".into(),
            outcomes: vec![
                Outcome { token_id: "u".into(), name: "Up".into(), price: 0.5, order_book: None },
                Outcome { token_id: "d".into(), name: "Down".into(), price: 0.5, order_book: None },
            ],
            tags: vec![],
            category: String::new(),
            active: true,
            closed: false,
            volume: 1000.0,
            liquidity,
            end_date: end_date.into(),
            event_slug: String::new(),
            event_id: String::new(),
            event_title: String::new(),
            group_slug: String::new(),
            neg_risk: false,
            neg_risk_augmented: false,
        }
    }

    #[test]
    fn parses_btc_candle() {
        let future = (Utc::now() + chrono::Duration::minutes(30)).to_rfc3339();
        let m = mk_market("Bitcoin Up or Down - April 4, 3AM ET?", &future, 500.0);
        let cs = scan_candle_markets(&[m], 2.0, 100.0);
        assert_eq!(cs.len(), 1);
        assert_eq!(cs[0].asset, "BTC");
    }

    #[test]
    fn skips_low_liquidity() {
        let future = (Utc::now() + chrono::Duration::minutes(30)).to_rfc3339();
        let m = mk_market("Ethereum Up or Down - April 4, 3AM ET?", &future, 50.0);
        assert!(scan_candle_markets(&[m], 2.0, 100.0).is_empty());
    }

    #[test]
    fn parses_eth_and_alt() {
        let future = (Utc::now() + chrono::Duration::minutes(30)).to_rfc3339();
        let m = mk_market("ETH Up or Down - April 4, 3AM ET?", &future, 500.0);
        let cs = scan_candle_markets(&[m], 2.0, 100.0);
        assert_eq!(cs.len(), 1);
        assert_eq!(cs[0].asset, "ETH");
    }

    #[test]
    fn filters_past_end_date() {
        let past = (Utc::now() - chrono::Duration::minutes(30)).to_rfc3339();
        let m = mk_market("Bitcoin Up or Down - April 4, 3AM ET?", &past, 500.0);
        assert!(scan_candle_markets(&[m], 2.0, 100.0).is_empty());
    }
}
