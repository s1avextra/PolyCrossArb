//! Domain models — parsed Polymarket markets and outcomes.
//!
//! L2 book state for live trading lives in `polymarket_ws::TokenBookState`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Outcome {
    pub token_id: String,
    pub name: String,
    pub price: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Market {
    pub condition_id: String,
    pub question: String,
    pub slug: String,
    pub outcomes: Vec<Outcome>,
    pub tags: Vec<String>,
    pub category: String,
    pub active: bool,
    pub closed: bool,
    pub volume: f64,
    pub liquidity: f64,
    pub end_date: String,
    pub event_slug: String,
    pub event_id: String,
    pub event_title: String,
    pub group_slug: String,
    pub neg_risk: bool,
    pub neg_risk_augmented: bool,
}
