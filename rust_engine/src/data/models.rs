//! Domain models — parsed Polymarket markets, order books, outcomes.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OrderBookLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OrderBook {
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
}

impl OrderBook {
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|l| l.price)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|l| l.price)
    }

    pub fn mid_price(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some((b + a) / 2.0),
            (Some(b), None) => Some(b),
            (None, Some(a)) => Some(a),
            _ => None,
        }
    }

    /// Volume-weighted average price to fill `size` on `side`.
    /// Returns None if insufficient liquidity.
    pub fn vwap(&self, side: BookSide, size: f64) -> Option<f64> {
        let levels = match side {
            BookSide::Ask => &self.asks,
            BookSide::Bid => &self.bids,
        };
        let mut remaining = size;
        let mut cost = 0.0;
        for lvl in levels {
            let fill = remaining.min(lvl.size);
            cost += fill * lvl.price;
            remaining -= fill;
            if remaining <= 0.0 {
                break;
            }
        }
        if remaining > 0.0 {
            None
        } else {
            Some(cost / size)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Outcome {
    pub token_id: String,
    pub name: String,
    pub price: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_book: Option<OrderBook>,
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

impl Market {
    pub fn outcome_price_sum(&self) -> f64 {
        self.outcomes.iter().map(|o| o.price).sum()
    }
}
