//! Polymarket WebSocket L2 book feed.
//!
//! Subscribes to real-time order book updates for active candle token_ids.
//! On each update, extracts MM best bid/ask and computes staleness.
//!
//! The Polymarket WS API uses JSON-RPC style messages:
//!   Subscribe: {"type": "subscribe", "channel": "book", "assets_ids": ["token_id"]}
//!   Update:    {"type": "book", "data": {"asset_id": "...", "bids": [...], "asks": [...]}}
//!
//! This runs as a tokio task alongside the exchange feeds, feeding into
//! the same PriceState so the edge evaluator sees up-to-the-tick MM prices.

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Book state for a single token (Up or Down side).
#[derive(Debug, Clone, Default)]
pub struct TokenBookState {
    pub best_bid: f64,
    pub best_ask: f64,
    pub mid: f64,
    pub last_update_us: u64,  // microseconds since UNIX epoch
}

/// Shared book state for all tracked tokens.
pub type SharedBookState = Arc<RwLock<HashMap<String, TokenBookState>>>;

pub fn new_shared_book() -> SharedBookState {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Subscribe message for Polymarket WS.
#[derive(Debug, Serialize)]
struct SubscribeMsg {
    #[serde(rename = "type")]
    msg_type: String,
    channel: String,
    assets_ids: Vec<String>,
}

/// Incoming book snapshot/update from Polymarket WS.
#[derive(Debug, Deserialize)]
struct BookUpdate {
    #[serde(rename = "type")]
    msg_type: Option<String>,
    data: Option<BookData>,
}

#[derive(Debug, Deserialize)]
struct BookData {
    asset_id: Option<String>,
    bids: Option<Vec<PriceLevel>>,
    asks: Option<Vec<PriceLevel>>,
}

#[derive(Debug, Deserialize)]
struct PriceLevel {
    price: String,
    size: String,
}

/// Connect to Polymarket WS and subscribe to book updates for given token_ids.
/// Updates the shared book state on each message.
pub async fn polymarket_book_feed(
    book_state: SharedBookState,
    token_ids: Arc<RwLock<Vec<String>>>,
) {
    loop {
        let ids = token_ids.read().await.clone();
        if ids.is_empty() {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }

        match connect_async(POLYMARKET_WS_URL).await {
            Ok((ws, _)) => {
                eprintln!("Polymarket WS connected, subscribing to {} tokens", ids.len());
                let (mut write, mut read) = ws.split();

                // Send subscription
                let sub = SubscribeMsg {
                    msg_type: "subscribe".to_string(),
                    channel: "book".to_string(),
                    assets_ids: ids,
                };
                if let Ok(json) = serde_json::to_string(&sub) {
                    let _ = write
                        .send(tokio_tungstenite::tungstenite::Message::Text(json.into()))
                        .await;
                }

                while let Some(Ok(msg)) = read.next().await {
                    if let Ok(text) = msg.into_text() {
                        if let Ok(update) = serde_json::from_str::<BookUpdate>(&text) {
                            if let Some(data) = update.data {
                                if let Some(asset_id) = data.asset_id {
                                    let now_us = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_micros() as u64;

                                    let best_bid = data
                                        .bids
                                        .as_ref()
                                        .and_then(|b| b.first())
                                        .and_then(|l| l.price.parse::<f64>().ok())
                                        .unwrap_or(0.0);

                                    let best_ask = data
                                        .asks
                                        .as_ref()
                                        .and_then(|a| a.first())
                                        .and_then(|l| l.price.parse::<f64>().ok())
                                        .unwrap_or(0.0);

                                    let mid = if best_bid > 0.0 && best_ask > 0.0 {
                                        (best_bid + best_ask) / 2.0
                                    } else if best_bid > 0.0 {
                                        best_bid
                                    } else {
                                        best_ask
                                    };

                                    let mut books = book_state.write().await;
                                    books.insert(
                                        asset_id,
                                        TokenBookState {
                                            best_bid,
                                            best_ask,
                                            mid,
                                            last_update_us: now_us,
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Polymarket WS error: {}", e);
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}
