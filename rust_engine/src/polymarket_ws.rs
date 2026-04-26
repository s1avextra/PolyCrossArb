//! Polymarket L2 book WebSocket feed.
//!
//! Subscribes to real-time order book updates for active candle token_ids.
//! Stores full L2 (bids + asks per token) so paper fills can walk the same
//! book the live exchange would see — closing the price-staleness gap that
//! drove the 2026-04-26 Rust port.
//!
//! Resubscribes whenever the tracked-tokens list changes (i.e. the cycle
//! loop's contract refresh injects a new candle window). Prior version
//! waited for a disconnect to reload the list.
//!
//! WS protocol:
//!   Subscribe: {"type":"subscribe","channel":"book","assets_ids":[...]}
//!   Update:    {"type":"book","data":{"asset_id":..., "bids":[...], "asks":[...]}}
//!   Tick:      {"type":"price_change","data":{"asset_id":..., "changes":[...]}}

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, RwLock};
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::connect_async;

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const PING_INTERVAL: Duration = Duration::from_secs(10);
const STALE_AFTER: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenBookState {
    pub best_bid: f64,
    pub best_ask: f64,
    pub mid: f64,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub last_update_us: u64,
}

impl TokenBookState {
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

pub type SharedBookState = Arc<RwLock<HashMap<String, TokenBookState>>>;

pub fn new_shared_book() -> SharedBookState {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Notifier handed back to the cycle loop. Call `notify_one` after mutating
/// `tracked_tokens` to make the WS feed reconnect with the new subscription.
pub fn new_subscription_notify() -> Arc<Notify> {
    Arc::new(Notify::new())
}

#[derive(Serialize)]
struct SubscribeMsg {
    #[serde(rename = "type")]
    msg_type: &'static str,
    channel: &'static str,
    assets_ids: Vec<String>,
}

#[derive(Deserialize)]
struct WsEnvelope {
    #[serde(rename = "type")]
    msg_type: Option<String>,
    data: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct BookSnapshot {
    asset_id: Option<String>,
    bids: Option<Vec<RawLevel>>,
    asks: Option<Vec<RawLevel>>,
}

#[derive(Deserialize)]
struct PriceChange {
    asset_id: Option<String>,
    changes: Option<Vec<ChangeEntry>>,
}

#[derive(Deserialize)]
struct ChangeEntry {
    price: String,
    side: String, // "BUY" or "SELL"
    size: String,
}

#[derive(Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

fn parse_levels(raw: &Option<Vec<RawLevel>>, descending: bool) -> Vec<BookLevel> {
    let Some(levels) = raw else { return Vec::new() };
    let mut out: Vec<BookLevel> = levels
        .iter()
        .filter_map(|l| {
            let p = l.price.parse::<f64>().ok()?;
            let s = l.size.parse::<f64>().ok()?;
            if p > 0.0 {
                Some(BookLevel { price: p, size: s })
            } else {
                None
            }
        })
        .collect();
    if descending {
        out.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
    } else {
        out.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));
    }
    out
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

fn apply_price_change(state: &mut TokenBookState, changes: &[ChangeEntry]) {
    for ch in changes {
        let Ok(price) = ch.price.parse::<f64>() else { continue };
        let Ok(size) = ch.size.parse::<f64>() else { continue };
        let descending = matches!(ch.side.as_str(), "BUY");
        let levels = if descending { &mut state.bids } else { &mut state.asks };
        if let Some(idx) = levels.iter().position(|l| (l.price - price).abs() < 1e-9) {
            if size <= 0.0 {
                levels.remove(idx);
            } else {
                levels[idx].size = size;
            }
        } else if size > 0.0 {
            levels.push(BookLevel { price, size });
            if descending {
                levels.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
            } else {
                levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));
            }
        }
    }
    state.best_bid = state.bids.first().map(|l| l.price).unwrap_or(0.0);
    state.best_ask = state.asks.first().map(|l| l.price).unwrap_or(0.0);
    state.mid = if state.best_bid > 0.0 && state.best_ask > 0.0 {
        (state.best_bid + state.best_ask) / 2.0
    } else if state.best_bid > 0.0 {
        state.best_bid
    } else {
        state.best_ask
    };
    state.last_update_us = now_us();
}

/// Run the Polymarket WS feed.
///
/// `tracked_tokens` is read on each (re)connect; mutate it from outside and
/// call `resubscribe.notify_one()` to force a reconnect with the new set.
pub async fn polymarket_book_feed(
    book_state: SharedBookState,
    tracked_tokens: Arc<RwLock<Vec<String>>>,
    resubscribe: Arc<Notify>,
) {
    let mut backoff = Duration::from_millis(500);

    loop {
        let ids = tracked_tokens.read().await.clone();
        if ids.is_empty() {
            tokio::select! {
                _ = resubscribe.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
            continue;
        }

        let session = run_session(&book_state, ids, &resubscribe).await;
        match session {
            Ok(()) => {
                backoff = Duration::from_millis(500);
            }
            Err(e) => {
                tracing::warn!(error = %e, "polymarket_ws session error; backing off");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

async fn run_session(
    book_state: &SharedBookState,
    ids: Vec<String>,
    resubscribe: &Arc<Notify>,
) -> Result<(), String> {
    let (ws, _) = connect_async(POLYMARKET_WS_URL)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let (mut write, mut read) = ws.split();
    let sub = SubscribeMsg {
        msg_type: "subscribe",
        channel: "book",
        assets_ids: ids,
    };
    let payload = serde_json::to_string(&sub).map_err(|e| format!("encode: {e}"))?;
    write
        .send(Message::Text(payload.into()))
        .await
        .map_err(|e| format!("send sub: {e}"))?;

    let mut last_msg = Instant::now();
    let mut ping_timer = tokio::time::interval(PING_INTERVAL);
    ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = resubscribe.notified() => {
                tracing::info!("polymarket_ws resubscribe requested");
                return Ok(());
            }
            _ = ping_timer.tick() => {
                if last_msg.elapsed() > STALE_AFTER {
                    return Err("stale (no frames)".into());
                }
                let _ = write.send(Message::Ping(Vec::new().into())).await;
            }
            msg = timeout(Duration::from_secs(60), read.next()) => {
                match msg {
                    Ok(Some(Ok(m))) => {
                        last_msg = Instant::now();
                        handle_frame(book_state, m).await;
                    }
                    Ok(Some(Err(e))) => return Err(format!("ws read: {e}")),
                    Ok(None) => return Err("ws closed".into()),
                    Err(_) => return Err("read timeout".into()),
                }
            }
        }
    }
}

async fn handle_frame(book_state: &SharedBookState, m: Message) {
    let Ok(text) = m.into_text() else { return };
    // Polymarket sometimes sends arrays of envelopes
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }
    if trimmed.starts_with('[') {
        let Ok(arr) = serde_json::from_str::<Vec<WsEnvelope>>(trimmed) else { return };
        for env in arr {
            apply_envelope(book_state, env).await;
        }
    } else {
        let Ok(env) = serde_json::from_str::<WsEnvelope>(trimmed) else { return };
        apply_envelope(book_state, env).await;
    }
}

async fn apply_envelope(book_state: &SharedBookState, env: WsEnvelope) {
    let Some(t) = env.msg_type.as_deref() else { return };
    let Some(data) = env.data else { return };
    match t {
        "book" => {
            let Ok(snap): Result<BookSnapshot, _> = serde_json::from_value(data) else { return };
            let Some(asset_id) = snap.asset_id else { return };
            let bids = parse_levels(&snap.bids, true);
            let asks = parse_levels(&snap.asks, false);
            let best_bid = bids.first().map(|l| l.price).unwrap_or(0.0);
            let best_ask = asks.first().map(|l| l.price).unwrap_or(0.0);
            let mid = if best_bid > 0.0 && best_ask > 0.0 {
                (best_bid + best_ask) / 2.0
            } else if best_bid > 0.0 {
                best_bid
            } else {
                best_ask
            };
            let state = TokenBookState {
                best_bid,
                best_ask,
                mid,
                bids,
                asks,
                last_update_us: now_us(),
            };
            let mut map = book_state.write().await;
            map.insert(asset_id, state);
        }
        "price_change" => {
            let Ok(pc): Result<PriceChange, _> = serde_json::from_value(data) else { return };
            let Some(asset_id) = pc.asset_id else { return };
            let Some(changes) = pc.changes else { return };
            let mut map = book_state.write().await;
            let entry = map.entry(asset_id).or_default();
            apply_price_change(entry, &changes);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_book_snapshot() {
        let levels = Some(vec![
            RawLevel { price: "0.51".into(), size: "100".into() },
            RawLevel { price: "0.50".into(), size: "200".into() },
        ]);
        let bids = parse_levels(&levels, true);
        assert_eq!(bids.len(), 2);
        assert!((bids[0].price - 0.51).abs() < 1e-9);
        assert!((bids[1].price - 0.50).abs() < 1e-9);
    }

    #[test]
    fn applies_price_change_inserts_and_removes() {
        let mut s = TokenBookState::default();
        s.bids = vec![BookLevel { price: 0.50, size: 100.0 }];
        s.asks = vec![BookLevel { price: 0.52, size: 50.0 }];
        let changes = vec![
            ChangeEntry { price: "0.51".into(), side: "BUY".into(), size: "150".into() },
            ChangeEntry { price: "0.52".into(), side: "SELL".into(), size: "0".into() },
        ];
        apply_price_change(&mut s, &changes);
        assert_eq!(s.bids.len(), 2);
        assert!((s.best_bid - 0.51).abs() < 1e-9);
        assert!(s.asks.is_empty());
    }

    #[test]
    fn vwap_fills_or_returns_none() {
        let mut s = TokenBookState::default();
        s.asks = vec![
            BookLevel { price: 0.5, size: 100.0 },
            BookLevel { price: 0.6, size: 50.0 },
        ];
        let v = s.vwap(BookSide::Ask, 100.0).unwrap();
        assert!((v - 0.5).abs() < 1e-9);
        let v2 = s.vwap(BookSide::Ask, 130.0).unwrap();
        assert!((v2 - ((0.5 * 100.0 + 0.6 * 30.0) / 130.0)).abs() < 1e-9);
        assert!(s.vwap(BookSide::Ask, 200.0).is_none());
    }
}
