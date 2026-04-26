//! Event-driven L2 replay engine.
//!
//! Walks PMXT v2 events for a token universe, maintains a per-token in-memory
//! book, and runs a strategy callback on every event. Pending orders fire
//! after a configurable static latency window — and **before** the
//! current event's update is applied, to prevent lookahead from
//! same-instant book changes.

use std::collections::BTreeMap;

use crate::backtest::fill_model::{FillReason, FillResult, OneTickTaker, OrderType, Side};
use crate::backtest::pmxt::{L2Event, L2EventBody};
use crate::execution::fees::polymarket_fee;

#[derive(Debug, Clone, Copy)]
pub struct StaticLatencyConfig {
    /// Time it takes for an order to reach the book after the strategy fires.
    pub insert_ms: u64,
}

impl Default for StaticLatencyConfig {
    fn default() -> Self {
        // Conservative Dublin-VPS-to-Polymarket-CLOB round trip.
        Self { insert_ms: 50 }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TokenBook {
    pub token_id: String,
    pub bids: BTreeMap<u64, f64>, // price * 1e9 → size, sorted ascending
    pub asks: BTreeMap<u64, f64>,
    pub best_bid: f64,
    pub best_ask: f64,
    pub last_update_ts_s: f64,
}

fn key(p: f64) -> u64 {
    (p * 1e9).round() as u64
}

impl TokenBook {
    pub fn new(token_id: impl Into<String>) -> Self {
        Self { token_id: token_id.into(), ..Default::default() }
    }

    pub fn apply_snapshot(&mut self, snap: &crate::backtest::pmxt::BookSnapshot) {
        self.bids.clear();
        self.asks.clear();
        for lv in &snap.bids {
            if lv.size > 0.0 {
                self.bids.insert(key(lv.price), lv.size);
            }
        }
        for lv in &snap.asks {
            if lv.size > 0.0 {
                self.asks.insert(key(lv.price), lv.size);
            }
        }
        self.best_bid = if snap.best_bid > 0.0 {
            snap.best_bid
        } else {
            self.bids
                .keys()
                .next_back()
                .map(|k| *k as f64 / 1e9)
                .unwrap_or(0.0)
        };
        self.best_ask = if snap.best_ask > 0.0 {
            snap.best_ask
        } else {
            self.asks.keys().next().map(|k| *k as f64 / 1e9).unwrap_or(0.0)
        };
        self.last_update_ts_s = snap.timestamp_s;
    }

    pub fn apply_change(&mut self, chg: &crate::backtest::pmxt::PriceChange) {
        let side = if chg.change_side.is_empty() { &chg.side } else { &chg.change_side };
        let book = if side.eq_ignore_ascii_case("buy") || side.eq_ignore_ascii_case("b") {
            &mut self.bids
        } else {
            &mut self.asks
        };
        if chg.change_size <= 0.0 {
            book.remove(&key(chg.change_price));
        } else {
            book.insert(key(chg.change_price), chg.change_size);
        }

        if let Some(top) = self.bids.keys().next_back() {
            self.best_bid = *top as f64 / 1e9;
        }
        if let Some(top) = self.asks.keys().next() {
            self.best_ask = *top as f64 / 1e9;
        }
        if chg.best_bid > 0.0 {
            self.best_bid = chg.best_bid;
        }
        if chg.best_ask > 0.0 {
            self.best_ask = chg.best_ask;
        }
        self.last_update_ts_s = chg.timestamp_s;
    }

    pub fn mid(&self) -> f64 {
        if self.best_bid > 0.0 && self.best_ask > 0.0 {
            (self.best_bid + self.best_ask) / 2.0
        } else {
            0.0
        }
    }

    pub fn ask_levels(&self) -> Vec<(f64, f64)> {
        self.asks.iter().map(|(k, s)| (*k as f64 / 1e9, *s)).collect()
    }

    pub fn bid_levels(&self) -> Vec<(f64, f64)> {
        self.bids.iter().rev().map(|(k, s)| (*k as f64 / 1e9, *s)).collect()
    }
}

#[derive(Debug, Clone)]
pub struct BacktestOrder {
    pub timestamp_s: f64,
    pub condition_id: String, // for resolver linkage
    pub token_id: String,
    pub side: String, // "buy" or "sell"
    pub size: f64,
    pub order_type: String, // "market" or "limit"
    pub limit_price: Option<f64>,
    pub fee_rate: f64,
    pub maker_fee_rate: f64,
}

#[derive(Debug, Clone)]
pub struct BacktestFill {
    pub order: BacktestOrder,
    pub fill_timestamp_s: f64,
    pub fill_price: f64,
    pub filled_size: f64,
    pub cost: f64,
    pub fee: f64,
    pub slippage: f64,
    pub book_age_ms: f64,
    pub success: bool,
    pub reason: String,
}

/// Strategy callback: called on every event after the per-tick book has
/// been updated (and any due fills flushed). Should return any orders the
/// strategy wants to place at this instant.
pub trait Strategy {
    fn on_event(
        &mut self,
        timestamp_s: f64,
        token_id: &str,
        book: &TokenBook,
        history: &BTreeMap<String, Vec<(f64, f64)>>,
    ) -> Vec<BacktestOrder>;
}

pub struct L2BacktestEngine {
    fill_model: OneTickTaker,
    latency: StaticLatencyConfig,
    history_window_seconds: f64,

    books: BTreeMap<String, TokenBook>,
    history: BTreeMap<String, Vec<(f64, f64)>>,
    pending_orders: Vec<BacktestOrder>,
    pub fills: Vec<BacktestFill>,
    pub event_count: u64,
}

impl L2BacktestEngine {
    pub fn new(fill_model: OneTickTaker, latency: StaticLatencyConfig) -> Self {
        Self {
            fill_model,
            latency,
            history_window_seconds: 300.0,
            books: BTreeMap::new(),
            history: BTreeMap::new(),
            pending_orders: Vec::new(),
            fills: Vec::new(),
            event_count: 0,
        }
    }

    /// Replay `events` (assumed sorted by timestamp). Drives the strategy.
    pub fn replay<S: Strategy>(
        &mut self,
        events: impl IntoIterator<Item = L2Event>,
        strategy: &mut S,
        default_fee_rate: f64,
    ) {
        for event in events {
            self.event_count += 1;
            let token_id = match &event.body {
                L2EventBody::BookSnapshot(s) => s.token_id.clone(),
                L2EventBody::PriceChange(c) => c.token_id.clone(),
            };
            if token_id.is_empty() {
                continue;
            }

            // Flush due fills BEFORE applying the new event — same lookahead
            // guard as the Python engine.
            self.flush_pending_orders(event.timestamp_s);

            {
                let book = self
                    .books
                    .entry(token_id.clone())
                    .or_insert_with(|| TokenBook::new(&token_id));
                match &event.body {
                    L2EventBody::BookSnapshot(s) => book.apply_snapshot(s),
                    L2EventBody::PriceChange(c) => book.apply_change(c),
                }
            }
            let book_clone = self.books.get(&token_id).cloned().unwrap();
            let mid = book_clone.mid();
            if mid > 0.0 {
                self.record_history(&token_id, event.timestamp_s, mid);
            }
            let new_orders = strategy.on_event(event.timestamp_s, &token_id, &book_clone, &self.history);
            for mut order in new_orders {
                if order.fee_rate == 0.0 {
                    order.fee_rate = default_fee_rate;
                }
                self.pending_orders.push(order);
            }
        }
    }

    fn record_history(&mut self, token_id: &str, ts: f64, mid: f64) {
        let entry = self.history.entry(token_id.to_string()).or_default();
        entry.push((ts, mid));
        let cutoff = ts - self.history_window_seconds;
        while !entry.is_empty() && entry[0].0 < cutoff {
            entry.remove(0);
        }
    }

    fn flush_pending_orders(&mut self, current_ts: f64) {
        let latency_s = self.latency.insert_ms as f64 / 1000.0;
        let mut still_pending: Vec<BacktestOrder> = Vec::new();
        let drained: Vec<BacktestOrder> = self.pending_orders.drain(..).collect();
        for order in drained {
            if current_ts - order.timestamp_s < latency_s {
                still_pending.push(order);
                continue;
            }
            let fill_ts = order.timestamp_s + latency_s;

            // Snapshot the book state we need so we don't hold the borrow.
            let book_snapshot = self.books.get(&order.token_id).cloned();
            let Some(book) = book_snapshot else {
                self.fills.push(BacktestFill {
                    order,
                    fill_timestamp_s: fill_ts,
                    fill_price: 0.0,
                    filled_size: 0.0,
                    cost: 0.0,
                    fee: 0.0,
                    slippage: 0.0,
                    book_age_ms: 0.0,
                    success: false,
                    reason: "no book at fill time".to_string(),
                });
                continue;
            };
            if book.best_bid <= 0.0 || book.best_ask <= 0.0 {
                self.fills.push(BacktestFill {
                    order,
                    fill_timestamp_s: fill_ts,
                    fill_price: 0.0,
                    filled_size: 0.0,
                    cost: 0.0,
                    fee: 0.0,
                    slippage: 0.0,
                    book_age_ms: 0.0,
                    success: false,
                    reason: "no book at fill time".to_string(),
                });
                continue;
            }

            let side = match Side::from_str(&order.side) {
                Some(s) => s,
                None => {
                    self.fills.push(BacktestFill {
                        order,
                        fill_timestamp_s: fill_ts,
                        fill_price: 0.0,
                        filled_size: 0.0,
                        cost: 0.0,
                        fee: 0.0,
                        slippage: 0.0,
                        book_age_ms: 0.0,
                        success: false,
                        reason: "invalid side".to_string(),
                    });
                    continue;
                }
            };
            let order_type = match order.order_type.as_str() {
                "limit" => OrderType::Limit,
                _ => OrderType::Market,
            };
            let result: FillResult = self.fill_model.fill(
                side,
                order.size,
                book.best_bid,
                book.best_ask,
                order_type,
                order.limit_price,
            );
            let book_age_ms = ((fill_ts - book.last_update_ts_s) * 1000.0).max(0.0);
            if !result.success {
                self.fills.push(BacktestFill {
                    order,
                    fill_timestamp_s: fill_ts,
                    fill_price: 0.0,
                    filled_size: 0.0,
                    cost: 0.0,
                    fee: 0.0,
                    slippage: 0.0,
                    book_age_ms,
                    success: false,
                    reason: result.reason.as_str().to_string(),
                });
                continue;
            }
            let effective_rate = if matches!(result.reason, FillReason::MakerFill) {
                order.maker_fee_rate
            } else {
                order.fee_rate
            };
            let fee = polymarket_fee(result.filled_size, result.fill_price, effective_rate);
            self.fills.push(BacktestFill {
                order,
                fill_timestamp_s: fill_ts,
                fill_price: result.fill_price,
                filled_size: result.filled_size,
                cost: result.fill_cost,
                fee,
                slippage: result.slippage_per_share,
                book_age_ms,
                success: true,
                reason: String::new(),
            });
        }
        self.pending_orders = still_pending;
    }

    pub fn summary(&self) -> Summary {
        let successful: Vec<&BacktestFill> = self.fills.iter().filter(|f| f.success).collect();
        let total_cost: f64 = successful.iter().map(|f| f.cost.abs()).sum();
        let total_fees: f64 = successful.iter().map(|f| f.fee).sum();
        let avg_slip = if successful.is_empty() {
            0.0
        } else {
            successful.iter().map(|f| f.slippage).sum::<f64>() / successful.len() as f64
        };
        let avg_book_age = if successful.is_empty() {
            0.0
        } else {
            successful.iter().map(|f| f.book_age_ms).sum::<f64>() / successful.len() as f64
        };
        Summary {
            events_processed: self.event_count,
            fills_total: self.fills.len() as u64,
            fills_success: successful.len() as u64,
            fills_failed: (self.fills.len() - successful.len()) as u64,
            total_cost,
            total_fees,
            avg_slippage: avg_slip,
            avg_book_age_ms: avg_book_age,
            tokens_tracked: self.books.len() as u64,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Summary {
    pub events_processed: u64,
    pub fills_total: u64,
    pub fills_success: u64,
    pub fills_failed: u64,
    pub total_cost: f64,
    pub total_fees: f64,
    pub avg_slippage: f64,
    pub avg_book_age_ms: f64,
    pub tokens_tracked: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::pmxt::{BookSnapshot, L2Level};

    struct NoopStrategy;
    impl Strategy for NoopStrategy {
        fn on_event(
            &mut self,
            _ts: f64,
            _tok: &str,
            _book: &TokenBook,
            _history: &BTreeMap<String, Vec<(f64, f64)>>,
        ) -> Vec<BacktestOrder> {
            Vec::new()
        }
    }

    fn snap_event(token: &str, ts: f64, bid: f64, ask: f64) -> L2Event {
        L2Event {
            timestamp_s: ts,
            market_id: "m".to_string(),
            body: L2EventBody::BookSnapshot(BookSnapshot {
                market_id: "m".to_string(),
                token_id: token.to_string(),
                best_bid: bid,
                best_ask: ask,
                timestamp_s: ts,
                bids: vec![L2Level { price: bid, size: 100.0 }],
                asks: vec![L2Level { price: ask, size: 100.0 }],
            }),
        }
    }

    #[test]
    fn empty_replay_produces_no_fills() {
        let mut e = L2BacktestEngine::new(OneTickTaker::default(), StaticLatencyConfig::default());
        let mut s = NoopStrategy;
        e.replay(std::iter::empty::<L2Event>(), &mut s, 0.072);
        assert_eq!(e.fills.len(), 0);
    }

    #[test]
    fn book_snapshot_updates_top_of_book() {
        let mut e = L2BacktestEngine::new(OneTickTaker::default(), StaticLatencyConfig::default());
        let mut s = NoopStrategy;
        e.replay(vec![snap_event("t", 1.0, 0.50, 0.52)], &mut s, 0.072);
        let book = e.books.get("t").unwrap();
        assert!((book.best_bid - 0.50).abs() < 1e-9);
        assert!((book.best_ask - 0.52).abs() < 1e-9);
    }

    struct OneShotBuy {
        fired: bool,
    }
    impl Strategy for OneShotBuy {
        fn on_event(
            &mut self,
            ts: f64,
            tok: &str,
            book: &TokenBook,
            _h: &BTreeMap<String, Vec<(f64, f64)>>,
        ) -> Vec<BacktestOrder> {
            if self.fired || book.best_ask <= 0.0 {
                return Vec::new();
            }
            self.fired = true;
            vec![BacktestOrder {
                timestamp_s: ts,
                condition_id: "c".into(),
                token_id: tok.into(),
                side: "buy".into(),
                size: 10.0,
                order_type: "market".into(),
                limit_price: None,
                fee_rate: 0.072,
                maker_fee_rate: 0.0,
            }]
        }
    }

    #[test]
    fn order_fires_after_latency_window() {
        let mut e = L2BacktestEngine::new(OneTickTaker::default(), StaticLatencyConfig { insert_ms: 50 });
        let mut s = OneShotBuy { fired: false };
        let events = vec![
            snap_event("t", 1.0, 0.50, 0.52),  // strategy fires here (ts=1.0)
            snap_event("t", 1.04, 0.50, 0.52), // 40ms later — still within latency, no fill
            snap_event("t", 1.10, 0.50, 0.52), // 100ms after order — past 50ms, flush should fire
        ];
        e.replay(events, &mut s, 0.072);
        assert_eq!(e.fills.len(), 1);
        let f = &e.fills[0];
        assert!(f.success);
        assert!((f.fill_price - 0.53).abs() < 1e-9); // best_ask 0.52 + 1 tick = 0.53
        assert!((f.fill_timestamp_s - 1.05).abs() < 1e-9);
    }
}
