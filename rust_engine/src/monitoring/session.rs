//! Live session monitoring — JSONL writer + counters.
//!
//! Schema mirrors `src/polymomentum/monitoring/session_monitor.py` so that
//! `validate_paper_replay.py` (and its Rust port) can consume the events
//! identically.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use serde::Serialize;
use serde_json::{json, Value};

#[derive(Default)]
struct Counters {
    order_count: u64,
    fill_count: u64,
    reject_count: u64,
    partial_fill_count: u64,
    cancel_count: u64,
    total_slippage: f64,
    total_fees: f64,
    total_cost: f64,
    signal_count: u64,
    signal_skip_count: u64,
    skip_reasons: std::collections::HashMap<String, u64>,
    price_gaps: Vec<f64>,
    fill_times: Vec<f64>,
    api_latencies: Vec<f64>,
    errors: Vec<Value>,
    source_dropouts: std::collections::HashMap<String, u64>,
}

pub struct SessionMonitor {
    session_id: String,
    summary_path: PathBuf,
    file: Mutex<std::fs::File>,
    start_time: f64,
    counters: Mutex<Counters>,
}

impl SessionMonitor {
    pub fn open(log_dir: impl Into<PathBuf>) -> Result<Self> {
        let log_dir = log_dir.into();
        std::fs::create_dir_all(&log_dir).ok();
        let session_id = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let events_path = log_dir.join(format!("session_{session_id}.jsonl"));
        let summary_path = log_dir.join(format!("summary_{session_id}.json"));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&events_path)
            .context("open session log")?;
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        tracing::info!(?events_path, "session monitor started");
        Ok(Self {
            session_id,
            summary_path,
            file: Mutex::new(file),
            start_time,
            counters: Mutex::new(Counters::default()),
        })
    }

    fn write_event(&self, category: &str, event_type: &str, mut data: Value) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let ts_iso = Utc
            .timestamp_opt(ts as i64, ((ts.fract()) * 1e9) as u32)
            .single()
            .map(|d| d.to_rfc3339())
            .unwrap_or_default();
        if let Value::Object(map) = &mut data {
            map.insert("ts".into(), json!(ts));
            map.insert("ts_iso".into(), json!(ts_iso));
            map.insert("cat".into(), json!(category));
            map.insert("type".into(), json!(event_type));
        }
        if let Ok(s) = serde_json::to_string(&data) {
            if let Ok(mut f) = self.file.lock() {
                let _ = writeln!(f, "{}", s);
                let _ = f.flush();
            }
        }
    }

    // ── Order lifecycle ────────────────────────────────────────────

    pub fn record_order_placed(&self, evt: &OrderPlaced) {
        self.counters.lock().unwrap().order_count += 1;
        self.write_event("order", "placed", serde_json::to_value(evt).unwrap_or(Value::Null));
    }

    pub fn record_order_filled(&self, evt: &OrderFilled) {
        let mut c = self.counters.lock().unwrap();
        c.fill_count += 1;
        c.total_slippage += (evt.fill_price - evt.limit_price).abs();
        c.total_fees += evt.fee;
        c.total_cost += evt.fill_price * evt.filled;
        c.fill_times.push(evt.fill_time_s);
        if evt.requested > 0.0 && (evt.filled / evt.requested) < 0.95 {
            c.partial_fill_count += 1;
        }
        drop(c);
        self.write_event("order", "filled", serde_json::to_value(evt).unwrap_or(Value::Null));
    }

    pub fn record_order_rejected(&self, token_id: &str, reason: &str, price: f64, size: f64) {
        self.counters.lock().unwrap().reject_count += 1;
        self.write_event(
            "order",
            "rejected",
            json!({
                "token_id": short(token_id, 20),
                "reason": short(reason, 100),
                "price": round_n(price, 4),
                "size": round_n(size, 2),
            }),
        );
    }

    pub fn record_signal_evaluation(&self, evt: &SignalEvaluation) {
        self.counters.lock().unwrap().signal_count += 1;
        self.write_event(
            "signal",
            "evaluation",
            serde_json::to_value(evt).unwrap_or(Value::Null),
        );
    }

    pub fn record_signal_skip(&self, contract_id: &str, reason: &str) {
        let mut c = self.counters.lock().unwrap();
        c.signal_skip_count += 1;
        *c.skip_reasons.entry(reason.to_string()).or_insert(0) += 1;
        drop(c);
        self.write_event(
            "signal",
            "skip",
            json!({
                "cid": short(contract_id, 16),
                "reason": reason,
            }),
        );
    }

    pub fn top_skip_reasons(&self, n: usize) -> Vec<(String, u64)> {
        let c = self.counters.lock().unwrap();
        let mut v: Vec<(String, u64)> = c.skip_reasons.iter().map(|(k, v)| (k.clone(), *v)).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1));
        v.truncate(n);
        v
    }

    pub fn record_resolution(
        &self,
        contract_id: &str,
        predicted: &str,
        actual: &str,
        won: bool,
        pnl: f64,
        entry_price: f64,
        open_btc: f64,
        close_btc: f64,
    ) {
        self.write_event(
            "resolution",
            "resolved",
            json!({
                "cid": short(contract_id, 16),
                "predicted": predicted,
                "actual": actual,
                "won": won,
                "pnl": round_n(pnl, 4),
                "entry_price": round_n(entry_price, 4),
                "open_btc": round_n(open_btc, 2),
                "close_btc": round_n(close_btc, 2),
                "btc_move": round_n(close_btc - open_btc, 2),
            }),
        );
    }

    pub fn record_oracle_resolution(
        &self,
        contract_id: &str,
        our_actual: &str,
        our_open_btc: f64,
        our_close_btc: f64,
        polymarket_actual: &str,
        polymarket_outcome_prices: &[f64],
        polymarket_closed: bool,
        agreed: bool,
        delay_s: f64,
    ) {
        self.write_event(
            "oracle",
            "resolution",
            json!({
                "cid": short(contract_id, 16),
                "our_actual": our_actual,
                "our_open_btc": round_n(our_open_btc, 2),
                "our_close_btc": round_n(our_close_btc, 2),
                "polymarket_actual": polymarket_actual,
                "polymarket_outcome_prices": polymarket_outcome_prices,
                "polymarket_closed": polymarket_closed,
                "agreed": agreed,
                "verification_delay_s": round_n(delay_s, 1),
            }),
        );
    }

    pub fn record_risk_state(
        &self,
        bankroll: f64,
        exposure: f64,
        available: f64,
        positions: u64,
        realized_pnl: f64,
        wins: u64,
        losses: u64,
    ) {
        let total = (wins + losses).max(1);
        self.write_event(
            "risk",
            "state",
            json!({
                "bankroll": round_n(bankroll, 2),
                "exposure": round_n(exposure, 2),
                "available": round_n(available, 2),
                "positions": positions,
                "realized_pnl": round_n(realized_pnl, 2),
                "wins": wins,
                "losses": losses,
                "win_rate": round_n(wins as f64 / total as f64, 3),
            }),
        );
    }

    pub fn record_price_snapshot(
        &self,
        btc_price: f64,
        n_sources: usize,
        spread: f64,
        staleness_ms: f64,
        sources: &std::collections::HashMap<String, f64>,
    ) {
        let mut c = self.counters.lock().unwrap();
        if sources.len() >= 2 {
            let mut prices: Vec<f64> = sources.values().copied().collect();
            prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let gap = prices.last().copied().unwrap_or(0.0) - prices.first().copied().unwrap_or(0.0);
            c.price_gaps.push(gap);
        }
        drop(c);

        let rounded: serde_json::Map<String, Value> = sources
            .iter()
            .map(|(k, v)| (k.clone(), json!(round_n(*v, 2))))
            .collect();
        self.write_event(
            "price",
            "snapshot",
            json!({
                "btc": round_n(btc_price, 2),
                "sources": n_sources,
                "spread": round_n(spread, 2),
                "staleness_ms": round_n(staleness_ms, 1),
                "source_prices": rounded,
            }),
        );
    }

    pub fn record_error(&self, component: &str, error: &str, recoverable: bool) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let mut c = self.counters.lock().unwrap();
        c.errors.push(json!({
            "component": component,
            "error": short(error, 200),
            "ts": now,
        }));
        drop(c);
        self.write_event(
            "system",
            "error",
            json!({
                "component": component,
                "error": short(error, 200),
                "recoverable": recoverable,
            }),
        );
    }

    pub fn save_summary(&self) -> Result<()> {
        let summary = self.get_summary();
        std::fs::write(&self.summary_path, serde_json::to_string_pretty(&summary)?)?;
        tracing::info!(session_id = %self.session_id, "session summary saved");
        Ok(())
    }

    pub fn get_summary(&self) -> Value {
        let c = self.counters.lock().unwrap();
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
            - self.start_time;
        let avg_fill_time = if c.fill_times.is_empty() {
            0.0
        } else {
            c.fill_times.iter().sum::<f64>() / c.fill_times.len() as f64
        };
        let avg_slippage = if c.fill_count == 0 {
            0.0
        } else {
            c.total_slippage / c.fill_count as f64
        };
        let avg_latency = if c.api_latencies.is_empty() {
            0.0
        } else {
            c.api_latencies.iter().sum::<f64>() / c.api_latencies.len() as f64
        };
        let avg_gap = if c.price_gaps.is_empty() {
            0.0
        } else {
            c.price_gaps.iter().sum::<f64>() / c.price_gaps.len() as f64
        };
        let max_gap = c.price_gaps.iter().cloned().fold(0.0_f64, f64::max);

        let mut sorted_skips: Vec<(String, u64)> =
            c.skip_reasons.iter().map(|(k, v)| (k.clone(), *v)).collect();
        sorted_skips.sort_by(|a, b| b.1.cmp(&a.1));

        json!({
            "session_id": self.session_id,
            "duration_minutes": round_n(elapsed / 60.0, 1),
            "orders": {
                "placed": c.order_count,
                "filled": c.fill_count,
                "rejected": c.reject_count,
                "partial_fills": c.partial_fill_count,
                "cancelled": c.cancel_count,
                "fill_rate": round_n(c.fill_count as f64 / c.order_count.max(1) as f64, 3),
                "avg_fill_time_s": round_n(avg_fill_time, 2),
                "avg_slippage": round_n(avg_slippage, 4),
                "total_fees": round_n(c.total_fees, 4),
                "total_cost": round_n(c.total_cost, 2),
            },
            "signals": {
                "total": c.signal_count,
                "skipped": c.signal_skip_count,
                "skip_reasons": sorted_skips,
            },
            "price_feed": {
                "avg_cross_exchange_gap": round_n(avg_gap, 2),
                "max_cross_exchange_gap": round_n(max_gap, 2),
                "source_dropouts": c.source_dropouts,
            },
            "system": {
                "avg_api_latency_ms": round_n(avg_latency, 1),
                "total_errors": c.errors.len(),
            },
        })
    }

}

#[derive(Debug, Clone, Serialize)]
pub struct OrderPlaced {
    pub token_id: String,
    pub side: String,
    pub price: f64,
    pub live_price: f64,
    pub size: f64,
    pub order_value: f64,
    pub order_id: String,
    pub book_best_ask: f64,
    pub book_ask_depth: f64,
    pub book_bid_depth: f64,
    pub balance_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderFilled {
    pub order_id: String,
    pub filled: f64,
    pub requested: f64,
    pub fill_pct: f64,
    pub fill_price: f64,
    pub limit_price: f64,
    pub slippage: f64,
    pub slippage_bps: f64,
    pub fill_time_s: f64,
    pub fee: f64,
    pub n_trades: u64,
}

/// Replay-grade signal evaluation event. Schema MUST match Python's
/// `record_signal_evaluation`.
#[derive(Debug, Clone, Serialize)]
pub struct SignalEvaluation {
    pub ts_ms: i64,
    pub cid: String,
    pub asset: String,
    pub open: f64,
    pub px: f64,
    pub chg: f64,
    pub chg_pct: f64,
    pub cons: f64,
    pub z: f64,
    pub conf: f64,
    pub elapsed_min: f64,
    pub remaining_min: f64,
    pub dir: String,
    pub vol_fast: f64,
    pub vol_slow: f64,
    pub implied_vol: f64,
    pub cross_boost: f64,
    pub up_price: f64,
    pub down_price: f64,
    pub zone: String,
    pub fair: f64,
    pub edge: f64,
    pub traded: bool,
    pub skip_reason: Option<String>,
    pub skip_detail: Option<String>,
}

fn short(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        s[..n].to_string()
    }
}

fn round_n(x: f64, decimals: u32) -> f64 {
    let p = 10f64.powi(decimals as i32);
    (x * p).round() / p
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn writes_jsonl_event() {
        let tmp = TempDir::new().unwrap();
        let m = SessionMonitor::open(tmp.path()).unwrap();
        m.record_signal_skip("cidabcdefg", "low_confidence");
        drop(m);
        // Pick the single session file the open call produced.
        let entry = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("session_")
            })
            .expect("session file");
        let body = std::fs::read_to_string(entry.path()).unwrap();
        assert!(body.contains("\"reason\":\"low_confidence\""));
        assert!(body.contains("\"cat\":\"signal\""));
    }

    #[test]
    fn top_skip_reasons_sorts() {
        let tmp = TempDir::new().unwrap();
        let m = SessionMonitor::open(tmp.path()).unwrap();
        m.record_signal_skip("c1", "low_edge");
        m.record_signal_skip("c2", "low_edge");
        m.record_signal_skip("c3", "negative_ev");
        let top = m.top_skip_reasons(2);
        assert_eq!(top[0].0, "low_edge");
        assert_eq!(top[0].1, 2);
    }
}
