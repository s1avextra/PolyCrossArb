//! Debug/diagnostic module for the latency pipeline.
//!
//! When enabled, logs every filter rejection reason, near-misses,
//! BTC movement distribution, MM staleness, and per-contract evaluation
//! details to a CSV file for post-hoc analysis.

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::time::Instant;

/// Reason a contract was skipped
#[derive(Debug, Clone, serde::Serialize)]
pub struct SkipReason {
    pub contract_id: String,
    pub reason: String,
    pub value: f64,       // the value that failed the check
    pub threshold: f64,   // what it needed to be
}

/// Per-tick diagnostic snapshot
#[derive(Debug, Clone, serde::Serialize)]
pub struct TickDiag {
    pub timestamp_s: f64,
    pub btc_price: f64,
    pub btc_move_5s: f64,
    pub btc_move_15s: f64,
    pub btc_move_30s: f64,
    pub n_sources: usize,
    pub spread: f64,
    pub vol: f64,
    pub n_contracts: usize,
    pub n_in_time_range: usize,     // contracts within min/max minutes_remaining
    pub n_stale_enough: usize,      // MM stale > threshold
    pub n_btc_moved_enough: usize,  // BTC move > threshold
    pub n_edge_enough: usize,       // edge > threshold
    pub n_signals: usize,           // actual signals emitted
    pub best_edge: f64,
    pub best_mm_staleness_s: f64,
    pub best_btc_move: f64,
}

/// Rolling BTC price tracker for move calculations
pub struct BtcTracker {
    /// Ring buffer of (timestamp_s, price) tuples, 1-second resolution
    history: Vec<(f64, f64)>,
    max_size: usize,
}

impl BtcTracker {
    pub fn new() -> Self {
        Self {
            history: Vec::with_capacity(120),
            max_size: 120, // 2 minutes of 1-second prices
        }
    }

    pub fn record(&mut self, timestamp_s: f64, price: f64) {
        // Only record if >0.5s since last record (deduplicate fast ticks)
        if let Some(last) = self.history.last() {
            if timestamp_s - last.0 < 0.5 {
                return;
            }
        }
        if self.history.len() >= self.max_size {
            self.history.remove(0);
        }
        self.history.push((timestamp_s, price));
    }

    /// Get BTC move over last N seconds
    pub fn move_over(&self, seconds: f64) -> f64 {
        if self.history.is_empty() { return 0.0; }
        let now_ts = self.history.last().unwrap().0;
        let cutoff = now_ts - seconds;
        let current = self.history.last().unwrap().1;

        // Find price closest to cutoff
        for &(ts, price) in self.history.iter() {
            if ts >= cutoff {
                return current - price;
            }
        }
        // If all history is within the window, use oldest
        current - self.history[0].1
    }
}

/// Aggregated statistics over a reporting period
pub struct DebugStats {
    pub tick_count: u64,
    pub total_contracts_evaluated: u64,
    pub skip_reasons: HashMap<String, u64>,
    pub btc_moves_5s: Vec<f64>,
    pub btc_moves_15s: Vec<f64>,
    pub mm_stalenesses: Vec<f64>,
    pub edges_seen: Vec<f64>,
    pub near_misses: Vec<SkipReason>,
    pub signals_emitted: u64,
}

impl DebugStats {
    pub fn new() -> Self {
        Self {
            tick_count: 0,
            total_contracts_evaluated: 0,
            skip_reasons: HashMap::new(),
            btc_moves_5s: Vec::new(),
            btc_moves_15s: Vec::new(),
            mm_stalenesses: Vec::new(),
            edges_seen: Vec::new(),
            near_misses: Vec::new(),
            signals_emitted: 0,
        }
    }

    pub fn record_skip(&mut self, reason: &str) {
        *self.skip_reasons.entry(reason.to_string()).or_insert(0) += 1;
    }

    pub fn record_near_miss(&mut self, contract_id: &str, reason: &str, value: f64, threshold: f64) {
        if self.near_misses.len() < 100 { // cap to prevent unbounded growth
            self.near_misses.push(SkipReason {
                contract_id: contract_id[..16.min(contract_id.len())].to_string(),
                reason: reason.to_string(),
                value,
                threshold,
            });
        }
    }

    /// Format a comprehensive report
    pub fn report(&self, elapsed_s: f64) -> String {
        let mut lines = Vec::new();
        lines.push("======================================================================".to_string());
        lines.push(format!("  DEBUG REPORT ({:.0}s elapsed)", elapsed_s));
        lines.push("======================================================================".to_string());

        lines.push(format!("  Ticks: {}  Contracts evaluated: {}  Signals: {}",
            self.tick_count, self.total_contracts_evaluated, self.signals_emitted));

        // Skip reasons sorted by count
        lines.push(format!("\n  --- SKIP REASONS ---"));
        let mut reasons: Vec<_> = self.skip_reasons.iter().collect();
        reasons.sort_by(|a, b| b.1.cmp(a.1));
        for (reason, count) in &reasons {
            let pct = **count as f64 / self.total_contracts_evaluated.max(1) as f64 * 100.0;
            lines.push(format!("  {:35} {:>8} ({:.1}%)", reason, count, pct));
        }

        // BTC movement distribution
        if !self.btc_moves_5s.is_empty() {
            let mut m5 = self.btc_moves_5s.clone();
            m5.sort_by(|a, b| a.partial_cmp(b).unwrap());
            lines.push(format!("\n  --- BTC MOVES (absolute) ---"));
            lines.push(format!("  5s:  mean=${:.1} median=${:.1} p90=${:.1} p99=${:.1} max=${:.1}",
                mean(&m5), percentile(&m5, 0.5), percentile(&m5, 0.9), percentile(&m5, 0.99), m5.last().unwrap_or(&0.0)));
        }
        if !self.btc_moves_15s.is_empty() {
            let mut m15 = self.btc_moves_15s.clone();
            m15.sort_by(|a, b| a.partial_cmp(b).unwrap());
            lines.push(format!("  15s: mean=${:.1} median=${:.1} p90=${:.1} p99=${:.1} max=${:.1}",
                mean(&m15), percentile(&m15, 0.5), percentile(&m15, 0.9), percentile(&m15, 0.99), m15.last().unwrap_or(&0.0)));
        }

        // MM staleness distribution
        if !self.mm_stalenesses.is_empty() {
            let mut ms = self.mm_stalenesses.clone();
            ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
            lines.push(format!("\n  --- MM STALENESS ---"));
            lines.push(format!("  mean={:.1}s median={:.1}s p90={:.1}s max={:.1}s",
                mean(&ms), percentile(&ms, 0.5), percentile(&ms, 0.9), ms.last().unwrap_or(&0.0)));
        }

        // Edge distribution
        if !self.edges_seen.is_empty() {
            let mut es = self.edges_seen.clone();
            es.sort_by(|a, b| a.partial_cmp(b).unwrap());
            lines.push(format!("\n  --- EDGES SEEN (when BTC moved enough) ---"));
            lines.push(format!("  mean={:.1}% median={:.1}% p90={:.1}% max={:.1}%",
                mean(&es)*100.0, percentile(&es, 0.5)*100.0, percentile(&es, 0.9)*100.0,
                es.last().unwrap_or(&0.0)*100.0));
        }

        // Near misses
        if !self.near_misses.is_empty() {
            lines.push(format!("\n  --- NEAR MISSES (last {}) ---", self.near_misses.len()));
            for nm in self.near_misses.iter().rev().take(10) {
                lines.push(format!("  {} {} val={:.3} needed={:.3}",
                    nm.contract_id, nm.reason, nm.value, nm.threshold));
            }
        }

        lines.join("\n")
    }

    pub fn reset(&mut self) {
        self.tick_count = 0;
        self.total_contracts_evaluated = 0;
        self.skip_reasons.clear();
        self.btc_moves_5s.clear();
        self.btc_moves_15s.clear();
        self.mm_stalenesses.clear();
        self.edges_seen.clear();
        self.near_misses.clear();
        self.signals_emitted = 0;
    }
}

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() { return 0.0; }
    v.iter().sum::<f64>() / v.len() as f64
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() { return 0.0; }
    let idx = ((sorted.len() as f64 * pct) as usize).min(sorted.len() - 1);
    sorted[idx]
}
