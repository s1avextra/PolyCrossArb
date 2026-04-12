//! Latency monitoring — instruments every stage of the pipeline.
//!
//! Tracks:
//!   - Per-exchange WebSocket message latency (receive → parse → state update)
//!   - Cross-exchange aggregation time
//!   - Polymarket price staleness (our price vs MM last update)
//!   - Signal generation latency (state → decision)
//!   - End-to-end tick-to-signal latency

use std::collections::HashMap;
use std::time::Instant;

/// Rolling statistics for a latency metric (nanoseconds)
#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub name: String,
    pub count: u64,
    pub sum_ns: u64,
    pub min_ns: u64,
    pub max_ns: u64,
    pub last_ns: u64,
    // Keep last 1000 samples for percentiles
    samples: Vec<u64>,
}

impl LatencyStats {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            count: 0,
            sum_ns: 0,
            min_ns: u64::MAX,
            max_ns: 0,
            last_ns: 0,
            samples: Vec::with_capacity(1000),
        }
    }

    pub fn record(&mut self, duration_ns: u64) {
        self.count += 1;
        self.sum_ns += duration_ns;
        self.last_ns = duration_ns;
        if duration_ns < self.min_ns { self.min_ns = duration_ns; }
        if duration_ns > self.max_ns { self.max_ns = duration_ns; }
        if self.samples.len() >= 1000 {
            self.samples.remove(0);
        }
        self.samples.push(duration_ns);
    }

    pub fn mean_us(&self) -> f64 {
        if self.count == 0 { return 0.0; }
        (self.sum_ns as f64 / self.count as f64) / 1000.0
    }

    pub fn p50_us(&self) -> f64 {
        self.percentile(0.50) / 1000.0
    }

    pub fn p99_us(&self) -> f64 {
        self.percentile(0.99) / 1000.0
    }

    fn percentile(&self, pct: f64) -> f64 {
        if self.samples.is_empty() { return 0.0; }
        let mut sorted = self.samples.clone();
        sorted.sort();
        let idx = ((sorted.len() as f64 * pct) as usize).min(sorted.len() - 1);
        sorted[idx] as f64
    }
}

/// Central latency monitor for all pipeline stages
#[derive(Debug, Clone)]
pub struct LatencyMonitor {
    pub metrics: HashMap<String, LatencyStats>,
    // Exchange-specific staleness tracking
    pub exchange_last_update: HashMap<String, Instant>,
    // Polymarket contract staleness
    pub mm_last_price_change: HashMap<String, (Instant, f64)>, // contract_id -> (when, price)
}

impl LatencyMonitor {
    pub fn new() -> Self {
        let mut metrics = HashMap::new();
        // Pre-register all metrics
        for name in &[
            "binance_ws_parse", "bybit_ws_parse", "okx_ws_parse", "mexc_ws_parse",
            "price_aggregation", "signal_generation", "tick_to_signal",
            "polymarket_ws_parse",
            // Per-stage breakdown for order placement
            "stage_price_read", "stage_edge_calc", "stage_signing",
            "stage_http_post", "stage_server_ack", "stage_total",
        ] {
            metrics.insert(name.to_string(), LatencyStats::new(name));
        }
        Self {
            metrics,
            exchange_last_update: HashMap::new(),
            mm_last_price_change: HashMap::new(),
        }
    }

    pub fn record(&mut self, metric: &str, duration_ns: u64) {
        self.metrics
            .entry(metric.to_string())
            .or_insert_with(|| LatencyStats::new(metric))
            .record(duration_ns);
    }

    pub fn record_exchange_update(&mut self, exchange: &str) {
        self.exchange_last_update.insert(exchange.to_string(), Instant::now());
    }

    pub fn exchange_staleness_ms(&self, exchange: &str) -> f64 {
        match self.exchange_last_update.get(exchange) {
            Some(t) => t.elapsed().as_secs_f64() * 1000.0,
            None => 99999.0,
        }
    }

    /// Record a Polymarket MM price update. Returns staleness in seconds if price changed.
    pub fn record_mm_price(&mut self, contract_id: &str, price: f64) -> Option<f64> {
        let now = Instant::now();
        match self.mm_last_price_change.get(contract_id) {
            Some((last_time, last_price)) => {
                if (price - last_price).abs() > 0.001 {
                    let staleness_s = last_time.elapsed().as_secs_f64();
                    self.mm_last_price_change.insert(contract_id.to_string(), (now, price));
                    Some(staleness_s)
                } else {
                    None // price unchanged
                }
            }
            None => {
                self.mm_last_price_change.insert(contract_id.to_string(), (now, price));
                None
            }
        }
    }

    /// Get staleness of a contract's MM price in seconds
    pub fn mm_staleness_s(&self, contract_id: &str) -> f64 {
        match self.mm_last_price_change.get(contract_id) {
            Some((t, _)) => t.elapsed().as_secs_f64(),
            None => 0.0,
        }
    }

    /// Format a summary report for stderr
    pub fn report(&self) -> String {
        let mut lines = Vec::new();
        lines.push("=== LATENCY REPORT ===".to_string());

        let mut keys: Vec<&String> = self.metrics.keys().collect();
        keys.sort();

        for key in keys {
            let s = &self.metrics[key];
            if s.count == 0 { continue; }
            lines.push(format!(
                "  {:25} n={:>8}  mean={:>8.1}us  p50={:>8.1}us  p99={:>8.1}us  max={:>8.1}us",
                s.name, s.count, s.mean_us(), s.p50_us(), s.p99_us(),
                s.max_ns as f64 / 1000.0,
            ));
        }

        // Exchange staleness
        lines.push("  --- Exchange staleness ---".to_string());
        for (ex, _) in &self.exchange_last_update {
            lines.push(format!("  {:25} {:.0}ms ago", ex, self.exchange_staleness_ms(ex)));
        }

        lines.join("\n")
    }
}
