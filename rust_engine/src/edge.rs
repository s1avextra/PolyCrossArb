//! Edge accumulation engine for latency-based candle trading.
//!
//! Core strategy: when we detect BTC has moved but the Polymarket MM
//! price is stale, we have an information edge. Instead of betting once,
//! we accumulate edge over the 5-minute window:
//!
//! 1. INITIAL ENTRY: BTC moves $20, MM stale → buy $1 at 0.52
//! 2. SCALE-IN: BTC moves further to $40, MM still stale → buy another $1
//! 3. SCALE-IN: BTC reverses to $15 → stop (edge shrunk)
//! 4. HOLD: wait for resolution
//!
//! This gives us a better average entry price and adapts to how
//! the edge evolves within the window.

use std::collections::HashMap;
use std::time::Instant;

use crate::fair_value;

/// A single entry in a position
#[derive(Debug, Clone, serde::Serialize)]
pub struct Entry {
    pub timestamp_s: f64,
    pub price: f64,       // what we paid (MM stale ask)
    pub fair_value: f64,  // our BS fair value at entry
    pub btc_price: f64,   // BTC at entry
    pub size_usd: f64,    // how much we bet
    pub edge: f64,        // fair_value - price
}

/// Accumulated position in one contract window
#[derive(Debug, Clone)]
pub struct AccumulatedPosition {
    pub contract_id: String,
    pub direction: String,     // "up" or "down"
    pub window_end_s: f64,     // unix timestamp when window closes
    pub window_minutes: f64,
    pub entries: Vec<Entry>,
    pub total_size_usd: f64,
    pub avg_entry_price: f64,
    pub max_size_usd: f64,     // cap per window
    pub created_at: Instant,
}

impl AccumulatedPosition {
    pub fn new(
        contract_id: &str,
        direction: &str,
        window_end_s: f64,
        window_minutes: f64,
        max_size_usd: f64,
    ) -> Self {
        Self {
            contract_id: contract_id.to_string(),
            direction: direction.to_string(),
            window_end_s,
            window_minutes,
            entries: Vec::new(),
            total_size_usd: 0.0,
            avg_entry_price: 0.0,
            max_size_usd,
            created_at: Instant::now(),
        }
    }

    /// Add an entry. Returns false if position is full.
    pub fn add_entry(&mut self, price: f64, fair_value: f64, btc_price: f64, size_usd: f64, timestamp_s: f64) -> bool {
        if self.total_size_usd + size_usd > self.max_size_usd {
            return false;
        }
        let edge = fair_value - price;
        self.entries.push(Entry {
            timestamp_s,
            price,
            fair_value,
            btc_price,
            size_usd,
            edge,
        });
        let prev_total = self.total_size_usd;
        self.total_size_usd += size_usd;
        // Weighted average entry price
        if self.total_size_usd > 0.0 {
            self.avg_entry_price = (self.avg_entry_price * prev_total + price * size_usd) / self.total_size_usd;
        }
        true
    }

    pub fn n_entries(&self) -> usize {
        self.entries.len()
    }

    pub fn avg_edge(&self) -> f64 {
        if self.entries.is_empty() { return 0.0; }
        self.entries.iter().map(|e| e.edge).sum::<f64>() / self.entries.len() as f64
    }

    pub fn time_in_position_s(&self) -> f64 {
        self.created_at.elapsed().as_secs_f64()
    }
}

/// Configuration for the edge accumulator
#[derive(Debug, Clone, serde::Deserialize)]
pub struct EdgeConfig {
    /// Minimum BTC move (in $) to trigger initial entry
    pub min_btc_move: f64,
    /// Minimum edge (fair_value - mm_price) to enter
    pub min_edge: f64,
    /// Minimum MM staleness (seconds) to consider price stale
    pub min_mm_staleness_s: f64,
    /// Minimum additional BTC move (in $) to trigger scale-in
    pub scale_in_btc_increment: f64,
    /// Maximum entries per window (scale-in limit)
    pub max_entries_per_window: usize,
    /// Size per entry in USD
    pub entry_size_usd: f64,
    /// Maximum total size per window
    pub max_size_per_window_usd: f64,
    /// Minimum minutes remaining to enter
    pub min_minutes_remaining: f64,
    /// Maximum minutes remaining (don't enter too early)
    pub max_minutes_remaining: f64,
    /// Minimum price sources required
    pub min_sources: usize,
    /// Edge shrink threshold — stop scaling in if edge drops below this
    pub edge_shrink_stop: f64,
    /// Bankroll limit — total capital available
    pub bankroll_usd: f64,
    /// Max fraction of bankroll exposed at once
    pub max_exposure_pct: f64,
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            min_btc_move: 25.0,         // raised from $15 — avoids transient spikes
            min_edge: 0.03,
            min_mm_staleness_s: 7.0,    // lowered from 10 — real MM staleness ~46s median
            scale_in_btc_increment: 15.0,
            max_entries_per_window: 5,
            entry_size_usd: 1.0,
            max_size_per_window_usd: 5.0,
            min_minutes_remaining: 0.5,
            max_minutes_remaining: 30.0, // raised from 4.5 — was killing 94% of opportunities
            min_sources: 2,
            edge_shrink_stop: 0.02,
            bankroll_usd: 6.0,
            max_exposure_pct: 0.80,
        }
    }
}

/// Trade signal emitted to Python orchestrator
#[derive(Debug, Clone, serde::Serialize)]
pub struct LatencySignal {
    pub action: String,           // "enter" or "scale_in"
    pub contract_id: String,
    pub token_id: String,
    pub direction: String,        // "up" or "down"
    pub mm_price: f64,            // stale MM ask
    pub fair_value: f64,          // our BS fair value
    pub edge: f64,                // fair_value - mm_price
    pub btc_price: f64,
    pub btc_move: f64,            // $ move from window open
    pub mm_staleness_s: f64,      // how long MM price has been stale
    pub entry_number: usize,      // 1, 2, 3... (scale-in count)
    pub total_position_usd: f64,  // total accumulated in this window
    pub size_usd: f64,            // this entry size
    pub minutes_remaining: f64,
    pub window_minutes: f64,
    // Latency instrumentation
    pub tick_to_signal_us: u64,   // microseconds from tick to signal
    pub sources: usize,
    pub spread: f64,
    pub timestamp_s: f64,
}

/// The edge accumulation engine
pub struct EdgeAccumulator {
    pub config: EdgeConfig,
    /// Active positions: contract_id -> AccumulatedPosition
    pub positions: HashMap<String, AccumulatedPosition>,
    /// BTC price at the start of each candle window: contract_id -> open_price
    pub window_opens: HashMap<String, f64>,
    /// Last BTC price at which we entered (for scale-in increment tracking)
    pub last_entry_btc: HashMap<String, f64>,
    /// Total signals emitted
    pub signal_count: u64,
    /// Current capital locked in open positions
    pub capital_locked: f64,
    /// Rolling BTC prices for momentum check (timestamp_s, price)
    btc_history: Vec<(f64, f64)>,
}

impl EdgeAccumulator {
    pub fn new(config: EdgeConfig) -> Self {
        Self {
            config,
            positions: HashMap::new(),
            window_opens: HashMap::new(),
            last_entry_btc: HashMap::new(),
            signal_count: 0,
            capital_locked: 0.0,
            btc_history: Vec::with_capacity(300),
        }
    }

    /// Record BTC price for momentum lookback (call every tick)
    pub fn record_btc(&mut self, timestamp_s: f64, price: f64) {
        // Keep ~2 minutes of 1-second prices
        if let Some(last) = self.btc_history.last() {
            if timestamp_s - last.0 < 0.5 { return; }
        }
        if self.btc_history.len() >= 300 {
            self.btc_history.remove(0);
        }
        self.btc_history.push((timestamp_s, price));
    }

    /// Get BTC price N seconds ago
    fn btc_ago(&self, seconds: f64) -> Option<f64> {
        if self.btc_history.is_empty() { return None; }
        let now = self.btc_history.last().unwrap().0;
        let target = now - seconds;
        for &(ts, price) in self.btc_history.iter().rev() {
            if ts <= target {
                return Some(price);
            }
        }
        Some(self.btc_history[0].1)
    }

    /// Check if momentum over last N seconds agrees with direction
    fn momentum_agrees(&self, direction: &str, lookback_s: f64) -> bool {
        let current = match self.btc_history.last() {
            Some((_, p)) => *p,
            None => return true, // no data, don't filter
        };
        let past = match self.btc_ago(lookback_s) {
            Some(p) => p,
            None => return true,
        };
        let trend = current - past;
        match direction {
            "up" => trend > 0.0,
            "down" => trend < 0.0,
            _ => true,
        }
    }

    pub fn available_capital(&self) -> f64 {
        (self.config.bankroll_usd * self.config.max_exposure_pct - self.capital_locked).max(0.0)
    }

    /// Evaluate whether to enter or scale into a candle contract.
    ///
    /// Called on every tick (~50ms) for every active contract.
    /// Returns a signal if a trade should be placed.
    /// When debug_stats is provided, records why each contract was skipped.
    pub fn evaluate(
        &mut self,
        contract_id: &str,
        token_id: &str,
        mm_up_price: f64,
        mm_down_price: f64,
        btc_price: f64,
        window_end_s: f64,
        window_minutes: f64,
        minutes_remaining: f64,
        mm_staleness_s: f64,
        vol: f64,
        n_sources: usize,
        spread: f64,
        tick_start_ns: u64,
        mut debug_stats: Option<&mut crate::debug::DebugStats>,
    ) -> Option<LatencySignal> {

        // Basic filters
        if n_sources < self.config.min_sources {
            if let Some(ds) = debug_stats { ds.record_skip("low_sources"); }
            return None;
        }
        if minutes_remaining < self.config.min_minutes_remaining {
            if let Some(ds) = debug_stats { ds.record_skip("too_close_to_resolution"); }
            return None;
        }
        if minutes_remaining > self.config.max_minutes_remaining {
            if let Some(ds) = debug_stats { ds.record_skip("too_far_from_resolution"); }
            return None;
        }
        if mm_staleness_s < self.config.min_mm_staleness_s {
            if let Some(ds) = debug_stats {
                ds.record_skip("mm_not_stale_enough");
                ds.mm_stalenesses.push(mm_staleness_s);
                // Near miss if close
                if mm_staleness_s > self.config.min_mm_staleness_s * 0.5 {
                    ds.record_near_miss(contract_id, "mm_staleness",
                        mm_staleness_s, self.config.min_mm_staleness_s);
                }
            }
            return None;
        }

        if let Some(ref mut ds) = debug_stats {
            // If we get here, contract passed time + staleness filters
            ds.mm_stalenesses.push(mm_staleness_s);
        }

        // Set window open price on first sight
        let open_price = *self.window_opens.entry(contract_id.to_string()).or_insert(btc_price);

        let btc_move = btc_price - open_price;
        let abs_move = btc_move.abs();

        if abs_move < self.config.min_btc_move {
            if let Some(ds) = debug_stats {
                ds.record_skip("btc_move_too_small");
                if abs_move > self.config.min_btc_move * 0.5 {
                    ds.record_near_miss(contract_id, "btc_move",
                        abs_move, self.config.min_btc_move);
                }
            }
            return None;
        }

        // Determine direction and the relevant MM token price
        let (direction, mm_ask) = if btc_move > 0.0 {
            ("up", mm_up_price)
        } else {
            ("down", mm_down_price)
        };

        // REVERSAL FILTER: require 30-second momentum to agree with direction.
        // All 19 losses had $0 30s pre-entry trend — the move was a transient spike.
        // Sustained momentum (30s agrees) had 100% of wins.
        if !self.momentum_agrees(direction, 30.0) {
            if let Some(ds) = debug_stats { ds.record_skip("reversal_filter_30s"); }
            return None;
        }

        // BANKROLL CHECK: don't exceed available capital
        if self.available_capital() < self.config.entry_size_usd {
            if let Some(ds) = debug_stats { ds.record_skip("bankroll_exhausted"); }
            return None;
        }

        // Skip if MM price is already adjusted (not stale)
        if mm_ask > 0.70 || mm_ask < 0.05 {
            if let Some(ds) = debug_stats { ds.record_skip("mm_price_already_adjusted"); }
            return None;
        }

        // Compute fair value using BS
        let sigma_remaining = btc_price * vol * (minutes_remaining / 525600.0_f64).sqrt();
        let sigma_remaining = sigma_remaining.max(1.0);
        let fair_value = fair_value::norm_cdf(abs_move / sigma_remaining);

        let edge = fair_value - mm_ask;

        if let Some(ref mut ds) = debug_stats {
            ds.edges_seen.push(edge);
        }

        if edge < self.config.min_edge {
            if let Some(ds) = debug_stats {
                ds.record_skip("edge_too_small");
                if edge > self.config.min_edge * 0.5 {
                    ds.record_near_miss(contract_id, "edge",
                        edge, self.config.min_edge);
                }
            }
            return None;
        }

        // Check if we already have a position in this window
        let existing = self.positions.get(contract_id);

        if let Some(pos) = existing {
            if pos.n_entries() >= self.config.max_entries_per_window {
                if let Some(ds) = debug_stats { ds.record_skip("max_entries_reached"); }
                return None;
            }
            if pos.total_size_usd >= self.config.max_size_per_window_usd {
                if let Some(ds) = debug_stats { ds.record_skip("max_size_reached"); }
                return None;
            }

            let last_btc = self.last_entry_btc.get(contract_id).copied().unwrap_or(open_price);
            let additional_move = (btc_price - last_btc).abs();
            if additional_move < self.config.scale_in_btc_increment {
                if let Some(ds) = debug_stats { ds.record_skip("scale_in_increment_not_met"); }
                return None;
            }

            if edge < self.config.edge_shrink_stop {
                if let Some(ds) = debug_stats { ds.record_skip("edge_shrunk"); }
                return None;
            }
        }

        // Generate signal
        let tick_to_signal_us = tick_start_ns / 1000; // approximate

        let entry_number = existing.map(|p| p.n_entries() + 1).unwrap_or(1);
        let prev_size = existing.map(|p| p.total_size_usd).unwrap_or(0.0);

        // Cap size by available capital
        let size_usd = self.config.entry_size_usd.min(self.available_capital());
        if size_usd < 0.50 {
            if let Some(ds) = debug_stats { ds.record_skip("bankroll_exhausted"); }
            return None;
        }
        let total_after = prev_size + size_usd;

        let now_s = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        // Update position tracking
        if let Some(pos) = self.positions.get_mut(contract_id) {
            pos.add_entry(mm_ask, fair_value, btc_price, size_usd, now_s);
        } else {
            let mut pos = AccumulatedPosition::new(
                contract_id, direction, window_end_s, window_minutes,
                self.config.max_size_per_window_usd,
            );
            pos.add_entry(mm_ask, fair_value, btc_price, size_usd, now_s);
            self.positions.insert(contract_id.to_string(), pos);
        }
        self.last_entry_btc.insert(contract_id.to_string(), btc_price);
        self.signal_count += 1;
        self.capital_locked += size_usd;

        Some(LatencySignal {
            action: if entry_number == 1 { "enter".to_string() } else { "scale_in".to_string() },
            contract_id: contract_id.to_string(),
            token_id: token_id.to_string(),
            direction: direction.to_string(),
            mm_price: mm_ask,
            fair_value,
            edge,
            btc_price,
            btc_move,
            mm_staleness_s,
            entry_number,
            total_position_usd: total_after,
            size_usd,
            minutes_remaining,
            window_minutes,
            tick_to_signal_us,
            sources: n_sources,
            spread,
            timestamp_s: now_s,
        })
    }

    /// Clean up expired positions (window has ended) and release capital
    pub fn cleanup_expired(&mut self, now_s: f64) {
        let expired: Vec<String> = self.positions.iter()
            .filter(|(_, p)| now_s > p.window_end_s)
            .map(|(k, _)| k.clone())
            .collect();

        for cid in &expired {
            if let Some(pos) = self.positions.remove(cid) {
                self.capital_locked = (self.capital_locked - pos.total_size_usd).max(0.0);
            }
            self.window_opens.remove(cid);
            self.last_entry_btc.remove(cid);
        }
    }
}
