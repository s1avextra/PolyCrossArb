//! BTC/ETH/etc momentum detector.
//!
//! Volatility-normalized signal: z-score = price move / (σ × √window).
//! Volatility is estimated via fast (~15min) and slow (~4h) EWMA of squared
//! log returns, with a configurable floor.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolatilityRegime {
    Low,
    Normal,
    High,
    Extreme,
}

impl VolatilityRegime {
    pub fn as_str(&self) -> &'static str {
        match self {
            VolatilityRegime::Low => "low",
            VolatilityRegime::Normal => "normal",
            VolatilityRegime::High => "high",
            VolatilityRegime::Extreme => "extreme",
        }
    }
}

pub fn classify_vol_regime(short_vol: f64, baseline_vol: f64) -> VolatilityRegime {
    if baseline_vol <= 0.0 || short_vol <= 0.0 {
        return VolatilityRegime::Normal;
    }
    let ratio = short_vol / baseline_vol;
    if ratio > 2.5 {
        VolatilityRegime::Extreme
    } else if ratio > 1.5 {
        VolatilityRegime::High
    } else if ratio < 0.5 {
        VolatilityRegime::Low
    } else {
        VolatilityRegime::Normal
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MomentumSignal {
    pub direction: String,
    pub confidence: f64,
    pub price_change: f64,
    pub price_change_pct: f64,
    pub consistency: f64,
    pub minutes_elapsed: f64,
    pub minutes_remaining: f64,
    pub current_price: f64,
    pub open_price: f64,
    pub z_score: f64,
    pub reversion_count: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct MomentumConfig {
    pub noise_z_threshold: f64,
    pub fast_vol_half_life_min: f64,
    pub slow_vol_half_life_min: f64,
    pub floor_vol: f64,
    pub max_ticks: usize,
}

impl Default for MomentumConfig {
    fn default() -> Self {
        Self {
            noise_z_threshold: 0.3,
            fast_vol_half_life_min: 15.0,
            slow_vol_half_life_min: 240.0,
            floor_vol: 0.10,
            max_ticks: 5000,
        }
    }
}

pub struct MomentumDetector {
    cfg: MomentumConfig,
    ticks: VecDeque<(f64, f64)>, // (ts, price)
    window_opens: HashMap<String, f64>,
    seed_vol: f64,
    fast_tau_s: f64,
    slow_tau_s: f64,
    fast_var: f64,
    slow_var: f64,
    ewma_warmed: bool,
}

const SECONDS_PER_YEAR: f64 = 365.25 * 86400.0;

impl MomentumDetector {
    pub fn new(seed_vol: Option<f64>, cfg: MomentumConfig) -> Self {
        let seed = seed_vol.unwrap_or(0.50);
        let seed = if seed > 0.0 { seed } else { 0.50 };
        let fast_tau_s = cfg.fast_vol_half_life_min * 60.0 / std::f64::consts::LN_2;
        let slow_tau_s = cfg.slow_vol_half_life_min * 60.0 / std::f64::consts::LN_2;
        Self {
            cfg,
            ticks: VecDeque::with_capacity(5000),
            window_opens: HashMap::new(),
            seed_vol: seed,
            fast_tau_s,
            slow_tau_s,
            fast_var: 0.0,
            slow_var: 0.0,
            ewma_warmed: false,
        }
    }

    pub fn realized_vol(&self) -> f64 {
        if !self.ewma_warmed {
            return self.seed_vol;
        }
        let v = (self.fast_var.max(0.0) * SECONDS_PER_YEAR).sqrt();
        v.clamp(self.cfg.floor_vol, 5.0)
    }

    pub fn slow_realized_vol(&self) -> f64 {
        if !self.ewma_warmed {
            return self.seed_vol;
        }
        let v = (self.slow_var.max(0.0) * SECONDS_PER_YEAR).sqrt();
        v.clamp(self.cfg.floor_vol, 5.0)
    }

    pub fn vol_ratio(&self) -> f64 {
        let slow = self.slow_realized_vol();
        if slow <= 0.0 {
            1.0
        } else {
            self.realized_vol() / slow
        }
    }

    pub fn set_realized_vol(&mut self, vol: f64) {
        if vol > 0.0 && !self.ewma_warmed {
            self.seed_vol = vol;
        }
    }

    pub fn add_tick(&mut self, price: f64, timestamp: Option<f64>) {
        let ts = timestamp.unwrap_or_else(now_ts);
        if let Some(&(last_ts, last_price)) = self.ticks.back() {
            let dt = ts - last_ts;
            if dt > 0.0 && last_price > 0.0 && price > 0.0 {
                let log_return = (price / last_price).ln();
                let r2_rate = (log_return * log_return) / dt;
                if self.ewma_warmed {
                    let fast_alpha = 1.0 - (-dt / self.fast_tau_s).exp();
                    let slow_alpha = 1.0 - (-dt / self.slow_tau_s).exp();
                    self.fast_var = (1.0 - fast_alpha) * self.fast_var + fast_alpha * r2_rate;
                    self.slow_var = (1.0 - slow_alpha) * self.slow_var + slow_alpha * r2_rate;
                } else {
                    self.fast_var = r2_rate;
                    self.slow_var = r2_rate;
                    self.ewma_warmed = true;
                }
            }
        }
        if self.ticks.len() == self.cfg.max_ticks {
            self.ticks.pop_front();
        }
        self.ticks.push_back((ts, price));
    }

    pub fn set_window_open(&mut self, contract_id: &str, price: f64) {
        self.window_opens.insert(contract_id.to_string(), price);
    }

    pub fn get_open_price(&self, contract_id: &str) -> Option<f64> {
        self.window_opens.get(contract_id).copied()
    }

    pub fn evict_stale_windows(&mut self, active: &HashSet<String>) -> usize {
        let stale: Vec<String> = self
            .window_opens
            .keys()
            .filter(|k| !active.contains(*k))
            .cloned()
            .collect();
        let n = stale.len();
        for s in stale {
            self.window_opens.remove(&s);
        }
        n
    }

    pub fn detect(
        &mut self,
        contract_id: &str,
        window_start_ago_minutes: f64,
        minutes_remaining: f64,
        current_price: f64,
        now_ts_override: Option<f64>,
    ) -> Option<MomentumSignal> {
        if self.ticks.is_empty() || minutes_remaining <= 0.0 {
            return None;
        }
        let now = now_ts_override.unwrap_or_else(now_ts);
        let window_start = now - window_start_ago_minutes * 60.0;

        let open_price = match self.window_opens.get(contract_id).copied() {
            Some(p) => p,
            None => {
                let mut found = None;
                for &(ts, price) in self.ticks.iter() {
                    if ts >= window_start {
                        found = Some(price);
                        break;
                    }
                }
                if let Some(p) = found {
                    self.window_opens.insert(contract_id.to_string(), p);
                }
                found?
            }
        };

        if open_price <= 0.0 {
            return None;
        }

        let price_change = current_price - open_price;
        let price_change_pct = price_change / open_price;
        let direction = if price_change >= 0.0 { "up" } else { "down" };

        // Walk ticks newest → oldest, stop at window_start.
        let mut recent: Vec<(f64, f64)> = Vec::new();
        for &(ts, p) in self.ticks.iter().rev() {
            if ts < window_start {
                break;
            }
            recent.push((ts, p));
        }
        if recent.len() < 3 {
            return None;
        }
        recent.reverse();

        let mut consistent = 0;
        let mut reversion_count = 0u32;
        let mut prev_side: Option<bool> = None;
        for i in 1..recent.len() {
            let tick_dir = recent[i].1 - recent[i - 1].1;
            let agrees = match direction {
                "up" => tick_dir >= 0.0,
                _ => tick_dir <= 0.0,
            };
            if agrees {
                consistent += 1;
            }
            let curr_side = recent[i].1 >= open_price;
            if let Some(prev) = prev_side {
                if curr_side != prev {
                    reversion_count += 1;
                }
            }
            prev_side = Some(curr_side);
        }
        let consistency = consistent as f64 / (recent.len() - 1).max(1) as f64;

        let minutes_elapsed = window_start_ago_minutes;
        let total_window = minutes_elapsed + minutes_remaining;
        let current_vol = self.realized_vol();
        let mut sigma_window = open_price * current_vol * (total_window / 525_600.0).sqrt();
        if sigma_window < 1.0 {
            sigma_window = 1.0;
        }
        let z_score = price_change.abs() / sigma_window;

        let time_factor = if total_window > 0.0 {
            (minutes_elapsed / total_window).min(1.0)
        } else {
            0.0
        };
        let reversion_penalty = (1.0 - reversion_count as f64 * 0.05).max(0.0);
        let z_factor = (z_score / 3.0).min(1.0);

        let mut confidence = 0.35 * time_factor
            + 0.35 * z_factor
            + 0.15 * consistency
            + 0.15 * reversion_penalty;
        confidence = confidence.clamp(0.10, 0.95);

        if minutes_remaining < 1.0 && z_score > 0.5 {
            confidence = (confidence + 0.10 * z_score.min(2.0)).min(0.95);
        } else if minutes_remaining < 2.0 && z_score > 1.0 {
            confidence = (confidence + 0.05 * z_score.min(3.0)).min(0.95);
        }

        if z_score < self.cfg.noise_z_threshold {
            confidence *= 0.4;
        }

        Some(MomentumSignal {
            direction: direction.to_string(),
            confidence,
            price_change,
            price_change_pct,
            consistency,
            minutes_elapsed,
            minutes_remaining,
            current_price,
            open_price,
            z_score,
            reversion_count,
        })
    }
}

fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_with_no_ticks_is_none() {
        let mut det = MomentumDetector::new(None, MomentumConfig::default());
        assert!(det.detect("c", 1.0, 4.0, 70_000.0, Some(0.0)).is_none());
    }

    #[test]
    fn upward_move_produces_up_signal() {
        let mut det = MomentumDetector::new(Some(0.5), MomentumConfig::default());
        let t0 = 1_700_000_000.0;
        // Add 60 ticks across a 5-min window, monotonically rising.
        for i in 0..60 {
            det.add_tick(70_000.0 + i as f64 * 5.0, Some(t0 + i as f64 * 5.0));
        }
        let now = t0 + 300.0;
        let sig = det.detect("c", 5.0, 0.5, 70_000.0 + 60.0 * 5.0, Some(now));
        let sig = sig.expect("signal");
        assert_eq!(sig.direction, "up");
        assert!(sig.consistency > 0.5);
        assert!(sig.confidence > 0.0);
    }

    #[test]
    fn evict_stale_windows() {
        let mut det = MomentumDetector::new(None, MomentumConfig::default());
        det.set_window_open("a", 1.0);
        det.set_window_open("b", 2.0);
        det.set_window_open("c", 3.0);
        let mut active = HashSet::new();
        active.insert("a".to_string());
        let n = det.evict_stale_windows(&active);
        assert_eq!(n, 2);
        assert!(det.get_open_price("a").is_some());
        assert!(det.get_open_price("b").is_none());
    }
}
