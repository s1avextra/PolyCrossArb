//! Historical BTC price replay with causality-guaranteed lookup.
//!
//! Two ingestion paths:
//!   - `load_csv` — read a Binance kline CSV (timestamp,open,high,low,close,volume)
//!     or a collector tick CSV (timestamp_ms,price,...). For klines we shift
//!     the stored timestamp to `open_time + interval` so a query at T cannot
//!     return a close price that wasn't yet observable at T.
//!   - `load_from_binance` — pull kline windows from the Binance public REST.
//!
//! Lookup is `O(log n)` via binary search over the sorted timestamp vector.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;

#[derive(Default, Clone)]
pub struct BTCHistory {
    pub(crate) timestamps_ms: Vec<i64>, // sorted ascending
    pub(crate) prices: Vec<f64>,
}

impl BTCHistory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn n_ticks(&self) -> usize {
        self.timestamps_ms.len()
    }

    pub fn time_span_seconds(&self) -> f64 {
        if self.timestamps_ms.len() < 2 {
            return 0.0;
        }
        (self.timestamps_ms.last().unwrap() - self.timestamps_ms.first().unwrap()) as f64 / 1000.0
    }

    pub fn first_timestamp_ms(&self) -> i64 {
        self.timestamps_ms.first().copied().unwrap_or(0)
    }

    pub fn last_timestamp_ms(&self) -> i64 {
        self.timestamps_ms.last().copied().unwrap_or(0)
    }

    /// Load a CSV. Auto-detects schema:
    ///   - Binance kline:    `timestamp,open,high,low,close,volume` (kline open_time)
    ///   - Collector ticks:  `timestamp_ms,...,price,...` (observation time)
    ///
    /// Klines are stored at `open_time + interval` so a `price_at(T)` query can
    /// only ever return a close that was observable at T.
    pub fn load_csv(&mut self, path: impl AsRef<Path>) -> Result<usize> {
        let path = path.as_ref();
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(path)
            .with_context(|| format!("open csv {}", path.display()))?;
        let headers: Vec<String> = reader
            .headers()?
            .iter()
            .map(|s| s.to_string())
            .collect();
        if headers.is_empty() {
            return Ok(0);
        }

        let lower: Vec<String> = headers.iter().map(|h| h.to_lowercase()).collect();
        let (ts_idx, price_idx, is_kline) = if lower.contains(&"timestamp".to_string())
            && lower.contains(&"close".to_string())
        {
            (
                lower.iter().position(|h| h == "timestamp").unwrap(),
                lower.iter().position(|h| h == "close").unwrap(),
                true,
            )
        } else if lower.contains(&"timestamp_ms".to_string())
            && lower.contains(&"price".to_string())
        {
            (
                lower.iter().position(|h| h == "timestamp_ms").unwrap(),
                lower.iter().position(|h| h == "price").unwrap(),
                false,
            )
        } else if lower.contains(&"timestamp".to_string()) && lower.len() >= 6 {
            (lower.iter().position(|h| h == "timestamp").unwrap(), 4, true)
        } else {
            anyhow::bail!("unknown CSV schema in {}: {:?}", path.display(), headers);
        };

        let mut raw: Vec<(i64, f64)> = Vec::new();
        for rec in reader.records() {
            let rec = match rec {
                Ok(r) => r,
                Err(_) => continue,
            };
            if rec.len() <= ts_idx.max(price_idx) {
                continue;
            }
            let ts = match rec.get(ts_idx).and_then(|s| s.parse::<f64>().ok()) {
                Some(v) => v as i64,
                None => continue,
            };
            let price = match rec.get(price_idx).and_then(|s| s.parse::<f64>().ok()) {
                Some(v) if v > 0.0 => v,
                _ => continue,
            };
            raw.push((ts, price));
        }
        if raw.is_empty() {
            return Ok(0);
        }
        raw.sort_by_key(|r| r.0);

        let added = raw.len();
        if is_kline {
            // Detect interval as the smallest positive gap; treat that as the
            // bar width. Storing at open_time + interval guarantees no
            // lookahead.
            let mut interval_ms = 1000_i64;
            let mut min_diff = i64::MAX;
            for w in raw.windows(2) {
                let diff = w[1].0 - w[0].0;
                if diff > 0 && diff < min_diff {
                    min_diff = diff;
                }
            }
            if min_diff != i64::MAX {
                interval_ms = min_diff;
            }
            for (ts, p) in raw {
                self.timestamps_ms.push(ts + interval_ms);
                self.prices.push(p);
            }
        } else {
            for (ts, p) in raw {
                self.timestamps_ms.push(ts);
                self.prices.push(p);
            }
        }
        self.dedupe_and_sort();
        Ok(added)
    }

    /// Pull klines from Binance public REST. Stores at `close_time` so the
    /// causality contract holds.
    pub async fn load_from_binance(
        &mut self,
        start_ms: i64,
        end_ms: i64,
        symbol: &str,
        interval: &str,
    ) -> Result<usize> {
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let mut cursor = start_ms;
        let mut added = 0usize;
        while cursor < end_ms {
            let resp = client
                .get("https://api.binance.com/api/v3/klines")
                .query(&[
                    ("symbol", symbol),
                    ("interval", interval),
                    ("startTime", &cursor.to_string()),
                    ("endTime", &end_ms.to_string()),
                    ("limit", "1000"),
                ])
                .send()
                .await;
            let resp = match resp {
                Ok(r) if r.status().is_success() => r,
                Ok(r) => {
                    tracing::warn!(status = %r.status(), "Binance kline non-2xx");
                    break;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Binance kline error");
                    break;
                }
            };
            let klines: Value = resp.json().await?;
            let arr = match klines.as_array() {
                Some(a) if !a.is_empty() => a,
                _ => break,
            };
            let mut last_open = cursor;
            for k in arr {
                let kk = match k.as_array() {
                    Some(a) if a.len() >= 7 => a,
                    _ => continue,
                };
                let close_time = kk[6].as_i64().unwrap_or(0);
                let close = kk[4]
                    .as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| kk[4].as_f64())
                    .unwrap_or(0.0);
                if close <= 0.0 || close_time <= 0 {
                    continue;
                }
                self.timestamps_ms.push(close_time);
                self.prices.push(close);
                added += 1;
                last_open = kk[0].as_i64().unwrap_or(close_time);
            }
            if last_open <= cursor {
                break;
            }
            cursor = last_open + 1;
            if arr.len() < 1000 {
                break;
            }
        }
        if added > 0 {
            self.dedupe_and_sort();
        }
        Ok(added)
    }

    fn dedupe_and_sort(&mut self) {
        if self.timestamps_ms.len() != self.prices.len() {
            return;
        }
        let mut paired: Vec<(i64, f64)> = self
            .timestamps_ms
            .drain(..)
            .zip(self.prices.drain(..))
            .collect();
        paired.sort_by_key(|p| p.0);
        let mut seen: BTreeMap<i64, f64> = BTreeMap::new();
        for (ts, p) in paired {
            seen.entry(ts).or_insert(p);
        }
        self.timestamps_ms.clear();
        self.prices.clear();
        for (ts, p) in seen {
            self.timestamps_ms.push(ts);
            self.prices.push(p);
        }
    }

    /// Most recent observable price at time T. Returns 0 if no data is
    /// available at or before T.
    pub fn price_at(&self, timestamp_ms: i64) -> f64 {
        if self.timestamps_ms.is_empty() {
            return 0.0;
        }
        let idx = match self.timestamps_ms.binary_search(&timestamp_ms) {
            Ok(i) => i,
            Err(0) => return 0.0,
            Err(i) => i - 1,
        };
        self.prices[idx]
    }

    pub fn price_at_seconds(&self, timestamp_s: f64) -> f64 {
        self.price_at((timestamp_s * 1000.0) as i64)
    }

    /// (open, high, low, close) over `[start_ms, end_ms]`. Returns zeros if
    /// the window has no ticks.
    pub fn range_at(&self, start_ms: i64, end_ms: i64) -> (f64, f64, f64, f64) {
        let lo = self
            .timestamps_ms
            .partition_point(|&t| t < start_ms);
        let hi = self
            .timestamps_ms
            .partition_point(|&t| t <= end_ms);
        if lo >= hi {
            return (0.0, 0.0, 0.0, 0.0);
        }
        let prices = &self.prices[lo..hi];
        let mut high = prices[0];
        let mut low = prices[0];
        for &p in prices {
            if p > high {
                high = p;
            }
            if p < low {
                low = p;
            }
        }
        (prices[0], high, low, *prices.last().unwrap())
    }

    /// Annualized realized volatility from log returns in the lookback window.
    /// Returns 0.50 if there's insufficient data, mirroring the Python default.
    pub fn realized_vol_at(&self, timestamp_ms: i64, lookback_seconds: f64) -> f64 {
        if self.timestamps_ms.len() < 50 {
            return 0.50;
        }
        let cutoff_lo = timestamp_ms - (lookback_seconds * 1000.0) as i64;
        let cutoff_hi = timestamp_ms;
        let lo = self.timestamps_ms.partition_point(|&t| t < cutoff_lo);
        let hi = self.timestamps_ms.partition_point(|&t| t <= cutoff_hi);
        if hi.saturating_sub(lo) < 30 {
            return 0.50;
        }
        let window_ts = &self.timestamps_ms[lo..hi];
        let window_p = &self.prices[lo..hi];

        let mut log_returns: Vec<f64> = Vec::with_capacity(window_p.len().saturating_sub(1));
        let mut deltas: Vec<f64> = Vec::with_capacity(window_p.len().saturating_sub(1));
        for i in 1..window_p.len() {
            if window_p[i - 1] <= 0.0 {
                continue;
            }
            let r = (window_p[i] / window_p[i - 1]).ln();
            let dt = (window_ts[i] - window_ts[i - 1]) as f64 / 1000.0;
            if dt > 0.0 {
                log_returns.push(r);
                deltas.push(dt);
            }
        }
        if log_returns.len() < 20 {
            return 0.50;
        }
        let avg_dt = deltas.iter().sum::<f64>() / deltas.len() as f64;
        let mean_r = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
        let var_r = log_returns
            .iter()
            .map(|r| (r - mean_r).powi(2))
            .sum::<f64>()
            / log_returns.len() as f64;
        if avg_dt <= 0.0 {
            return 0.50;
        }
        let var_per_second = var_r / avg_dt;
        let annualized = (var_per_second * 365.25 * 86400.0).sqrt();
        annualized.clamp(0.05, 5.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_csv(rows: &[&str]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        for r in rows {
            writeln!(f, "{}", r).unwrap();
        }
        f
    }

    #[test]
    fn empty_history_returns_zero() {
        let h = BTCHistory::new();
        assert_eq!(h.price_at(0), 0.0);
        assert_eq!(h.price_at(1_700_000_000_000), 0.0);
    }

    #[test]
    fn binance_kline_csv_shifts_to_close_time() {
        // Binance kline rows: open_time, open, high, low, close, volume
        // Stored at open_time + interval so a query exactly at open_time
        // returns 0 (the kline isn't observable yet).
        let f = write_csv(&[
            "timestamp,open,high,low,close,volume",
            "1700000000000,70000,70010,69990,70005,1.0",
            "1700000060000,70005,70015,69995,70010,1.0",
            "1700000120000,70010,70020,70000,70015,1.0",
        ]);
        let mut h = BTCHistory::new();
        h.load_csv(f.path()).unwrap();

        // Interval = 60_000 ms. Stored timestamps = open_time + 60_000.
        // Query at exact open_time => returns 0 (no kline closed yet).
        assert_eq!(h.price_at(1_700_000_000_000), 0.0);
        // Query at close_time of first kline => returns its close.
        assert!((h.price_at(1_700_000_060_000) - 70005.0).abs() < 1e-9);
        // Query 1ms before close => still 0 (causality).
        assert_eq!(h.price_at(1_700_000_059_999), 0.0);
    }

    #[test]
    fn collector_tick_csv_uses_observation_time() {
        let f = write_csv(&[
            "timestamp_ms,source,price",
            "1700000000000,binance,70000",
            "1700000001000,bybit,70010",
            "1700000002000,okx,70020",
        ]);
        let mut h = BTCHistory::new();
        h.load_csv(f.path()).unwrap();
        assert_eq!(h.price_at(1_699_999_999_000), 0.0);
        assert!((h.price_at(1_700_000_001_500) - 70010.0).abs() < 1e-9);
        assert!((h.price_at(1_700_000_010_000) - 70020.0).abs() < 1e-9);
    }

    #[test]
    fn realized_vol_returns_default_below_threshold() {
        let mut h = BTCHistory::new();
        // Just a couple of ticks — below the 50 min threshold.
        h.timestamps_ms = vec![1, 2, 3];
        h.prices = vec![100.0, 101.0, 99.0];
        assert!((h.realized_vol_at(3, 3600.0) - 0.50).abs() < 1e-9);
    }

    #[test]
    fn range_at_returns_ohlc() {
        let f = write_csv(&[
            "timestamp_ms,source,price",
            "1700000000000,b,100",
            "1700000001000,b,105",
            "1700000002000,b,95",
            "1700000003000,b,102",
        ]);
        let mut h = BTCHistory::new();
        h.load_csv(f.path()).unwrap();
        let (o, hi, lo, c) = h.range_at(1_700_000_000_000, 1_700_000_003_000);
        assert!((o - 100.0).abs() < 1e-9);
        assert!((hi - 105.0).abs() < 1e-9);
        assert!((lo - 95.0).abs() < 1e-9);
        assert!((c - 102.0).abs() < 1e-9);
    }
}
