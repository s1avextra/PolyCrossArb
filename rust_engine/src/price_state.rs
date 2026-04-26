//! Shared multi-source price aggregation state.
//!
//! Lives in the library so both binaries (`polymomentum-engine` and the
//! legacy `polymomentum-legacy`) and the `exchange` module can share it.

use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct PriceState {
    pub prices: HashMap<String, f64>,
    pub last_update: Instant,
    pub mid_price: f64,
    pub spread: f64,
    pub implied_vol: f64,
    pub source_timestamps: HashMap<String, Instant>,
    pub alt_prices: HashMap<String, HashMap<String, f64>>,
    pub alt_mid: HashMap<String, f64>,
    pub alt_timestamps: HashMap<String, Instant>,
}

impl Default for PriceState {
    fn default() -> Self {
        Self::new()
    }
}

impl PriceState {
    pub fn new() -> Self {
        Self {
            prices: HashMap::new(),
            last_update: Instant::now(),
            mid_price: 0.0,
            spread: 0.0,
            implied_vol: 0.50,
            source_timestamps: HashMap::new(),
            alt_prices: HashMap::new(),
            alt_mid: HashMap::new(),
            alt_timestamps: HashMap::new(),
        }
    }

    pub fn update(&mut self, source: &str, price: f64) {
        if price <= 0.0 {
            return;
        }
        self.prices.insert(source.to_string(), price);
        self.source_timestamps.insert(source.to_string(), Instant::now());
        self.last_update = Instant::now();

        let now = Instant::now();
        let live: Vec<f64> = self
            .prices
            .iter()
            .filter(|(src, _)| {
                self.source_timestamps
                    .get(*src)
                    .map(|t| now.duration_since(*t).as_secs() < 10)
                    .unwrap_or(false)
            })
            .map(|(_, p)| *p)
            .collect();

        if !live.is_empty() {
            self.mid_price = live.iter().sum::<f64>() / live.len() as f64;
            let min = live.iter().cloned().fold(f64::MAX, f64::min);
            let max = live.iter().cloned().fold(f64::MIN, f64::max);
            self.spread = max - min;
        }
    }

    pub fn update_alt(&mut self, asset: &str, source: &str, price: f64) {
        if price <= 0.0 {
            return;
        }
        let key = format!("{asset}:{source}");
        self.alt_timestamps.insert(key, Instant::now());

        let sources = self.alt_prices.entry(asset.to_string()).or_default();
        sources.insert(source.to_string(), price);

        let now = Instant::now();
        let live: Vec<f64> = sources
            .iter()
            .filter(|(src, _)| {
                let key = format!("{asset}:{src}");
                self.alt_timestamps
                    .get(&key)
                    .map(|t| now.duration_since(*t).as_secs() < 10)
                    .unwrap_or(false)
            })
            .map(|(_, p)| *p)
            .collect();

        if !live.is_empty() {
            self.alt_mid.insert(
                asset.to_string(),
                live.iter().sum::<f64>() / live.len() as f64,
            );
        }
    }

    pub fn n_live_sources(&self) -> usize {
        let now = Instant::now();
        self.source_timestamps
            .values()
            .filter(|t| now.duration_since(**t).as_secs() < 10)
            .count()
    }
}
