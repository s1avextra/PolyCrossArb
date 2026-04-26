//! Backtest fill models for synthetic order book fills.
//!
//! Models:
//! - [`OneTickTaker`]   — touch + 1 tick adverse (default for market orders)
//! - [`BookWalkTaker`]  — walks real L2 depth, falls back to one-tick adverse
//! - [`Maker`]          — probabilistic post-at-touch with taker fallback
//! - [`Perfect`]        — touch fill, no slippage (sanity baseline)
//!
//! All return `FillResult` with `success=false` and a `reason` when the
//! input is invalid; never panic on bad books.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub const DEFAULT_TICK: f64 = 0.01;

#[derive(Debug, Clone, Copy)]
pub struct FillResult {
    pub filled_size: f64,
    pub fill_price: f64,
    pub fill_cost: f64, // signed: positive for buy, negative for sell
    pub slippage_per_share: f64,
    pub success: bool,
    pub reason: FillReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillReason {
    None,
    Empty,
    Invalid,
    LimitNotCrossed,
    LimitMissingPrice,
    MakerFill,
    TakerFallback,
}

impl FillReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            FillReason::None => "",
            FillReason::Empty => "size <= 0",
            FillReason::Invalid => "invalid book",
            FillReason::LimitNotCrossed => "limit not crossed",
            FillReason::LimitMissingPrice => "limit price required",
            FillReason::MakerFill => "maker_fill",
            FillReason::TakerFallback => "taker_fallback",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn from_str(s: &str) -> Option<Side> {
        match s.to_lowercase().as_str() {
            "buy" | "b" => Some(Side::Buy),
            "sell" | "s" => Some(Side::Sell),
            _ => None,
        }
    }

    fn cost_sign(&self) -> f64 {
        match self {
            Side::Buy => 1.0,
            Side::Sell => -1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Market,
    Limit,
}

fn one_tick_adverse_price(side: Side, best_bid: f64, best_ask: f64, tick_size: f64) -> f64 {
    let p = match side {
        Side::Buy => best_ask + tick_size,
        Side::Sell => best_bid - tick_size,
    };
    p.clamp(tick_size, 1.0 - tick_size)
}

/// Synthetic-book taker fill model. Market orders pay touch + 1 tick adverse;
/// limit orders that cross fill at touch.
#[derive(Debug, Clone, Copy)]
pub struct OneTickTaker {
    pub tick_size: f64,
}

impl Default for OneTickTaker {
    fn default() -> Self {
        Self { tick_size: DEFAULT_TICK }
    }
}

impl OneTickTaker {
    pub fn fill(
        &self,
        side: Side,
        size: f64,
        best_bid: f64,
        best_ask: f64,
        order_type: OrderType,
        limit_price: Option<f64>,
    ) -> FillResult {
        if size <= 0.0 {
            return failed(FillReason::Empty);
        }
        if best_bid <= 0.0 || best_ask <= 0.0 || best_bid >= best_ask {
            return failed(FillReason::Invalid);
        }

        let (fill_price, slippage) = match order_type {
            OrderType::Limit => {
                let Some(lp) = limit_price else {
                    return failed(FillReason::LimitMissingPrice);
                };
                match side {
                    Side::Buy if lp >= best_ask => (best_ask, 0.0),
                    Side::Sell if lp <= best_bid => (best_bid, 0.0),
                    _ => return failed(FillReason::LimitNotCrossed),
                }
            }
            OrderType::Market => {
                let p = one_tick_adverse_price(side, best_bid, best_ask, self.tick_size);
                let touch = match side {
                    Side::Buy => best_ask,
                    Side::Sell => best_bid,
                };
                (p, (p - touch).abs())
            }
        };

        FillResult {
            filled_size: size,
            fill_price,
            fill_cost: fill_price * size * side.cost_sign(),
            slippage_per_share: slippage,
            success: true,
            reason: FillReason::None,
        }
    }
}

/// Walks real L2 depth (bid/ask vectors). If size exceeds depth, fills the
/// remainder at one-tick adverse from the last known level.
#[derive(Debug, Clone, Copy)]
pub struct BookWalkTaker {
    pub tick_size: f64,
}

impl Default for BookWalkTaker {
    fn default() -> Self {
        Self { tick_size: DEFAULT_TICK }
    }
}

impl BookWalkTaker {
    /// `bids` must be sorted descending by price, `asks` ascending.
    pub fn fill(
        &self,
        side: Side,
        size: f64,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
    ) -> FillResult {
        if size <= 0.0 {
            return failed(FillReason::Empty);
        }
        let levels: &[(f64, f64)] = match side {
            Side::Buy => asks,
            Side::Sell => bids,
        };
        if levels.is_empty() {
            return failed(FillReason::Empty);
        }

        let mut remaining = size;
        let mut total_cost = 0.0;
        for &(price, avail) in levels {
            if remaining <= 0.0 {
                break;
            }
            let take = remaining.min(avail);
            total_cost += take * price;
            remaining -= take;
        }
        if remaining > 0.0 {
            let last = levels[levels.len() - 1].0;
            let synth = match side {
                Side::Buy => last + self.tick_size,
                Side::Sell => (last - self.tick_size).max(self.tick_size),
            };
            total_cost += remaining * synth;
        }

        let vwap = total_cost / size;
        let touch = levels[0].0;
        let slippage = (vwap - touch).abs();
        FillResult {
            filled_size: size,
            fill_price: vwap,
            fill_cost: vwap * size * side.cost_sign(),
            slippage_per_share: slippage,
            success: true,
            reason: FillReason::None,
        }
    }
}

/// Maker-first probabilistic fill. With `fill_prob` we post inside the spread
/// (touch ∓ 1 tick) at 0% fee; otherwise we cross with one-tick adverse.
pub struct Maker {
    pub fill_prob: f64,
    pub tick_size: f64,
    rng: StdRng,
}

impl Maker {
    pub fn new(fill_prob: f64, tick_size: f64, seed: Option<u64>) -> Self {
        let rng = match seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        };
        Self { fill_prob, tick_size, rng }
    }

    pub fn fill(
        &mut self,
        side: Side,
        size: f64,
        best_bid: f64,
        best_ask: f64,
    ) -> FillResult {
        if size <= 0.0 {
            return failed(FillReason::Empty);
        }
        if best_bid <= 0.0 || best_ask <= 0.0 || best_bid >= best_ask {
            return failed(FillReason::Invalid);
        }

        if self.rng.gen::<f64>() < self.fill_prob {
            // Maker fill at improvement vs touch.
            let fill_price = match side {
                Side::Buy => (best_ask - self.tick_size).max(self.tick_size),
                Side::Sell => (best_bid + self.tick_size).min(1.0 - self.tick_size),
            };
            let touch = match side {
                Side::Buy => best_ask,
                Side::Sell => best_bid,
            };
            let improvement = (touch - fill_price).abs();
            FillResult {
                filled_size: size,
                fill_price,
                fill_cost: fill_price * size * side.cost_sign(),
                slippage_per_share: -improvement, // negative = improvement
                success: true,
                reason: FillReason::MakerFill,
            }
        } else {
            // Taker fallback.
            let fill_price = one_tick_adverse_price(side, best_bid, best_ask, self.tick_size);
            let touch = match side {
                Side::Buy => best_ask,
                Side::Sell => best_bid,
            };
            FillResult {
                filled_size: size,
                fill_price,
                fill_cost: fill_price * size * side.cost_sign(),
                slippage_per_share: (fill_price - touch).abs(),
                success: true,
                reason: FillReason::TakerFallback,
            }
        }
    }
}

/// Sanity baseline — fills at touch with zero slippage.
#[derive(Debug, Clone, Copy, Default)]
pub struct Perfect;

impl Perfect {
    pub fn fill(&self, side: Side, size: f64, best_bid: f64, best_ask: f64) -> FillResult {
        if size <= 0.0 {
            return failed(FillReason::Empty);
        }
        let price = match side {
            Side::Buy => best_ask,
            Side::Sell => best_bid,
        };
        FillResult {
            filled_size: size,
            fill_price: price,
            fill_cost: price * size * side.cost_sign(),
            slippage_per_share: 0.0,
            success: true,
            reason: FillReason::None,
        }
    }
}

fn failed(reason: FillReason) -> FillResult {
    FillResult {
        filled_size: 0.0,
        fill_price: 0.0,
        fill_cost: 0.0,
        slippage_per_share: 0.0,
        success: false,
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_tick_taker_market_buy_pays_one_tick_adverse() {
        let f = OneTickTaker::default();
        let r = f.fill(Side::Buy, 10.0, 0.50, 0.52, OrderType::Market, None);
        assert!(r.success);
        assert!((r.fill_price - 0.53).abs() < 1e-9);
        assert!((r.slippage_per_share - 0.01).abs() < 1e-9);
    }

    #[test]
    fn one_tick_taker_limit_not_crossed_fails() {
        let f = OneTickTaker::default();
        let r = f.fill(Side::Buy, 10.0, 0.50, 0.52, OrderType::Limit, Some(0.51));
        assert!(!r.success);
        assert_eq!(r.reason, FillReason::LimitNotCrossed);
    }

    #[test]
    fn one_tick_taker_invalid_book_fails() {
        let f = OneTickTaker::default();
        let r = f.fill(Side::Buy, 10.0, 0.0, 0.0, OrderType::Market, None);
        assert!(!r.success);
        assert_eq!(r.reason, FillReason::Invalid);
    }

    #[test]
    fn book_walk_vwaps_across_levels() {
        let f = BookWalkTaker::default();
        let asks = vec![(0.50, 100.0), (0.60, 50.0)];
        let r = f.fill(Side::Buy, 130.0, &[], &asks);
        assert!(r.success);
        let expected_vwap = (0.50 * 100.0 + 0.60 * 30.0) / 130.0;
        assert!((r.fill_price - expected_vwap).abs() < 1e-9);
    }

    #[test]
    fn book_walk_falls_through_with_synthetic_remainder() {
        let f = BookWalkTaker::default();
        let asks = vec![(0.50, 50.0)];
        let r = f.fill(Side::Buy, 100.0, &[], &asks);
        assert!(r.success);
        // 50@0.50 + 50@0.51 → vwap = 0.505
        let expected = (0.50 * 50.0 + 0.51 * 50.0) / 100.0;
        assert!((r.fill_price - expected).abs() < 1e-9);
    }

    #[test]
    fn perfect_fills_at_touch() {
        let f = Perfect;
        let r = f.fill(Side::Buy, 10.0, 0.50, 0.52);
        assert!(r.success);
        assert!((r.fill_price - 0.52).abs() < 1e-9);
        assert_eq!(r.slippage_per_share, 0.0);
    }

    #[test]
    fn maker_with_seed_is_deterministic() {
        let mut a = Maker::new(0.65, DEFAULT_TICK, Some(42));
        let mut b = Maker::new(0.65, DEFAULT_TICK, Some(42));
        let ra = a.fill(Side::Buy, 1.0, 0.50, 0.52);
        let rb = b.fill(Side::Buy, 1.0, 0.50, 0.52);
        assert!((ra.fill_price - rb.fill_price).abs() < 1e-12);
        assert_eq!(ra.reason, rb.reason);
    }
}
