//! Paper-mode fill model.
//!
//! Mirrors `candle_pipeline.py::_execute_candle_trade` paper branch:
//! adverse 30 bps slippage on top-of-book + crypto-category fee. Maker mode
//! flips to a -10 bps improvement and a maker rebate (0% fee).

use crate::execution::fees::polymarket_fee;

#[derive(Debug, Clone, Copy)]
pub struct PaperFillCfg {
    pub prefer_maker: bool,
    pub default_taker_rate: f64,
    pub slippage_bps_taker: i32,
    pub slippage_bps_maker: i32,
}

impl Default for PaperFillCfg {
    fn default() -> Self {
        Self {
            prefer_maker: false,
            default_taker_rate: 0.072,
            slippage_bps_taker: 30,
            slippage_bps_maker: -10,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PaperFill {
    pub fill_price: f64,
    pub shares: f64,
    pub fee: f64,
    pub fee_rate: f64,
}

pub fn simulate_paper_fill(
    market_price: f64,
    position_usd: f64,
    cfg: &PaperFillCfg,
) -> Option<PaperFill> {
    if market_price <= 0.0 || position_usd <= 0.0 {
        return None;
    }
    let (slippage_bps, fee_rate) = if cfg.prefer_maker {
        (cfg.slippage_bps_maker, 0.0)
    } else {
        (cfg.slippage_bps_taker, cfg.default_taker_rate)
    };
    let slipped = market_price * (1.0 + slippage_bps as f64 / 10_000.0);
    let slipped = slipped.clamp(0.01, 0.99);
    let shares = position_usd / slipped;
    let fee = polymarket_fee(shares, slipped, fee_rate);
    Some(PaperFill {
        fill_price: slipped,
        shares,
        fee,
        fee_rate,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taker_slippage_is_adverse() {
        let cfg = PaperFillCfg::default();
        let f = simulate_paper_fill(0.50, 10.0, &cfg).unwrap();
        assert!(f.fill_price > 0.50);
        assert!(f.shares > 0.0);
        assert!(f.fee > 0.0);
    }

    #[test]
    fn maker_slippage_is_improvement() {
        let cfg = PaperFillCfg { prefer_maker: true, ..PaperFillCfg::default() };
        let f = simulate_paper_fill(0.50, 10.0, &cfg).unwrap();
        assert!(f.fill_price < 0.50);
        assert!(f.fee.abs() < 1e-9); // maker rebate = 0%
    }

    #[test]
    fn refuses_invalid_price() {
        let cfg = PaperFillCfg::default();
        assert!(simulate_paper_fill(0.0, 10.0, &cfg).is_none());
        assert!(simulate_paper_fill(0.5, 0.0, &cfg).is_none());
    }
}
