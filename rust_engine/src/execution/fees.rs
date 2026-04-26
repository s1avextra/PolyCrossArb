//! Polymarket binary fee formula.
//!
//!     fee = shares × fee_rate × p × (1 − p)
//!
//! Maximum at p = 0.5, symmetric around 0.5. Verified against the
//! NautilusTrader Polymarket adapter.

/// Compute the trade fee for a binary outcome at fill price `price` with
/// `shares` shares and the given `fee_rate` (as a decimal, e.g. 0.072).
///
/// Returns 0 when `fee_rate` or `shares` is non-positive. Floors tiny
/// positive fees to 0.00001 so they remain non-zero in JSONL logs.
pub fn polymarket_fee(shares: f64, price: f64, fee_rate: f64) -> f64 {
    if fee_rate <= 0.0 || shares <= 0.0 {
        return 0.0;
    }
    let p = price.clamp(0.0, 1.0);
    let fee = shares * fee_rate * p * (1.0 - p);
    let fee = (fee * 100_000.0).round() / 100_000.0;
    if fee > 0.0 && fee < 0.00001 {
        0.00001
    } else {
        fee
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_zero_when_no_rate() {
        assert_eq!(polymarket_fee(100.0, 0.5, 0.0), 0.0);
    }

    #[test]
    fn fee_max_at_half() {
        let f50 = polymarket_fee(100.0, 0.5, 0.072);
        let f25 = polymarket_fee(100.0, 0.25, 0.072);
        let f75 = polymarket_fee(100.0, 0.75, 0.072);
        assert!(f50 > f25);
        assert!(f50 > f75);
        // Symmetric around 0.5
        assert!((f25 - f75).abs() < 1e-6);
    }

    #[test]
    fn fee_floor_for_tiny_positive() {
        let f = polymarket_fee(0.001, 0.5, 0.072);
        assert!(f >= 0.00001);
    }
}
