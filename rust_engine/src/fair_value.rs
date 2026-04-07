//! Black-Scholes binary option fair value calculator
//!
//! P(S > K at T) = N(d2)
//! d2 = [ln(S/K) + (r - σ²/2)T] / (σ√T)

/// Standard normal CDF (Abramowitz & Stegun approximation)
pub fn norm_cdf(x: f64) -> f64 {
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x >= 0.0 { 1.0 } else { -1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x / 2.0).exp();
    0.5 * (1.0 + sign * y)
}

/// Compute fair value of a binary "BTC > strike" contract
///
/// Returns probability [0.01, 0.99]
pub fn binary_option_price(
    spot: f64,
    strike: f64,
    days_to_expiry: f64,
    volatility: f64,
) -> f64 {
    if spot <= 0.0 || strike <= 0.0 || days_to_expiry <= 0.0 || volatility <= 0.0 {
        return 0.5;
    }

    let t = days_to_expiry / 365.25;
    let r = 0.05; // risk-free rate
    let sigma = volatility;

    let d2 = ((spot / strike).ln() + (r - 0.5 * sigma * sigma) * t) / (sigma * t.sqrt());
    let price = norm_cdf(d2);

    price.clamp(0.01, 0.99)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atm_option() {
        // ATM: spot = strike, should be ~50%
        let p = binary_option_price(67000.0, 67000.0, 7.0, 0.40);
        assert!(p > 0.45 && p < 0.55, "ATM should be ~50%, got {}", p);
    }

    #[test]
    fn test_deep_itm() {
        // Deep ITM: spot >> strike
        let p = binary_option_price(67000.0, 50000.0, 7.0, 0.40);
        assert!(p > 0.95, "Deep ITM should be >95%, got {}", p);
    }

    #[test]
    fn test_deep_otm() {
        // Deep OTM: spot << strike
        let p = binary_option_price(67000.0, 100000.0, 7.0, 0.40);
        assert!(p < 0.05, "Deep OTM should be <5%, got {}", p);
    }

    #[test]
    fn test_near_expiry() {
        // Near expiry: spot > strike, should be very high
        let p = binary_option_price(67000.0, 60000.0, 0.01, 0.40);
        assert!(p > 0.98, "Near expiry ITM should be >98%, got {}", p);
    }
}
