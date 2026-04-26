//! Black-Scholes binary option fair value calculator
//!
//! P(S > K at T) = N(d2)
//! d2 = [ln(S/K) + (r - σ²/2)T] / (σ√T)

/// Standard normal CDF via Abramowitz & Stegun 7.1.26 erf approximation.
///
/// Uses N(x) = (1 + erf(x / sqrt(2))) / 2 with the A&S polynomial on erf, not
/// on N directly. The previous (buggy) implementation had systematic errors
/// up to ~3.7 pp because it applied the A&S coefficients to x directly
/// instead of x/sqrt(2) and used exp(-x²/2) instead of exp(-x²).
///
/// Max residual error ≤ 1.5e-7 (per A&S 7.1.26 bound).
pub fn norm_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf_abramowitz_stegun(x / std::f64::consts::SQRT_2))
}

/// Abramowitz & Stegun 7.1.26 approximation of erf(x).
/// Max absolute error ≤ 1.5e-7 for all real x.
fn erf_abramowitz_stegun(x: f64) -> f64 {
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x >= 0.0 { 1.0 } else { -1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + p * x);
    let poly = ((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t;
    let y = 1.0 - poly * (-x * x).exp();
    sign * y
}

/// Compute fair value of a binary "spot > strike" contract.
///
/// Returns probability clamped to [0.01, 0.99]. `risk_free_rate` is annualized
/// (e.g. 0.05 for 5%). Returns 0.5 on degenerate inputs.
pub fn binary_option_price_with_rate(
    spot: f64,
    strike: f64,
    days_to_expiry: f64,
    volatility: f64,
    risk_free_rate: f64,
) -> f64 {
    if spot <= 0.0 || strike <= 0.0 || days_to_expiry <= 0.0 || volatility < 0.01 {
        return 0.5;
    }

    let t = days_to_expiry / 365.25;
    let sigma = volatility;
    let r = risk_free_rate;

    let d2 = ((spot / strike).ln() + (r - 0.5 * sigma * sigma) * t) / (sigma * t.sqrt());
    norm_cdf(d2).clamp(0.01, 0.99)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binary_option_price_with_rate_default(
        spot: f64,
        strike: f64,
        days: f64,
        vol: f64,
    ) -> f64 {
        binary_option_price_with_rate(spot, strike, days, vol, 0.05)
    }

    #[test]
    fn test_atm_option() {
        // ATM: spot = strike, should be ~50%
        let p = binary_option_price_with_rate_default(67000.0, 67000.0, 7.0, 0.40);
        assert!(p > 0.45 && p < 0.55, "ATM should be ~50%, got {}", p);
    }

    #[test]
    fn test_deep_itm() {
        // Deep ITM: spot >> strike
        let p = binary_option_price_with_rate_default(67000.0, 50000.0, 7.0, 0.40);
        assert!(p > 0.95, "Deep ITM should be >95%, got {}", p);
    }

    #[test]
    fn test_deep_otm() {
        // Deep OTM: spot << strike
        let p = binary_option_price_with_rate_default(67000.0, 100000.0, 7.0, 0.40);
        assert!(p < 0.05, "Deep OTM should be <5%, got {}", p);
    }

    #[test]
    fn test_near_expiry() {
        // Near expiry: spot > strike, should be very high
        let p = binary_option_price_with_rate_default(67000.0, 60000.0, 0.01, 0.40);
        assert!(p > 0.98, "Near expiry ITM should be >98%, got {}", p);
    }

    #[test]
    fn test_norm_cdf_known_values() {
        // Standard normal CDF values to 1e-6 precision.
        assert!((norm_cdf(0.0) - 0.5).abs() < 1e-6);
        assert!((norm_cdf(1.0) - 0.8413447).abs() < 1e-6);
        assert!((norm_cdf(-1.0) - 0.1586553).abs() < 1e-6);
        assert!((norm_cdf(1.96) - 0.9750021).abs() < 1e-6);
        assert!((norm_cdf(-1.96) - 0.0249979).abs() < 1e-6);
        assert!((norm_cdf(2.0) - 0.9772499).abs() < 1e-6);
    }

    #[test]
    fn test_norm_cdf_symmetry() {
        for x in [0.25_f64, 0.5, 1.0, 1.5, 2.0, 3.0] {
            let diff = (norm_cdf(x) + norm_cdf(-x) - 1.0).abs();
            assert!(diff < 1e-12, "symmetry failed at x={}: diff={}", x, diff);
        }
    }
}
