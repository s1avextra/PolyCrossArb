//! Configuration loader.
//!
//! Reads from environment variables (matches pydantic-settings naming) so the
//! existing `.env` files on the VPS work unchanged.

use std::env;

#[derive(Debug, Clone)]
// Some fields are read by future-phase code paths or surfaced as public
// config knobs (Kelly fraction, alert_required, etc.) even though the
// current pipeline does not branch on them. Silence the dead-field warnings.
#[allow(dead_code)]
pub struct Settings {
    pub poly_api_key: String,
    pub poly_api_secret: String,
    pub poly_api_passphrase: String,
    pub poly_base_url: String,
    pub poly_gamma_url: String,

    pub private_key: String,
    pub polygon_rpc_url: String,

    pub bankroll_usd: f64,
    pub max_total_exposure_usd: f64,
    pub max_position_per_market_usd: f64,
    pub cooldown_seconds: f64,
    pub min_profit_usd: f64,
    pub kelly_fraction: f64,
    pub min_crypto_edge: f64,
    pub max_crypto_position_pct: f64,

    pub candle_zone_early_min_confidence: f64,
    pub candle_zone_early_min_z: f64,
    pub candle_zone_early_min_edge: f64,
    pub candle_zone_primary_min_z: f64,
    pub candle_zone_late_min_confidence: f64,
    pub candle_zone_late_min_z: f64,
    pub candle_zone_late_min_edge: f64,
    pub candle_zone_terminal_min_confidence: f64,
    pub candle_zone_terminal_min_z: f64,
    pub candle_zone_terminal_min_edge: f64,
    pub candle_dead_zone_lo: f64,
    pub candle_dead_zone_hi: f64,
    pub candle_min_price: f64,
    pub candle_max_price: f64,
    pub candle_edge_cap: f64,
    pub candle_skip_dead_zone: bool,
    pub candle_min_ev_buffer: f64,

    pub candle_noise_z_threshold: f64,
    pub candle_position_pct: f64,
    pub candle_vol_high_multiplier: f64,
    pub candle_vol_extreme_multiplier: f64,
    pub candle_cross_asset_enabled: bool,
    pub candle_cross_asset_min_correlation: f64,
    pub candle_cross_asset_confidence_boost: f64,

    pub candle_prefer_maker: bool,
    pub candle_maker_timeout_s: f64,

    pub candle_breaker_min_trades: i64,
    pub candle_breaker_min_win_rate: f64,
    pub candle_breaker_max_drawdown_pct: f64,

    pub kill_switch_path: String,
    pub alert_required: bool,

    pub data_dir: String,
    pub logs_dir: String,
    pub state_db_path: String,
    pub session_log_dir: String,
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_f64(key: &str, default: f64) -> f64 {
    env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn env_i64(key: &str, default: i64) -> i64 {
    env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|s| matches!(s.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}

impl Settings {
    pub fn from_env() -> Self {
        // Try to load .env if it exists (best-effort, no hard dep on dotenv)
        let _ = load_dotenv_best_effort(".env");

        let data_dir = env_str("POLYMOMENTUM_DATA_DIR", "/opt/polymomentum/data");
        let logs_dir = env_str("POLYMOMENTUM_LOGS_DIR", "/opt/polymomentum/logs");

        Self {
            poly_api_key: env_str("POLY_API_KEY", ""),
            poly_api_secret: env_str("POLY_API_SECRET", ""),
            poly_api_passphrase: env_str("POLY_API_PASSPHRASE", ""),
            poly_base_url: env_str("POLY_BASE_URL", "https://clob.polymarket.com"),
            poly_gamma_url: env_str("POLY_GAMMA_URL", "https://gamma-api.polymarket.com"),

            private_key: env_str("PRIVATE_KEY", ""),
            polygon_rpc_url: env_str("POLYGON_RPC_URL", "https://polygon-rpc.com"),

            bankroll_usd: env_f64("BANKROLL_USD", 0.0),
            max_total_exposure_usd: env_f64("MAX_TOTAL_EXPOSURE_USD", 80.0),
            max_position_per_market_usd: env_f64("MAX_POSITION_PER_MARKET_USD", 20.0),
            cooldown_seconds: env_f64("COOLDOWN_SECONDS", 120.0),
            min_profit_usd: env_f64("MIN_PROFIT_USD", 0.10),
            kelly_fraction: env_f64("KELLY_FRACTION", 0.25),
            min_crypto_edge: env_f64("MIN_CRYPTO_EDGE", 0.03),
            max_crypto_position_pct: env_f64("MAX_CRYPTO_POSITION_PCT", 0.10),

            candle_zone_early_min_confidence: env_f64("CANDLE_ZONE_EARLY_MIN_CONFIDENCE", 0.55),
            candle_zone_early_min_z: env_f64("CANDLE_ZONE_EARLY_MIN_Z", 2.0),
            candle_zone_early_min_edge: env_f64("CANDLE_ZONE_EARLY_MIN_EDGE", 0.03),
            candle_zone_primary_min_z: env_f64("CANDLE_ZONE_PRIMARY_MIN_Z", 1.0),
            candle_zone_late_min_confidence: env_f64("CANDLE_ZONE_LATE_MIN_CONFIDENCE", 0.65),
            candle_zone_late_min_z: env_f64("CANDLE_ZONE_LATE_MIN_Z", 0.5),
            candle_zone_late_min_edge: env_f64("CANDLE_ZONE_LATE_MIN_EDGE", 0.08),
            candle_zone_terminal_min_confidence: env_f64("CANDLE_ZONE_TERMINAL_MIN_CONFIDENCE", 0.55),
            candle_zone_terminal_min_z: env_f64("CANDLE_ZONE_TERMINAL_MIN_Z", 0.3),
            candle_zone_terminal_min_edge: env_f64("CANDLE_ZONE_TERMINAL_MIN_EDGE", 0.03),
            candle_dead_zone_lo: env_f64("CANDLE_DEAD_ZONE_LO", 0.80),
            candle_dead_zone_hi: env_f64("CANDLE_DEAD_ZONE_HI", 0.90),
            candle_min_price: env_f64("CANDLE_MIN_PRICE", 0.10),
            candle_max_price: env_f64("CANDLE_MAX_PRICE", 0.90),
            candle_edge_cap: env_f64("CANDLE_EDGE_CAP", 0.25),
            candle_skip_dead_zone: env_bool("CANDLE_SKIP_DEAD_ZONE", true),
            candle_min_ev_buffer: env_f64("CANDLE_MIN_EV_BUFFER", 0.05),

            candle_noise_z_threshold: env_f64("CANDLE_NOISE_Z_THRESHOLD", 0.3),
            candle_position_pct: env_f64("CANDLE_POSITION_PCT", 0.10),
            candle_vol_high_multiplier: env_f64("CANDLE_VOL_HIGH_MULTIPLIER", 1.5),
            candle_vol_extreme_multiplier: env_f64("CANDLE_VOL_EXTREME_MULTIPLIER", 2.0),
            candle_cross_asset_enabled: env_bool("CANDLE_CROSS_ASSET_ENABLED", false),
            candle_cross_asset_min_correlation: env_f64("CANDLE_CROSS_ASSET_MIN_CORRELATION", 0.70),
            candle_cross_asset_confidence_boost: env_f64("CANDLE_CROSS_ASSET_CONFIDENCE_BOOST", 0.10),

            candle_prefer_maker: env_bool("CANDLE_PREFER_MAKER", false),
            candle_maker_timeout_s: env_f64("CANDLE_MAKER_TIMEOUT_S", 3.0),

            candle_breaker_min_trades: env_i64("CANDLE_BREAKER_MIN_TRADES", 20),
            candle_breaker_min_win_rate: env_f64("CANDLE_BREAKER_MIN_WIN_RATE", 0.65),
            candle_breaker_max_drawdown_pct: env_f64("CANDLE_BREAKER_MAX_DRAWDOWN_PCT", 0.30),

            kill_switch_path: env_str("KILL_SWITCH_PATH", "/tmp/polymomentum/KILL"),
            alert_required: env_bool("ALERT_REQUIRED", false),

            state_db_path: env_str(
                "STATE_DB_PATH",
                &format!("{}/candle/state.db", logs_dir),
            ),
            session_log_dir: env_str(
                "SESSION_LOG_DIR",
                &format!("{}/sessions", logs_dir),
            ),

            data_dir,
            logs_dir,
        }
    }
}

fn load_dotenv_best_effort(path: &str) -> std::io::Result<()> {
    let content = std::fs::read_to_string(path)?;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            let k = k.trim();
            let v = v.trim().trim_matches('"').trim_matches('\'');
            if env::var(k).is_err() {
                env::set_var(k, v);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let s = Settings::from_env();
        assert!(s.candle_min_price < s.candle_max_price);
        assert!(s.candle_dead_zone_lo < s.candle_dead_zone_hi);
        assert!(s.kelly_fraction > 0.0 && s.kelly_fraction <= 1.0);
        assert!(s.max_position_per_market_usd > 0.0);
    }
}
