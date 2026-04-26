//! PolyMomentum engine library.
//!
//! Single-binary Rust trading engine for Polymarket Up/Down crypto candle
//! markets. Modules:
//!
//! - `config`            — env-driven settings
//! - `data::{gamma, scanner, ctf, wallet, models}`
//! - `strategy::{momentum, decision}` (+ `fair_value` BS pricer)
//! - `execution::fees`   — Polymarket binary fee formula
//! - `risk::manager`     — SQLite RiskManager (matches the Python state.db schema)
//! - `monitoring::{session, alerter}` — JSONL writer + Slack webhook
//! - `live::pipeline`    — main runtime: cycle loop, paper resolution, oracle verification
//! - `polymarket_ws`, `exchange`, `price_state` — market data plumbing
//! - `clob`, `signing`   — EIP-712-signed CLOB direct order placement (live mode)

pub mod clob;
pub mod config;
pub mod data;
pub mod exchange;
pub mod execution;
pub mod fair_value;
pub mod live;
pub mod monitoring;
pub mod polymarket_ws;
pub mod price_state;
pub mod risk;
pub mod signing;
pub mod strategy;

pub use fair_value::norm_cdf;
