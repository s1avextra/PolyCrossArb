//! PolyMomentum engine library.
//!
//! Phase 1 layout: foundation modules (config, data, strategy) are exposed here.
//! Live runtime, risk, monitoring, and backtest will land in subsequent phases.

pub mod clob;
pub mod config;
pub mod data;
pub mod debug;
pub mod edge;
pub mod exchange;
pub mod execution;
pub mod fair_value;
pub mod ipc;
pub mod latency;
pub mod live;
pub mod monitoring;
pub mod polymarket_ws;
pub mod price_state;
pub mod risk;
pub mod signing;
pub mod strategy;

pub use fair_value::norm_cdf;
