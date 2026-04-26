//! Polymarket Conditional Token Framework (CTF) reader.
//!
//! Reads on-chain market resolution from Polygon.
//!
//! For binary markets:
//!   payoutDenominator(cid) == 0 → not resolved
//!   payoutNumerators(cid, 0) > num[1] → outcome 0 (Up) won
//!   num0 == num1 → tie

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde_json::json;

pub const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const PAYOUT_DENOMINATOR: &str = "0xdd34de67";
const PAYOUT_NUMERATORS: &str = "0x0504c814";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    Up,
    Down,
    Tie,
    NotResolved,
}

impl Resolution {
    pub fn as_str(&self) -> &'static str {
        match self {
            Resolution::Up => "up",
            Resolution::Down => "down",
            Resolution::Tie => "tie",
            Resolution::NotResolved => "pending",
        }
    }
}

pub struct CtfReader {
    rpc_url: String,
    http: Client,
}

impl CtfReader {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("client");
        Self { rpc_url: rpc_url.into(), http }
    }

    async fn eth_call(&self, data: &str) -> Result<u128> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": CTF_ADDRESS, "data": data}, "latest"],
            "id": 1,
        });
        let resp = self.http.post(&self.rpc_url).json(&body).send().await?;
        let json: serde_json::Value = resp.json().await.context("decode json-rpc")?;
        if let Some(err) = json.get("error") {
            return Err(anyhow!("CTF eth_call error: {err}"));
        }
        let result = json.get("result").and_then(|v| v.as_str()).unwrap_or("0x0");
        let trimmed = result.trim_start_matches("0x");
        let trimmed = if trimmed.is_empty() { "0" } else { trimmed };
        // The result fits in u128 for any sensible payout numerator/denominator,
        // but we parse as u128 from hex.
        u128::from_str_radix(trimmed, 16).context("parse hex")
    }

    pub async fn get_resolution(&self, condition_id: &str) -> Result<(Resolution, [u128; 2])> {
        let cid = condition_id.trim_start_matches("0x");
        if cid.len() != 64 {
            return Err(anyhow!(
                "condition_id must be 32 bytes (64 hex chars), got {}",
                cid.len() / 2
            ));
        }

        let denom_call = format!("{PAYOUT_DENOMINATOR}{cid}");
        let denom = self.eth_call(&denom_call).await?;
        if denom == 0 {
            return Ok((Resolution::NotResolved, [0, 0]));
        }

        let num0_call = format!(
            "{PAYOUT_NUMERATORS}{cid}{}",
            "0".repeat(64),
        );
        let num1_call = format!(
            "{PAYOUT_NUMERATORS}{cid}{}",
            format!("{:0>64}", "1"),
        );
        let num0 = self.eth_call(&num0_call).await?;
        let num1 = self.eth_call(&num1_call).await?;
        let res = if num0 == num1 {
            Resolution::Tie
        } else if num0 > num1 {
            Resolution::Up
        } else {
            Resolution::Down
        };
        Ok((res, [num0, num1]))
    }
}
