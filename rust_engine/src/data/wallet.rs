//! On-chain wallet balance reader (USDC.e + native USDC + POL).

use anyhow::{Context, Result};
use k256::ecdsa::SigningKey;
use reqwest::Client;
use serde_json::json;
use sha3::{Digest, Keccak256};

pub const USDC_E: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
pub const USDC_NATIVE: &str = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359";
const BALANCE_OF: &str = "0x70a08231";

#[derive(Debug, Clone, Default)]
pub struct WalletBalances {
    pub address: String,
    pub usdc_e: f64,
    pub usdc_native: f64,
    pub total_usdc: f64,
    pub pol: f64,
}

pub fn address_from_private_key(pk_hex: &str) -> Result<String> {
    let pk_clean = pk_hex.trim_start_matches("0x");
    let bytes = hex::decode(pk_clean).context("decode private key")?;
    if bytes.len() != 32 {
        return Err(anyhow::anyhow!("private key must be 32 bytes"));
    }
    let signing_key = SigningKey::from_slice(&bytes).context("invalid private key")?;
    let verifying_key = signing_key.verifying_key();
    let public = verifying_key.to_encoded_point(false);
    let public_bytes = &public.as_bytes()[1..]; // strip 0x04 prefix
    let mut hasher = Keccak256::new();
    hasher.update(public_bytes);
    let hash = hasher.finalize();
    let address = &hash[12..]; // last 20 bytes
    Ok(format!("0x{}", hex::encode(address)))
}

pub struct WalletReader {
    rpc_url: String,
    http: Client,
    address: String,
}

impl WalletReader {
    pub fn new(rpc_url: impl Into<String>, private_key: &str) -> Result<Self> {
        let address = address_from_private_key(private_key)?;
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("client");
        Ok(Self { rpc_url: rpc_url.into(), http, address })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    async fn balance_of(&self, token: &str) -> Result<u128> {
        let addr = self.address.trim_start_matches("0x").to_lowercase();
        let padded = format!("{:0>64}", addr);
        let data = format!("{BALANCE_OF}{padded}");
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": token, "data": data}, "latest"],
            "id": 1,
        });
        let resp = self.http.post(&self.rpc_url).json(&body).send().await?;
        let v: serde_json::Value = resp.json().await?;
        let raw = v.get("result").and_then(|x| x.as_str()).unwrap_or("0x0");
        let trimmed = raw.trim_start_matches("0x");
        let trimmed = if trimmed.is_empty() { "0" } else { trimmed };
        Ok(u128::from_str_radix(trimmed, 16).unwrap_or(0))
    }

    pub async fn fetch_balances(&self) -> Result<WalletBalances> {
        let usdc_e = self.balance_of(USDC_E).await.unwrap_or(0) as f64 / 1e6;
        let usdc_native = self.balance_of(USDC_NATIVE).await.unwrap_or(0) as f64 / 1e6;
        let pol = self.fetch_pol_balance().await.unwrap_or(0.0);
        Ok(WalletBalances {
            address: self.address.clone(),
            usdc_e,
            usdc_native,
            total_usdc: usdc_e + usdc_native,
            pol,
        })
    }

    async fn fetch_pol_balance(&self) -> Result<f64> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_getBalance",
            "params": [self.address, "latest"],
            "id": 2,
        });
        let resp = self.http.post(&self.rpc_url).json(&body).send().await?;
        let v: serde_json::Value = resp.json().await?;
        let raw = v.get("result").and_then(|x| x.as_str()).unwrap_or("0x0");
        let trimmed = raw.trim_start_matches("0x");
        let trimmed = if trimmed.is_empty() { "0" } else { trimmed };
        let wei = u128::from_str_radix(trimmed, 16).unwrap_or(0);
        Ok(wei as f64 / 1e18)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_known_address() {
        // Test vector from EIP-55: well-known test private key.
        let pk = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let addr = address_from_private_key(pk).unwrap();
        assert_eq!(addr.to_lowercase(), "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266");
    }
}
