//! Slack webhook alerter.
//!
//! Posts a single message per alert; non-blocking on failure (we never want
//! a webhook outage to halt the bot).

use std::time::Duration;

use anyhow::Result;
use reqwest::Client;
use serde_json::json;

#[derive(Clone)]
pub struct Alerter {
    webhook: Option<String>,
    http: Client,
}

impl Alerter {
    pub fn new(webhook: Option<String>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("client");
        Self { webhook, http }
    }

    pub fn enabled(&self) -> bool {
        self.webhook.is_some()
    }

    pub async fn send(&self, severity: &str, title: &str, body: &str) -> Result<()> {
        let Some(url) = self.webhook.clone() else {
            return Ok(());
        };
        let icon = match severity {
            "info" => ":information_source:",
            "warning" => ":warning:",
            "critical" => ":rotating_light:",
            _ => ":speech_balloon:",
        };
        let text = format!("{icon} *{title}*\n{body}");
        let resp = self
            .http
            .post(&url)
            .json(&json!({"text": text}))
            .send()
            .await;
        match resp {
            Ok(r) if r.status().is_success() => Ok(()),
            Ok(r) => {
                tracing::warn!(status = %r.status(), "alerter non-2xx");
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = %e, "alerter post failed");
                Ok(())
            }
        }
    }
}
