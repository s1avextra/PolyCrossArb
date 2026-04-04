//! WebSocket price feeds from 4 exchanges + Deribit IV

use crate::PriceState;
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;

pub async fn binance_feed(state: Arc<RwLock<PriceState>>) {
    loop {
        match connect_async("wss://stream.binance.com:9443/ws/btcusdt@ticker").await {
            Ok((ws, _)) => {
                eprintln!("Binance connected");
                let (_, mut read) = ws.split();
                while let Some(Ok(msg)) = read.next().await {
                    if let Ok(text) = msg.into_text() {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(price) = v["c"].as_str().and_then(|s| s.parse::<f64>().ok()) {
                                state.write().await.update("binance", price);
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("Binance error: {}", e),
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

pub async fn bybit_feed(state: Arc<RwLock<PriceState>>) {
    loop {
        match connect_async("wss://stream.bybit.com/v5/public/spot").await {
            Ok((ws, _)) => {
                eprintln!("Bybit connected");
                let (mut write, mut read) = ws.split();
                use futures_util::SinkExt;
                let sub = r#"{"op":"subscribe","args":["tickers.BTCUSDT"]}"#;
                let _ = write.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string().into())).await;

                while let Some(Ok(msg)) = read.next().await {
                    if let Ok(text) = msg.into_text() {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(price) = v["data"]["lastPrice"].as_str().and_then(|s| s.parse::<f64>().ok()) {
                                state.write().await.update("bybit", price);
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("Bybit error: {}", e),
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

pub async fn okx_feed(state: Arc<RwLock<PriceState>>) {
    loop {
        match connect_async("wss://ws.okx.com:8443/ws/v5/public").await {
            Ok((ws, _)) => {
                eprintln!("OKX connected");
                let (mut write, mut read) = ws.split();
                use futures_util::SinkExt;
                let sub = r#"{"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT"}]}"#;
                let _ = write.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string().into())).await;

                while let Some(Ok(msg)) = read.next().await {
                    if let Ok(text) = msg.into_text() {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(data) = v["data"].as_array().and_then(|a| a.first()) {
                                if let Some(price) = data["last"].as_str().and_then(|s| s.parse::<f64>().ok()) {
                                    state.write().await.update("okx", price);
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("OKX error: {}", e),
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

pub async fn mexc_feed(state: Arc<RwLock<PriceState>>) {
    loop {
        match connect_async("wss://wbs.mexc.com/ws").await {
            Ok((ws, _)) => {
                eprintln!("MEXC connected");
                let (mut write, mut read) = ws.split();
                use futures_util::SinkExt;
                let sub = r#"{"method":"SUBSCRIPTION","params":["spot@public.miniTicker.v3.api@BTCUSDT@UTC+8"]}"#;
                let _ = write.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string().into())).await;

                while let Some(Ok(msg)) = read.next().await {
                    if let Ok(text) = msg.into_text() {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(price) = v["d"]["c"].as_str().and_then(|s| s.parse::<f64>().ok()) {
                                state.write().await.update("mexc", price);
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("MEXC error: {}", e),
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

/// Fetch BTC ATM implied volatility from Deribit (free, no auth)
pub async fn fetch_deribit_iv() -> Option<f64> {
    let client = reqwest::Client::new();

    // Get index price
    let idx_resp = client
        .get("https://www.deribit.com/api/v2/public/get_index_price")
        .query(&[("index_name", "btc_usd")])
        .send()
        .await
        .ok()?;
    let idx_data: serde_json::Value = idx_resp.json().await.ok()?;
    let btc_price = idx_data["result"]["index_price"].as_f64()?;

    // Get option summaries
    let resp = client
        .get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency")
        .query(&[("currency", "BTC"), ("kind", "option")])
        .send()
        .await
        .ok()?;
    let data: serde_json::Value = resp.json().await.ok()?;
    let results = data["result"].as_array()?;

    let mut ivs = Vec::new();
    for opt in results {
        let iv = opt["mark_iv"].as_f64().unwrap_or(0.0);
        if iv <= 0.0 { continue; }

        let name = opt["instrument_name"].as_str().unwrap_or("");
        let parts: Vec<&str> = name.split('-').collect();
        if parts.len() < 4 { continue; }

        let strike: f64 = parts[2].parse().unwrap_or(0.0);
        if strike <= 0.0 { continue; }

        let ratio = strike / btc_price;
        if ratio < 0.95 || ratio > 1.05 { continue; }

        ivs.push(iv / 100.0); // Deribit reports as percentage
    }

    if ivs.is_empty() { return None; }
    Some(ivs.iter().sum::<f64>() / ivs.len() as f64)
}
