//! Unix Domain Socket IPC server.
//!
//! Replaces stdin/stdout with a bidirectional UDS for Python↔Rust communication.
//! Protocol: length-prefixed MessagePack frames.
//!
//! Frame format: [4-byte big-endian length] [msgpack payload]
//!
//! Messages Python→Rust:
//!   ContractUpdate  — active candle contracts with MM prices
//!   ConfigUpdate    — strategy parameters
//!   RiskUpdate      — available capital from risk manager
//!   Shutdown        — graceful shutdown
//!
//! Messages Rust→Python:
//!   TradeSignal     — edge detected, order placed or pending
//!   FillReport      — order fill confirmation with latency breakdown
//!   LatencyReport   — periodic latency stats
//!   Heartbeat       — keep-alive

use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

/// Default socket path
pub const DEFAULT_SOCKET_PATH: &str = "/tmp/polymomentum/engine.sock";

// ── Wire messages ──────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum InboundMessage {
    #[serde(rename = "contracts")]
    ContractUpdate { data: Vec<ContractData> },
    #[serde(rename = "config")]
    ConfigUpdate { data: serde_json::Value },
    #[serde(rename = "risk")]
    RiskUpdate { available_capital: f64 },
    #[serde(rename = "shutdown")]
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractData {
    pub contract_id: String,
    pub token_id: String,
    pub up_price: f64,
    pub down_price: f64,
    pub end_time_s: f64,
    pub window_minutes: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum OutboundMessage {
    #[serde(rename = "trade_signal")]
    TradeSignal {
        contract_id: String,
        token_id: String,
        direction: String,
        edge: f64,
        fair_value: f64,
        mm_price: f64,
        size_usd: f64,
    },
    #[serde(rename = "fill_report")]
    FillReport {
        order_id: String,
        latency_us: u64,
        sign_us: u64,
        post_us: u64,
        fill_price: f64,
    },
    #[serde(rename = "latency_report")]
    LatencyReport {
        avg_us: u64,
        p99_us: u64,
        n_orders: usize,
    },
    #[serde(rename = "heartbeat")]
    Heartbeat { ts: f64 },
}

// ── Frame I/O ──────────────────────────────────────────────────────

/// Write a length-prefixed msgpack frame to a stream.
pub async fn write_frame(
    stream: &mut UnixStream,
    msg: &OutboundMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let payload = rmp_serde::to_vec(msg)?;
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;
    Ok(())
}

/// Read a length-prefixed msgpack frame from a stream.
pub async fn read_frame(
    stream: &mut UnixStream,
) -> Result<InboundMessage, Box<dyn std::error::Error + Send + Sync>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 10 * 1024 * 1024 {
        return Err("Frame too large (>10MB)".into());
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    let msg: InboundMessage = rmp_serde::from_slice(&payload)?;
    Ok(msg)
}

// ── Server ─────────────────────────────────────────────────────────

/// UDS server: accepts one client, handles bidirectional I/O with select!.
///
/// On disconnect, loops back to accept the next connection (auto-reconnect).
/// Sends heartbeats every 5 seconds to detect dead connections early.
pub async fn serve_connection(
    socket_path: &str,
    in_tx: mpsc::Sender<InboundMessage>,
    mut out_rx: mpsc::Receiver<OutboundMessage>,
) {
    // Ensure parent directory exists
    if let Some(parent) = Path::new(socket_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::remove_file(socket_path);

    let listener = match UnixListener::bind(socket_path) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("IPC: Failed to bind {}: {}", socket_path, e);
            return;
        }
    };
    eprintln!("IPC: Listening on {}", socket_path);

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                eprintln!("IPC: Client connected");

                let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(5));

                loop {
                    tokio::select! {
                        // Inbound: read from client
                        result = read_frame(&mut stream) => {
                            match result {
                                Ok(msg) => {
                                    let is_shutdown = matches!(msg, InboundMessage::Shutdown);
                                    if in_tx.send(msg).await.is_err() {
                                        break;
                                    }
                                    if is_shutdown {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    eprintln!("IPC: Read error (client disconnected?): {}", e);
                                    break;
                                }
                            }
                        }
                        // Outbound: write to client
                        Some(msg) = out_rx.recv() => {
                            if let Err(e) = write_frame(&mut stream, &msg).await {
                                eprintln!("IPC: Write error: {}", e);
                                break;
                            }
                        }
                        // Heartbeat every 5s
                        _ = heartbeat.tick() => {
                            let ts = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs_f64();
                            let hb = OutboundMessage::Heartbeat { ts };
                            if let Err(e) = write_frame(&mut stream, &hb).await {
                                eprintln!("IPC: Heartbeat write error: {}", e);
                                break;
                            }
                        }
                    }
                }

                eprintln!("IPC: Client disconnected, waiting for reconnect...");
            }
            Err(e) => {
                eprintln!("IPC: Accept error: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}
