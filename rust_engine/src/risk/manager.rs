//! Risk manager — bankroll, exposure, position tracking, SQLite persistence.
//!
//! Mirrors `src/polymomentum/risk/manager.py` schema. Compatible with existing
//! state.db so a Python→Rust cutover doesn't lose state.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub market_condition_id: String,
    pub outcome_idx: i64,
    pub side: String, // "long" or "short"
    pub size: f64,
    pub entry_price: f64,
    pub entry_time: f64,
    pub event_id: String,
}

impl Position {
    pub fn notional(&self) -> f64 {
        self.size * self.entry_price
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub timestamp: f64,
    pub market_condition_id: String,
    pub outcome_idx: i64,
    pub side: String,
    pub size: f64,
    pub price: f64,
    pub cost: f64,
    pub event_id: String,
    pub pnl: f64,
    pub paper: bool,
}

#[derive(Debug, Clone)]
pub struct RiskConfig {
    pub initial_bankroll: f64,
    pub exposure_ratio: f64,
    pub max_per_market_ratio: f64,
    pub max_per_market_override: f64,
    pub cooldown_seconds: f64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            initial_bankroll: 0.0,
            exposure_ratio: 0.80,
            max_per_market_ratio: 0.20,
            max_per_market_override: 20.0,
            cooldown_seconds: 120.0,
        }
    }
}

#[derive(Clone)]
pub struct RiskManager {
    db: Arc<Mutex<Connection>>,
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    cfg: RiskConfig,
    positions: std::collections::HashMap<String, Position>,
    last_trade_time: std::collections::HashMap<String, f64>,
    total_pnl: f64,
    total_fees_paid: f64,
}

impl RiskManager {
    pub async fn open(state_db_path: impl AsRef<Path>, cfg: RiskConfig) -> Result<Self> {
        let path: PathBuf = state_db_path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let conn = Connection::open(&path).context("open state db")?;
        conn.pragma_update(None, "journal_mode", "WAL").ok();
        conn.execute_batch(SCHEMA)?;

        let mr = Self {
            db: Arc::new(Mutex::new(conn)),
            inner: Arc::new(Mutex::new(Inner {
                cfg,
                positions: Default::default(),
                last_trade_time: Default::default(),
                total_pnl: 0.0,
                total_fees_paid: 0.0,
            })),
        };
        mr.load_state().await?;
        Ok(mr)
    }

    pub async fn effective_bankroll(&self) -> f64 {
        let i = self.inner.lock().await;
        (i.cfg.initial_bankroll + i.total_pnl).max(0.0)
    }

    pub async fn max_total_exposure(&self) -> f64 {
        let i = self.inner.lock().await;
        (i.cfg.initial_bankroll + i.total_pnl).max(0.0) * i.cfg.exposure_ratio
    }

    pub async fn max_per_market(&self) -> f64 {
        let i = self.inner.lock().await;
        let bk = (i.cfg.initial_bankroll + i.total_pnl).max(0.0);
        (bk * i.cfg.max_per_market_ratio).min(i.cfg.max_per_market_override)
    }

    pub async fn total_exposure(&self) -> f64 {
        let i = self.inner.lock().await;
        i.positions.values().map(|p| p.notional()).sum()
    }

    pub async fn available_capital(&self) -> f64 {
        let i = self.inner.lock().await;
        let bk = (i.cfg.initial_bankroll + i.total_pnl).max(0.0);
        let max = bk * i.cfg.exposure_ratio;
        let exp: f64 = i.positions.values().map(|p| p.notional()).sum();
        (max - exp).max(0.0)
    }

    pub async fn total_pnl(&self) -> f64 {
        self.inner.lock().await.total_pnl
    }

    pub async fn total_fees_paid(&self) -> f64 {
        self.inner.lock().await.total_fees_paid
    }

    pub async fn record_pnl(&self, amount: f64) -> Result<()> {
        {
            let mut i = self.inner.lock().await;
            i.total_pnl += amount;
        }
        self.save_state().await
    }

    pub async fn record_fees(&self, amount: f64) {
        let mut i = self.inner.lock().await;
        i.total_fees_paid += amount;
    }

    pub async fn cooldown_remaining(&self, event_id: &str, now_s: f64) -> f64 {
        let i = self.inner.lock().await;
        let Some(&last) = i.last_trade_time.get(event_id) else {
            return 0.0;
        };
        (i.cfg.cooldown_seconds - (now_s - last)).max(0.0)
    }

    pub async fn record_trade(&self, record: TradeRecord) -> Result<()> {
        let conn = self.db.lock().await;
        conn.execute(
            "INSERT INTO trades (timestamp, market_condition_id, outcome_idx, \
             side, size, price, cost, event_id, pnl, paper) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                record.timestamp,
                record.market_condition_id,
                record.outcome_idx,
                record.side,
                record.size,
                record.price,
                record.cost,
                record.event_id,
                record.pnl,
                record.paper as i64,
            ],
        )?;
        Ok(())
    }

    pub async fn get_meta(&self, key: &str) -> Result<Option<String>> {
        let conn = self.db.lock().await;
        let mut stmt = conn.prepare("SELECT value FROM meta WHERE key=?")?;
        let row: Option<String> = stmt
            .query_row([key], |r| r.get(0))
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                _ => Err(e),
            })?;
        Ok(row)
    }

    pub async fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.db.lock().await;
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            params![key, value],
        )?;
        Ok(())
    }

    pub async fn save_paper_positions(&self, positions: &[(String, Value)]) -> Result<()> {
        let conn = self.db.lock().await;
        conn.execute_batch("BEGIN; DELETE FROM paper_positions;")?;
        {
            let mut stmt = conn.prepare(
                "INSERT INTO paper_positions (contract_id, payload) VALUES (?, ?)",
            )?;
            for (cid, payload) in positions {
                stmt.execute(params![cid, payload.to_string()])?;
            }
        }
        conn.execute_batch("COMMIT;")?;
        Ok(())
    }

    pub async fn load_paper_positions(&self) -> Result<Vec<(String, Value)>> {
        let conn = self.db.lock().await;
        let mut stmt = conn.prepare("SELECT contract_id, payload FROM paper_positions")?;
        let rows = stmt.query_map([], |r| {
            let cid: String = r.get(0)?;
            let payload: String = r.get(1)?;
            Ok((cid, payload))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (cid, payload) = row?;
            if let Ok(v) = serde_json::from_str::<Value>(&payload) {
                out.push((cid, v));
            }
        }
        Ok(out)
    }

    pub async fn save_oracle_pending(&self, entries: &[(String, Value)]) -> Result<()> {
        let conn = self.db.lock().await;
        conn.execute_batch("BEGIN; DELETE FROM oracle_pending;")?;
        {
            let mut stmt = conn.prepare(
                "INSERT INTO oracle_pending (contract_id, payload) VALUES (?, ?)",
            )?;
            for (cid, payload) in entries {
                stmt.execute(params![cid, payload.to_string()])?;
            }
        }
        conn.execute_batch("COMMIT;")?;
        Ok(())
    }

    pub async fn load_oracle_pending(&self) -> Result<Vec<(String, Value)>> {
        let conn = self.db.lock().await;
        let mut stmt = conn.prepare("SELECT contract_id, payload FROM oracle_pending")?;
        let rows = stmt.query_map([], |r| {
            let cid: String = r.get(0)?;
            let payload: String = r.get(1)?;
            Ok((cid, payload))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (cid, payload) = row?;
            if let Ok(v) = serde_json::from_str::<Value>(&payload) {
                out.push((cid, v));
            }
        }
        Ok(out)
    }

    pub async fn save_state(&self) -> Result<()> {
        let inner = self.inner.lock().await;
        let conn = self.db.lock().await;
        conn.execute_batch("BEGIN")?;
        for (k, v) in [
            ("total_pnl", inner.total_pnl),
            ("total_fees_paid", inner.total_fees_paid),
            (
                "saved_at",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0),
            ),
        ] {
            conn.execute(
                "INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
                params![k, serde_json::Value::from(v).to_string()],
            )?;
        }

        conn.execute("DELETE FROM positions", [])?;
        {
            let mut stmt = conn.prepare(
                "INSERT INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            for (key, p) in inner.positions.iter() {
                stmt.execute(params![
                    key,
                    p.market_condition_id,
                    p.outcome_idx,
                    p.side,
                    p.size,
                    p.entry_price,
                    p.entry_time,
                    p.event_id
                ])?;
            }
        }

        conn.execute("DELETE FROM cooldowns", [])?;
        {
            let mut stmt = conn.prepare("INSERT INTO cooldowns VALUES (?, ?)")?;
            for (eid, ts) in inner.last_trade_time.iter() {
                stmt.execute(params![eid, ts])?;
            }
        }

        conn.execute_batch("COMMIT")?;
        Ok(())
    }

    async fn load_state(&self) -> Result<()> {
        let conn = self.db.lock().await;
        let mut inner = self.inner.lock().await;

        if let Ok(rows) = conn
            .prepare("SELECT key, value FROM state")?
            .query_map([], |r| {
                let k: String = r.get(0)?;
                let v: String = r.get(1)?;
                Ok((k, v))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
        {
            for (k, v) in rows {
                let parsed: Value = serde_json::from_str(&v).unwrap_or(Value::Null);
                let f = parsed.as_f64().unwrap_or(0.0);
                match k.as_str() {
                    "total_pnl" => inner.total_pnl = f,
                    "total_fees_paid" => inner.total_fees_paid = f,
                    _ => {}
                }
            }
        }

        let mut stmt = conn.prepare("SELECT * FROM positions")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                Position {
                    market_condition_id: r.get(1)?,
                    outcome_idx: r.get(2)?,
                    side: r.get(3)?,
                    size: r.get(4)?,
                    entry_price: r.get(5)?,
                    entry_time: r.get(6)?,
                    event_id: r.get::<_, Option<String>>(7)?.unwrap_or_default(),
                },
            ))
        })?;
        for row in rows {
            let (k, p) = row?;
            inner.positions.insert(k, p);
        }

        let mut stmt = conn.prepare("SELECT event_id, last_trade_time FROM cooldowns")?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, f64>(1)?))
        })?;
        for row in rows {
            let (eid, ts) = row?;
            inner.last_trade_time.insert(eid, ts);
        }

        if !inner.positions.is_empty() || inner.total_pnl != 0.0 {
            tracing::info!(
                positions = inner.positions.len(),
                total_pnl = inner.total_pnl,
                total_fees = inner.total_fees_paid,
                "state restored"
            );
        }
        Ok(())
    }
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS positions (
    key                 TEXT PRIMARY KEY,
    market_condition_id TEXT NOT NULL,
    outcome_idx         INTEGER NOT NULL,
    side                TEXT NOT NULL,
    size                REAL NOT NULL,
    entry_price         REAL NOT NULL,
    entry_time          REAL NOT NULL,
    event_id            TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS trades (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp           REAL NOT NULL,
    market_condition_id TEXT NOT NULL,
    outcome_idx         INTEGER NOT NULL,
    side                TEXT NOT NULL,
    size                REAL NOT NULL,
    price               REAL NOT NULL,
    cost                REAL NOT NULL,
    event_id            TEXT NOT NULL DEFAULT '',
    pnl                 REAL NOT NULL DEFAULT 0,
    paper               INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS cooldowns (
    event_id        TEXT PRIMARY KEY,
    last_trade_time REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS paper_positions (
    contract_id TEXT PRIMARY KEY,
    payload     TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS oracle_pending (
    contract_id TEXT PRIMARY KEY,
    payload     TEXT NOT NULL
);
";

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn opens_creates_schema() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.db");
        let mr = RiskManager::open(&path, RiskConfig::default()).await.unwrap();
        assert!((mr.total_pnl().await).abs() < 1e-9);
    }

    #[tokio::test]
    async fn round_trip_pnl() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.db");
        let mr = RiskManager::open(&path, RiskConfig {
            initial_bankroll: 100.0,
            ..Default::default()
        }).await.unwrap();
        mr.record_pnl(5.5).await.unwrap();
        assert!((mr.total_pnl().await - 5.5).abs() < 1e-9);
        // Re-open
        drop(mr);
        let mr2 = RiskManager::open(&path, RiskConfig {
            initial_bankroll: 100.0,
            ..Default::default()
        }).await.unwrap();
        assert!((mr2.total_pnl().await - 5.5).abs() < 1e-9);
    }

    #[tokio::test]
    async fn paper_positions_round_trip() {
        let tmp = TempDir::new().unwrap();
        let mr = RiskManager::open(tmp.path().join("s.db"), RiskConfig::default()).await.unwrap();
        let entries = vec![
            ("c1".to_string(), serde_json::json!({"size": 5})),
        ];
        mr.save_paper_positions(&entries).await.unwrap();
        let loaded = mr.load_paper_positions().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, "c1");
        assert_eq!(loaded[0].1["size"].as_i64(), Some(5));
    }
}
