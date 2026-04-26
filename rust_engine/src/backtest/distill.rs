//! Distilled candles cache — v1 schema.
//!
//! Cross-bot shared cache format. See
//! `docs/cross_bot_distilled_cache_response.md` and
//! `2026-04-26_distilled_cache_v1_confirmed_from_polyarbitrage.md` in
//! `/opt/shared/cross_bot_notes/` for the negotiated spec.
//!
//! File: `/opt/shared/pmxt_v2_distilled_candles/<hour>.v1.candles.jsonl.gz`
//!
//! Each line is one event tagged by `ev`:
//!   { "ev": "book",  "ts": f64, "mkt": "0x..", "tok": "..", "bb": f64, "ba": f64,
//!     "bids": [[price_str, size_str]...], "asks": [...] }
//!   { "ev": "chg",   "ts": f64, "mkt": "0x..", "tok": "..", "s": "BUY|SELL",
//!     "bb": f64, "ba": f64, "p": price_str, "sz": size_str }
//!   { "ev": "trade", "ts": f64, "mkt": "0x..", "tok": "..", "s": "BUY|SELL",
//!     "p": price_str, "sz": size_str, "tx": "0x.." (optional) }
//!
//! The reader contract is: missing|corrupt|schema-mismatch → caller falls
//! back to the parquet (or to the per-tenant sidecar). Atomic-rename writes,
//! no lockfiles.

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use arrow_array::{
    Array, BooleanArray, Decimal128Array, FixedSizeBinaryArray, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter,
};
use parquet::arrow::ProjectionMask;
use serde::{Deserialize, Serialize};

use crate::backtest::pmxt::{BookSnapshot, L2Event, L2EventBody, L2Level, PriceChange};

/// Wire schema version embedded in the filename.
pub const SCHEMA_VERSION: &str = "v1";
/// Default shared-cache dir on the multi-tenant VPS.
pub const SHARED_CACHE_DIR: &str = "/opt/shared/pmxt_v2_distilled_candles";

/// One JSONL line. Discriminated by `ev`. Fields documented in the module
/// doc above and in the cross-bot notes.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "ev")]
pub enum DistilledEvent {
    #[serde(rename = "book")]
    Book {
        ts: f64,
        mkt: String,
        tok: String,
        bb: f64,
        ba: f64,
        bids: Vec<[String; 2]>,
        asks: Vec<[String; 2]>,
    },
    #[serde(rename = "chg")]
    Change {
        ts: f64,
        mkt: String,
        tok: String,
        s: String,
        bb: f64,
        ba: f64,
        p: String,
        sz: String,
    },
    #[serde(rename = "trade")]
    Trade {
        ts: f64,
        mkt: String,
        tok: String,
        s: String,
        p: String,
        sz: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        tx: Option<String>,
    },
}

pub fn shared_cache_path_for_hour(dir: impl AsRef<Path>, hour: DateTime<Utc>) -> PathBuf {
    dir.as_ref().join(format!(
        "{}.{}.candles.jsonl.gz",
        hour.format("%Y-%m-%dT%H"),
        SCHEMA_VERSION,
    ))
}

/// Distill one parquet hour into the v1 JSONL.gz format. Pre-filters to the
/// given candle cids; writes book + price_change + last_trade_price events.
/// Atomic rename: writes to `<out>.tmp.<pid>` then renames.
pub fn distill_parquet_to_jsonl(
    parquet_path: &Path,
    candle_cids: &HashSet<String>,
    out_path: &Path,
) -> Result<DistillStats> {
    let file = File::open(parquet_path)
        .with_context(|| format!("open parquet {}", parquet_path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("parquet builder {}", parquet_path.display()))?;

    // Project the columns we emit. Add transaction_hash for trades.
    let needed = [
        "timestamp",
        "timestamp_received",
        "market",
        "event_type",
        "asset_id",
        "bids",
        "asks",
        "price",
        "size",
        "side",
        "best_bid",
        "best_ask",
        "transaction_hash",
    ];
    let leaf_indices: Vec<usize> = needed
        .iter()
        .filter_map(|name| {
            builder
                .parquet_schema()
                .columns()
                .iter()
                .position(|c| c.name() == *name)
        })
        .collect();
    let market_idx = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|c| c.name() == "market")
        .context("parquet schema missing `market` column")?;
    let mask = ProjectionMask::leaves(builder.parquet_schema(), leaf_indices);
    let predicate_mask = ProjectionMask::leaves(builder.parquet_schema(), vec![market_idx]);
    let mut builder = builder.with_projection(mask);

    let cid_set: HashSet<Vec<u8>> = candle_cids.iter().map(|s| s.as_bytes().to_vec()).collect();
    let predicate = ArrowPredicateFn::new(predicate_mask, move |batch| {
        let col = batch.column(0);
        let n = batch.num_rows();
        let mut keep = Vec::with_capacity(n);
        if let Some(arr) = col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            for i in 0..n {
                if arr.is_null(i) {
                    keep.push(false);
                } else {
                    keep.push(cid_set.contains(arr.value(i)));
                }
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            for i in 0..n {
                if arr.is_null(i) {
                    keep.push(false);
                } else {
                    keep.push(cid_set.contains(arr.value(i).as_bytes()));
                }
            }
        } else {
            for _ in 0..n {
                keep.push(false);
            }
        }
        Ok(BooleanArray::from(keep))
    });
    builder = builder.with_row_filter(RowFilter::new(vec![Box::new(predicate) as Box<dyn ArrowPredicate>]));

    let reader = builder.build().context("build parquet reader")?;

    // Atomic rename: write to a tmp first.
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let tmp = out_path.with_extension(format!(
        "jsonl.gz.tmp.{}",
        std::process::id(),
    ));
    let f = File::create(&tmp).with_context(|| format!("create tmp {}", tmp.display()))?;
    let buf = BufWriter::new(f);
    let mut gz = GzEncoder::new(buf, Compression::fast());

    let mut stats = DistillStats::default();
    for batch_result in reader {
        let batch = batch_result.context("read batch")?;
        emit_batch(&batch, &mut gz, &mut stats)?;
    }
    gz.try_finish().context("finish gz writer")?;
    let inner = gz.finish().context("flush gz buffer")?;
    inner.into_inner().context("flush buf writer")?.sync_all().ok();
    std::fs::rename(&tmp, out_path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), out_path.display()))?;

    Ok(stats)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DistillStats {
    pub book_events: u64,
    pub change_events: u64,
    pub trade_events: u64,
    pub bytes_written: u64,
}

impl DistillStats {
    pub fn total(&self) -> u64 {
        self.book_events + self.change_events + self.trade_events
    }
}

fn emit_batch<W: Write>(
    batch: &RecordBatch,
    gz: &mut GzEncoder<W>,
    stats: &mut DistillStats,
) -> Result<()> {
    let n = batch.num_rows();
    let market_col = batch.column_by_name("market").context("missing market")?;
    let event_type_col = batch
        .column_by_name("event_type")
        .context("missing event_type")?
        .as_any()
        .downcast_ref::<StringArray>()
        .context("event_type not StringArray")?;
    let asset_id_col = batch
        .column_by_name("asset_id")
        .context("missing asset_id")?
        .as_any()
        .downcast_ref::<StringArray>()
        .context("asset_id not StringArray")?;
    let timestamp_col = batch
        .column_by_name("timestamp")
        .context("missing timestamp")?
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .context("timestamp not TimestampMillisecondArray")?;
    let timestamp_received_col = batch
        .column_by_name("timestamp_received")
        .and_then(|c| c.as_any().downcast_ref::<TimestampMillisecondArray>());
    let bids_col = batch
        .column_by_name("bids")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let asks_col = batch
        .column_by_name("asks")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let side_col = batch
        .column_by_name("side")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let price_col = batch
        .column_by_name("price")
        .and_then(|c| c.as_any().downcast_ref::<Decimal128Array>());
    let size_col = batch
        .column_by_name("size")
        .and_then(|c| c.as_any().downcast_ref::<Decimal128Array>());
    let best_bid_col = batch
        .column_by_name("best_bid")
        .and_then(|c| c.as_any().downcast_ref::<Decimal128Array>());
    let best_ask_col = batch
        .column_by_name("best_ask")
        .and_then(|c| c.as_any().downcast_ref::<Decimal128Array>());
    let tx_hash_col = batch
        .column_by_name("transaction_hash")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());

    let market_fixed = market_col.as_any().downcast_ref::<FixedSizeBinaryArray>();
    let market_string = market_col.as_any().downcast_ref::<StringArray>();

    for i in 0..n {
        let market = if let Some(arr) = market_fixed {
            std::str::from_utf8(arr.value(i)).unwrap_or("").to_string()
        } else if let Some(arr) = market_string {
            arr.value(i).to_string()
        } else {
            continue;
        };
        let event_type = event_type_col.value(i);
        let asset_id = asset_id_col.value(i).to_string();
        let ts_ms = if !timestamp_col.is_null(i) {
            timestamp_col.value(i)
        } else if let Some(rcv) = timestamp_received_col {
            if rcv.is_null(i) {
                continue;
            }
            rcv.value(i)
        } else {
            continue;
        };
        let ts = ts_ms as f64 / 1000.0;
        let bb = best_bid_col.map(|c| decimal_to_f64(c, i)).unwrap_or(0.0);
        let ba = best_ask_col.map(|c| decimal_to_f64(c, i)).unwrap_or(0.0);

        let evt: Option<DistilledEvent> = match event_type {
            "book" => {
                let bids_str = bids_col
                    .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                    .unwrap_or("[]");
                let asks_str = asks_col
                    .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                    .unwrap_or("[]");
                let bids = parse_levels_strs(bids_str);
                let asks = parse_levels_strs(asks_str);
                stats.book_events += 1;
                Some(DistilledEvent::Book {
                    ts,
                    mkt: market,
                    tok: asset_id,
                    bb,
                    ba,
                    bids,
                    asks,
                })
            }
            "price_change" => {
                let s = side_col
                    .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                    .unwrap_or("")
                    .to_string();
                let p = decimal_to_string(price_col, i);
                let sz = decimal_to_string(size_col, i);
                stats.change_events += 1;
                Some(DistilledEvent::Change {
                    ts,
                    mkt: market,
                    tok: asset_id,
                    s,
                    bb,
                    ba,
                    p,
                    sz,
                })
            }
            "last_trade_price" => {
                let s = side_col
                    .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                    .unwrap_or("")
                    .to_string();
                let p = decimal_to_string(price_col, i);
                let sz = decimal_to_string(size_col, i);
                let tx = tx_hash_col
                    .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i).to_string()) });
                stats.trade_events += 1;
                Some(DistilledEvent::Trade {
                    ts,
                    mkt: market,
                    tok: asset_id,
                    s,
                    p,
                    sz,
                    tx,
                })
            }
            _ => None,
        };

        if let Some(e) = evt {
            let line = serde_json::to_string(&e).context("serialize jsonl line")?;
            gz.write_all(line.as_bytes())?;
            gz.write_all(b"\n")?;
            stats.bytes_written += line.len() as u64 + 1;
        }
    }
    Ok(())
}

fn decimal_to_f64(arr: &Decimal128Array, i: usize) -> f64 {
    if arr.is_null(i) {
        return 0.0;
    }
    let raw = arr.value(i);
    let scale = arr.scale();
    let divisor = 10f64.powi(scale as i32);
    raw as f64 / divisor
}

fn decimal_to_string(arr: Option<&Decimal128Array>, i: usize) -> String {
    let Some(arr) = arr else { return String::new() };
    if arr.is_null(i) {
        return String::new();
    }
    let raw = arr.value(i);
    let scale = arr.scale();
    let divisor = 10i128.pow(scale.max(0) as u32);
    let int_part = raw / divisor;
    let frac_part = (raw % divisor).abs();
    if scale <= 0 {
        format!("{}", int_part)
    } else {
        format!(
            "{}.{:0width$}",
            int_part,
            frac_part,
            width = scale as usize,
        )
    }
}

fn parse_levels_strs(s: &str) -> Vec<[String; 2]> {
    let s = s.trim();
    if s.is_empty() || s == "null" {
        return Vec::new();
    }
    let v: serde_json::Value = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let Some(arr) = v.as_array() else { return Vec::new() };
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        match entry {
            serde_json::Value::Array(pair) if pair.len() >= 2 => {
                let p = json_value_to_string(&pair[0]);
                let s = json_value_to_string(&pair[1]);
                if !p.is_empty() && !s.is_empty() {
                    out.push([p, s]);
                }
            }
            serde_json::Value::Object(obj) => {
                let p = obj.get("price").map(json_value_to_string).unwrap_or_default();
                let s = obj.get("size").map(json_value_to_string).unwrap_or_default();
                if !p.is_empty() && !s.is_empty() {
                    out.push([p, s]);
                }
            }
            _ => {}
        }
    }
    out
}

fn json_value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => String::new(),
    }
}

/// Read a v1 JSONL.gz distilled file and emit `L2Event`s for the engine.
/// Trade events are skipped — the engine only consumes book + chg.
/// Returns `condition_ids` that contributed events, useful for sanity
/// checking the universe matches expectations.
pub fn read_distilled(path: &Path) -> Result<(Vec<L2Event>, HashSet<String>)> {
    use std::io::BufRead;
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let buf = BufReader::new(f);
    let gz = GzDecoder::new(buf);
    let reader = BufReader::new(gz);

    let mut events = Vec::new();
    let mut cids = HashSet::new();
    for line_res in reader.lines() {
        let line = line_res.context("read jsonl line")?;
        if line.trim().is_empty() {
            continue;
        }
        let evt: DistilledEvent = match serde_json::from_str(&line) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(error = %e, "skipping malformed distilled line");
                continue;
            }
        };
        match evt {
            DistilledEvent::Book { ts, mkt, tok, bb, ba, bids, asks } => {
                cids.insert(mkt.clone());
                let bids = bids
                    .into_iter()
                    .filter_map(|[p, s]| {
                        let p = p.parse::<f64>().ok()?;
                        let s = s.parse::<f64>().ok()?;
                        if p > 0.0 { Some(L2Level { price: p, size: s }) } else { None }
                    })
                    .collect::<Vec<_>>();
                let asks = asks
                    .into_iter()
                    .filter_map(|[p, s]| {
                        let p = p.parse::<f64>().ok()?;
                        let s = s.parse::<f64>().ok()?;
                        if p > 0.0 { Some(L2Level { price: p, size: s }) } else { None }
                    })
                    .collect::<Vec<_>>();
                let snap = BookSnapshot {
                    market_id: mkt.clone(),
                    token_id: tok,
                    best_bid: if bb > 0.0 { bb } else { bids.first().map(|l| l.price).unwrap_or(0.0) },
                    best_ask: if ba > 0.0 { ba } else { asks.first().map(|l| l.price).unwrap_or(0.0) },
                    timestamp_s: ts,
                    bids,
                    asks,
                };
                events.push(L2Event {
                    timestamp_s: ts,
                    market_id: mkt,
                    body: L2EventBody::BookSnapshot(snap),
                });
            }
            DistilledEvent::Change { ts, mkt, tok, s, bb, ba, p, sz } => {
                cids.insert(mkt.clone());
                let chg_price = p.parse::<f64>().unwrap_or(0.0);
                let chg_size = sz.parse::<f64>().unwrap_or(0.0);
                let chg = PriceChange {
                    market_id: mkt.clone(),
                    token_id: tok,
                    side: s.clone(),
                    best_bid: bb,
                    best_ask: ba,
                    timestamp_s: ts,
                    change_price: chg_price,
                    change_size: chg_size,
                    change_side: s,
                };
                events.push(L2Event {
                    timestamp_s: ts,
                    market_id: mkt,
                    body: L2EventBody::PriceChange(chg),
                });
            }
            DistilledEvent::Trade { .. } => {
                // Engine doesn't consume trades yet; future fill calibration
                // can re-read the distilled file directly.
            }
        }
    }
    events.sort_by(|a, b| a.timestamp_s.partial_cmp(&b.timestamp_s).unwrap_or(std::cmp::Ordering::Equal));
    Ok((events, cids))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_test_jsonl_gz(path: &Path, events: &[DistilledEvent]) {
        let f = File::create(path).unwrap();
        let buf = BufWriter::new(f);
        let mut gz = GzEncoder::new(buf, Compression::fast());
        for e in events {
            let line = serde_json::to_string(e).unwrap();
            gz.write_all(line.as_bytes()).unwrap();
            gz.write_all(b"\n").unwrap();
        }
        gz.try_finish().unwrap();
        gz.finish().unwrap().into_inner().unwrap().sync_all().ok();
    }

    #[test]
    fn round_trip_book_event() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.v1.candles.jsonl.gz");
        let events = vec![DistilledEvent::Book {
            ts: 1745683200.123,
            mkt: "0xabc".into(),
            tok: "12345".into(),
            bb: 0.50,
            ba: 0.52,
            bids: vec![["0.50".into(), "100.0".into()]],
            asks: vec![["0.52".into(), "50.0".into()]],
        }];
        write_test_jsonl_gz(&path, &events);
        let (loaded, cids) = read_distilled(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(cids.len(), 1);
        assert!(cids.contains("0xabc"));
        let L2EventBody::BookSnapshot(s) = &loaded[0].body else { panic!() };
        assert_eq!(s.bids.len(), 1);
        assert!((s.best_ask - 0.52).abs() < 1e-9);
    }

    #[test]
    fn trade_events_are_skipped_on_read() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.v1.candles.jsonl.gz");
        let events = vec![
            DistilledEvent::Book {
                ts: 1.0,
                mkt: "0xabc".into(),
                tok: "1".into(),
                bb: 0.50,
                ba: 0.52,
                bids: vec![],
                asks: vec![],
            },
            DistilledEvent::Trade {
                ts: 2.0,
                mkt: "0xabc".into(),
                tok: "1".into(),
                s: "BUY".into(),
                p: "0.51".into(),
                sz: "10.0".into(),
                tx: Some("0xtx".into()),
            },
        ];
        write_test_jsonl_gz(&path, &events);
        let (loaded, _) = read_distilled(&path).unwrap();
        assert_eq!(loaded.len(), 1); // trade dropped
    }

    #[test]
    fn change_events_round_trip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.v1.candles.jsonl.gz");
        let events = vec![DistilledEvent::Change {
            ts: 1.0,
            mkt: "0xabc".into(),
            tok: "1".into(),
            s: "BUY".into(),
            bb: 0.51,
            ba: 0.52,
            p: "0.51".into(),
            sz: "150.0".into(),
        }];
        write_test_jsonl_gz(&path, &events);
        let (loaded, _) = read_distilled(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        let L2EventBody::PriceChange(c) = &loaded[0].body else { panic!() };
        assert_eq!(c.side, "BUY");
        assert!((c.change_price - 0.51).abs() < 1e-9);
        assert!((c.change_size - 150.0).abs() < 1e-9);
    }

    #[test]
    fn shared_path_format() {
        let h = DateTime::parse_from_rfc3339("2026-04-23T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let p = shared_cache_path_for_hour("/opt/shared/pmxt_v2_distilled_candles", h);
        assert_eq!(
            p.to_str().unwrap(),
            "/opt/shared/pmxt_v2_distilled_candles/2026-04-23T01.v1.candles.jsonl.gz"
        );
    }

    #[test]
    fn decimal_string_preserves_scale() {
        // Build a tiny Decimal128Array with scale=4, value=5000 → "0.5000"
        use arrow_array::builder::Decimal128Builder;
        let mut b = Decimal128Builder::new().with_data_type(arrow_schema::DataType::Decimal128(9, 4));
        b.append_value(5000);
        b.append_value(-12500);
        b.append_null();
        let arr = b.finish();
        assert_eq!(decimal_to_string(Some(&arr), 0), "0.5000");
        assert_eq!(decimal_to_string(Some(&arr), 1), "-1.2500");
        assert_eq!(decimal_to_string(Some(&arr), 2), "");
    }
}
