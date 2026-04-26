//! PMXT v2 historical L2 archive loader.
//!
//! Reads `https://r2v2.pmxt.dev/polymarket_orderbook_YYYY-MM-DDTHH.parquet`
//! files. Each file is one UTC hour of Polymarket order book events.
//!
//! Schema (from archive.pmxt.dev/docs/v2-data-overview):
//!
//! ```text
//! timestamp_received  timestamp[ms, UTC]    delta-encoded
//! timestamp           timestamp[ms, UTC]
//! market              fixed_size_binary[66] dict — "0x" + 64 hex
//! event_type          string                book | price_change | last_trade_price | tick_size_change
//! asset_id            string
//! bids                string nullable       JSON `[["px","sz"],...]`
//! asks                string nullable
//! price               decimal(9,4) nullable
//! size                decimal(18,6) nullable
//! side                string nullable       BUY | SELL
//! best_bid            decimal(9,4) nullable
//! best_ask            decimal(9,4) nullable
//! ```
//!
//! The loader returns `L2Event { body: BookSnapshot | PriceChange }`. Trade
//! prints (`last_trade_price`) and tick_size_change events are dropped — they
//! aren't used by the backtest engine today.

use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use arrow_array::{
    Array, Decimal128Array, FixedSizeBinaryArray, RecordBatch, StringArray, TimestampMillisecondArray,
};
use chrono::{DateTime, Utc};
use arrow_array::BooleanArray;
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
use parquet::arrow::ProjectionMask;
use reqwest::Client;
use serde::{Deserialize, Serialize};

const PMXT_V2_BASE_URL: &str = "https://r2v2.pmxt.dev";
pub const DEFAULT_CACHE_DIR: &str = "data/pmxt_v2_cache";
/// Multi-tenant cache shared with the peer bot polyarbitrage on the VPS.
pub const SHARED_CACHE_DIR: &str = "/opt/shared/pmxt_v2_cache";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct L2Level {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub market_id: String,
    pub token_id: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub timestamp_s: f64,
    pub bids: Vec<L2Level>,
    pub asks: Vec<L2Level>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PriceChange {
    pub market_id: String,
    pub token_id: String,
    pub side: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub timestamp_s: f64,
    pub change_price: f64,
    pub change_size: f64,
    pub change_side: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum L2EventBody {
    BookSnapshot(BookSnapshot),
    PriceChange(PriceChange),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2Event {
    pub timestamp_s: f64,
    pub market_id: String,
    pub body: L2EventBody,
}

pub struct PMXTv2Loader {
    cache_dir: PathBuf,
    http: Client,
}

impl PMXTv2Loader {
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        let cache_dir = cache_dir.into();
        std::fs::create_dir_all(&cache_dir).ok();
        Self {
            cache_dir,
            // PMXT v2 hour files run 100–500 MB; only the connect step has a
            // bounded timeout so a slow link can finish a long download.
            http: Client::builder()
                .connect_timeout(Duration::from_secs(20))
                .build()
                .expect("client"),
        }
    }

    /// Pick the cache dir from env (`PMXT_V2_CACHE_DIR`), else the shared
    /// multi-tenant dir if it exists, else the project-local fallback.
    pub fn from_env() -> Self {
        let dir = std::env::var("PMXT_V2_CACHE_DIR")
            .unwrap_or_else(|_| {
                if Path::new(SHARED_CACHE_DIR).exists() {
                    SHARED_CACHE_DIR.into()
                } else {
                    DEFAULT_CACHE_DIR.into()
                }
            });
        Self::new(dir)
    }

    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    pub fn cache_path_for_hour(&self, hour: DateTime<Utc>) -> PathBuf {
        self.cache_dir
            .join(format!("polymarket_orderbook_{}.parquet", hour.format("%Y-%m-%dT%H")))
    }

    pub fn is_cached(&self, hour: DateTime<Utc>) -> bool {
        let p = self.cache_path_for_hour(hour);
        p.exists() && p.metadata().map(|m| m.len() > 0).unwrap_or(false)
    }

    pub fn url_for_hour(hour: DateTime<Utc>) -> String {
        format!(
            "{}/polymarket_orderbook_{}.parquet",
            PMXT_V2_BASE_URL,
            hour.format("%Y-%m-%dT%H"),
        )
    }

    /// Download a single hour's parquet to the cache directory.
    pub async fn download_hour(&self, hour: DateTime<Utc>, force: bool) -> Result<PathBuf> {
        let path = self.cache_path_for_hour(hour);
        if !force && self.is_cached(hour) {
            return Ok(path);
        }
        let url = Self::url_for_hour(hour);
        tracing::info!(%url, ?path, "downloading PMXT v2 hour");
        let tmp = path.with_extension("parquet.tmp");
        let mut resp = self.http.get(&url).send().await.context("send GET")?;
        if !resp.status().is_success() {
            anyhow::bail!("PMXT v2 returned HTTP {} for {}", resp.status(), url);
        }
        let mut f = std::fs::File::create(&tmp).context("create tmp file")?;
        use std::io::Write;
        while let Some(chunk) = resp.chunk().await.context("read chunk")? {
            f.write_all(&chunk).context("write chunk")?;
        }
        drop(f);
        std::fs::rename(&tmp, &path).context("rename tmp to final")?;
        Ok(path)
    }

    /// Read one hour's events from cache (the file must already be cached).
    /// Pass `condition_ids = None` for "all markets in the file".
    pub fn load_cached_hour(
        &self,
        hour: DateTime<Utc>,
        condition_ids: Option<&HashSet<String>>,
    ) -> Result<Vec<L2Event>> {
        let path = self.cache_path_for_hour(hour);
        if !path.exists() {
            anyhow::bail!("PMXT v2 hour {} not cached at {}", hour, path.display());
        }
        read_parquet(&path, condition_ids)
    }

    /// Convenience: ensure the file is downloaded, then read it.
    pub async fn load_hour(
        &self,
        hour: DateTime<Utc>,
        condition_ids: Option<&HashSet<String>>,
    ) -> Result<Vec<L2Event>> {
        self.download_hour(hour, false).await?;
        self.load_cached_hour(hour, condition_ids)
    }

    /// Sidecar path for a `(hour, cid_set)` event cache. Filenames are
    /// distinct from the parquet (different suffix) so we never touch the
    /// upstream archive — important on the multi-tenant VPS where
    /// polyarbitrage shares the cache directory.
    fn sidecar_path(&self, hour: DateTime<Utc>, cid_hash: u64) -> PathBuf {
        self.cache_dir.join(format!(
            "polymarket_orderbook_{}.{:016x}.events.bin.gz",
            hour.format("%Y-%m-%dT%H"),
            cid_hash,
        ))
    }

    /// Load events for the given hour, using a per-(hour, cid_set) sidecar
    /// cache if one exists. First call decodes the parquet (slow) then
    /// writes a compact gzipped bincode of the filtered events. Subsequent
    /// calls with the same cid_set deserialize the sidecar (~10× faster).
    pub fn load_with_sidecar(
        &self,
        hour: DateTime<Utc>,
        condition_ids: &HashSet<String>,
    ) -> Result<Vec<L2Event>> {
        let mut sorted: Vec<&String> = condition_ids.iter().collect();
        sorted.sort();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hasher::write_usize(&mut hasher, sorted.len());
        for s in &sorted {
            std::hash::Hash::hash(s.as_str(), &mut hasher);
        }
        let cid_hash = std::hash::Hasher::finish(&hasher);
        let sidecar = self.sidecar_path(hour, cid_hash);

        if sidecar.exists() {
            if let Ok(events) = read_sidecar(&sidecar) {
                tracing::debug!(
                    path = %sidecar.display(),
                    events = events.len(),
                    "loaded events from sidecar cache"
                );
                return Ok(events);
            }
            // Corrupt/incompatible sidecar — fall through to a fresh decode.
            tracing::warn!(?sidecar, "sidecar unreadable; re-decoding parquet");
        }

        let events = self.load_cached_hour(hour, Some(condition_ids))?;
        if let Err(e) = write_sidecar(&sidecar, &events) {
            tracing::warn!(error = %e, ?sidecar, "sidecar write failed");
        }
        Ok(events)
    }

    /// Scan a cached hour and return the unique `condition_id`s that have
    /// any events in it. Useful for discovering the harness universe
    /// directly from the historical archive.
    pub fn distinct_condition_ids(&self, hour: DateTime<Utc>) -> Result<HashSet<String>> {
        let path = self.cache_path_for_hour(hour);
        if !path.exists() {
            anyhow::bail!("PMXT v2 hour {} not cached at {}", hour, path.display());
        }
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let mut out = HashSet::new();
        for batch in reader {
            let batch = batch?;
            let Some(col) = batch.column_by_name("market") else { continue };
            if let Some(arr) = col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        if let Ok(s) = std::str::from_utf8(arr.value(i)) {
                            out.insert(s.to_string());
                        }
                    }
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        out.insert(arr.value(i).to_string());
                    }
                }
            }
        }
        Ok(out)
    }
}

fn read_parquet(path: &Path, condition_ids: Option<&HashSet<String>>) -> Result<Vec<L2Event>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("parquet builder {}", path.display()))?;

    // Project only the columns the decoder reads. Drops fee_rate_bps,
    // old_tick_size, new_tick_size, transaction_hash.
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

    // Push the condition_ids filter down so the parquet reader skips full
    // rows (and their JSON bid/ask columns) when the market doesn't match.
    // Going from "decode 39k cids → filter to 70 in memory" to "skip
    // 99.8% of rows before they ever materialize" turns a 6-second
    // decode into ~100 ms. The original parquet file is read-only —
    // never mutated — so the shared multi-tenant cache stays untouched.
    if let Some(filter) = condition_ids {
        let cid_set: HashSet<Vec<u8>> = filter.iter().map(|s| s.as_bytes().to_vec()).collect();
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
    }

    let reader = builder.build().context("build parquet reader")?;

    let mut events: Vec<L2Event> = Vec::new();
    for batch_result in reader {
        let batch: RecordBatch = batch_result.context("read batch")?;
        decode_batch(&batch, condition_ids, &mut events)?;
    }
    events.sort_by(|a, b| a.timestamp_s.partial_cmp(&b.timestamp_s).unwrap_or(std::cmp::Ordering::Equal));
    Ok(events)
}

fn decode_batch(
    batch: &RecordBatch,
    condition_ids: Option<&HashSet<String>>,
    out: &mut Vec<L2Event>,
) -> Result<()> {
    let n = batch.num_rows();
    let market_col = batch.column_by_name("market")
        .context("missing column `market`")?;
    let event_type_col = batch.column_by_name("event_type")
        .context("missing column `event_type`")?
        .as_any()
        .downcast_ref::<StringArray>()
        .context("`event_type` not a StringArray")?;
    let asset_id_col = batch.column_by_name("asset_id")
        .context("missing column `asset_id`")?
        .as_any()
        .downcast_ref::<StringArray>()
        .context("`asset_id` not a StringArray")?;
    let timestamp_col = batch.column_by_name("timestamp")
        .context("missing column `timestamp`")?
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .context("`timestamp` not TimestampMillisecondArray")?;
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

    let market_fixed = market_col.as_any().downcast_ref::<FixedSizeBinaryArray>();
    let market_string = market_col.as_any().downcast_ref::<StringArray>();

    for i in 0..n {
        let market_id = if let Some(arr) = market_fixed {
            std::str::from_utf8(arr.value(i)).unwrap_or("").to_string()
        } else if let Some(arr) = market_string {
            arr.value(i).to_string()
        } else {
            continue;
        };
        if let Some(filter) = condition_ids {
            if !filter.contains(&market_id) {
                continue;
            }
        }

        let event_type = event_type_col.value(i);
        if event_type != "book" && event_type != "price_change" {
            continue;
        }
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
        let ts_s = ts_ms as f64 / 1000.0;

        let best_bid = best_bid_col
            .map(|c| decimal_to_f64(c, i))
            .unwrap_or(0.0);
        let best_ask = best_ask_col
            .map(|c| decimal_to_f64(c, i))
            .unwrap_or(0.0);

        match event_type {
            "book" => {
                let bids_str = bids_col.and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) }).unwrap_or("[]");
                let asks_str = asks_col.and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) }).unwrap_or("[]");
                let mut bids = parse_levels_json(bids_str);
                let mut asks = parse_levels_json(asks_str);
                bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
                asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));
                let snap = BookSnapshot {
                    market_id: market_id.clone(),
                    token_id: asset_id,
                    best_bid: if best_bid > 0.0 { best_bid } else { bids.first().map(|l| l.price).unwrap_or(0.0) },
                    best_ask: if best_ask > 0.0 { best_ask } else { asks.first().map(|l| l.price).unwrap_or(0.0) },
                    timestamp_s: ts_s,
                    bids,
                    asks,
                };
                out.push(L2Event {
                    timestamp_s: ts_s,
                    market_id,
                    body: L2EventBody::BookSnapshot(snap),
                });
            }
            "price_change" => {
                let side = side_col
                    .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                    .unwrap_or("")
                    .to_string();
                let chg_price = price_col.map(|c| decimal_to_f64(c, i)).unwrap_or(0.0);
                let chg_size = size_col.map(|c| decimal_to_f64(c, i)).unwrap_or(0.0);
                let chg = PriceChange {
                    market_id: market_id.clone(),
                    token_id: asset_id,
                    side: side.clone(),
                    best_bid,
                    best_ask,
                    timestamp_s: ts_s,
                    change_price: chg_price,
                    change_size: chg_size,
                    change_side: side,
                };
                out.push(L2Event {
                    timestamp_s: ts_s,
                    market_id,
                    body: L2EventBody::PriceChange(chg),
                });
            }
            _ => {}
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

fn parse_levels_json(s: &str) -> Vec<L2Level> {
    let s = s.trim();
    if s.is_empty() || s == "null" {
        return Vec::new();
    }
    // PMXT v2 stores `[["price","size"], ...]` as JSON strings.
    let v: serde_json::Value = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let Some(arr) = v.as_array() else { return Vec::new() };
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        let parsed = match entry {
            serde_json::Value::Array(pair) if pair.len() >= 2 => {
                let p = parse_num(&pair[0]);
                let s = parse_num(&pair[1]);
                match (p, s) {
                    (Some(p), Some(s)) if p > 0.0 => Some(L2Level { price: p, size: s }),
                    _ => None,
                }
            }
            serde_json::Value::Object(obj) => {
                let p = obj.get("price").and_then(parse_num);
                let s = obj.get("size").and_then(parse_num);
                match (p, s) {
                    (Some(p), Some(s)) if p > 0.0 => Some(L2Level { price: p, size: s }),
                    _ => None,
                }
            }
            _ => None,
        };
        if let Some(lvl) = parsed {
            out.push(lvl);
        }
    }
    out
}

/// Sidecar cache file format:
///   magic_u32       = 0x504D5851  ("PMXQ")
///   version_u32     = 1
///   gzipped bincode of Vec<L2Event>
///
/// The sidecar lives next to the .parquet file in the cache dir but with a
/// distinct filename; it's read-only data we add and never mutates the
/// upstream archive.
const SIDECAR_MAGIC: u32 = 0x504D_5851;
const SIDECAR_VERSION: u32 = 1;

fn write_sidecar(path: &Path, events: &[L2Event]) -> Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("bin.gz.tmp");
    let f = std::fs::File::create(&tmp).context("create sidecar tmp")?;
    let mut buf = std::io::BufWriter::new(f);
    buf.write_all(&SIDECAR_MAGIC.to_le_bytes())?;
    buf.write_all(&SIDECAR_VERSION.to_le_bytes())?;
    let encoder = flate2::write::GzEncoder::new(buf, flate2::Compression::fast());
    let mut bin = std::io::BufWriter::new(encoder);
    bincode::serialize_into(&mut bin, events).context("bincode serialize")?;
    bin.flush()?;
    let encoder = bin.into_inner().context("flush gz writer")?;
    let mut buf = encoder.finish().context("finish gz")?;
    buf.flush()?;
    drop(buf);
    std::fs::rename(&tmp, path).context("rename tmp sidecar")?;
    Ok(())
}

fn read_sidecar(path: &Path) -> Result<Vec<L2Event>> {
    use std::io::Read;
    let f = std::fs::File::open(path).context("open sidecar")?;
    let mut buf = std::io::BufReader::new(f);
    let mut hdr = [0u8; 8];
    buf.read_exact(&mut hdr).context("read sidecar header")?;
    let magic = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
    if magic != SIDECAR_MAGIC {
        anyhow::bail!("sidecar magic mismatch (file may be a different format)");
    }
    if version != SIDECAR_VERSION {
        anyhow::bail!("sidecar version {} unsupported (expected {})", version, SIDECAR_VERSION);
    }
    let decoder = flate2::read::GzDecoder::new(buf);
    let mut bin = std::io::BufReader::new(decoder);
    let events: Vec<L2Event> = bincode::deserialize_from(&mut bin).context("bincode deserialize")?;
    Ok(events)
}

fn parse_num(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_levels_json_array_pairs() {
        let levels = parse_levels_json(r#"[["0.50","100.0"],["0.49","50.0"]]"#);
        assert_eq!(levels.len(), 2);
        assert!((levels[0].price - 0.50).abs() < 1e-9);
        assert!((levels[1].size - 50.0).abs() < 1e-9);
    }

    #[test]
    fn parse_levels_json_handles_empty_and_null() {
        assert!(parse_levels_json("").is_empty());
        assert!(parse_levels_json("null").is_empty());
        assert!(parse_levels_json("[]").is_empty());
    }

    #[test]
    fn url_for_hour_uses_utc_format() {
        let h = DateTime::parse_from_rfc3339("2026-04-26T14:00:00Z").unwrap().with_timezone(&Utc);
        assert_eq!(
            PMXTv2Loader::url_for_hour(h),
            "https://r2v2.pmxt.dev/polymarket_orderbook_2026-04-26T14.parquet"
        );
    }
}
