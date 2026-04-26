"""PMXT v2 historical L2 order book loader.

V2 archive — hosted at https://r2v2.pmxt.dev — replaces v1 (r2.pmxt.dev)
with a properly typed Parquet schema, full market coverage (v1 was
missing ~50% of live markets due to incomplete subscription handling),
and redundant ingestion (gaps now rare).

Schema (per archive.pmxt.dev/docs/v2-data-overview):

    timestamp_received  timestamp[ms, UTC]    delta-encoded
    timestamp           timestamp[ms, UTC]    source ts from Polymarket
    market              fixed_size_binary[66] dict — "0x" + 64 hex chars
    event_type          string                "book" | "price_change" |
                                              "last_trade_price" | "tick_size_change"
    asset_id            string                outcome token id (decimal)
    bids                string nullable       JSON `[["px","sz"],...]` (book only)
    asks                string nullable       JSON  (book only)
    price               decimal(9,4) nullable price_change/last_trade_price
    size                decimal(18,6) nullable price_change/last_trade_price
    side                string nullable       BUY|SELL
    best_bid            decimal(9,4) nullable book event
    best_ask            decimal(9,4) nullable book event
    fee_rate_bps        uint16 nullable       tick_size_change events
    transaction_hash    string nullable       last_trade_price events
    old_tick_size       decimal(9,4) nullable tick_size_change events
    new_tick_size       decimal(9,4) nullable tick_size_change events

URL pattern (UTC hour):
    https://r2v2.pmxt.dev/polymarket_orderbook_YYYY-MM-DDTHH.parquet

File sizes: 100-400 MB each. Coverage: 2026-04-13T20 → present (~1h lag).

V1's loader (pmxt_loader.py) parsed JSON-blob-in-string columns; this
v2 loader uses native types. Both produce the same `L2Event` dataclass
so the rest of the backtest pipeline is unchanged. The v2 loader
additionally surfaces `last_trade_price` events (actual fills with
on-chain transaction hash) which v1 did not capture.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# Reuse v1 dataclasses for back-compat with the existing backtest engine
from polymomentum.backtest.pmxt_loader import (
    BookSnapshot,
    L2Event,
    L2Level,
    PriceChange,
    _parse_levels,
)

log = logging.getLogger(__name__)

# Default to the shared multi-bot cache on the VPS, fall back to a
# project-local dir for laptop development.
PMXT_V2_BASE_URL = "https://r2v2.pmxt.dev"
DEFAULT_CACHE_DIR = os.environ.get(
    "PMXT_V2_CACHE_DIR",
    "/opt/shared/pmxt_v2_cache" if Path("/opt/shared").exists() else "data/pmxt_v2_cache",
)


@dataclass
class TradePrint:
    """A `last_trade_price` event — an actual fill on Polymarket.

    V1 didn't surface these (only book snapshots and incremental price
    changes). With v2 we get the full trade tape: who bought what at
    what price, with the on-chain transaction hash. Used for slippage
    calibration and counterparty analysis.
    """
    market_id: str
    asset_id: str
    side: str           # BUY or SELL
    price: float
    size: float
    timestamp: float
    transaction_hash: str


def _archive_url_for_hour(hour: datetime) -> str:
    """Build the v2 PMXT archive URL for a given UTC hour."""
    if hour.tzinfo is None:
        hour = hour.replace(tzinfo=timezone.utc)
    h = hour.astimezone(timezone.utc)
    stamp = h.strftime("%Y-%m-%dT%H")
    return f"{PMXT_V2_BASE_URL}/polymarket_orderbook_{stamp}.parquet"


def _cache_path_for_hour(cache_dir: Path, hour: datetime) -> Path:
    if hour.tzinfo is None:
        hour = hour.replace(tzinfo=timezone.utc)
    h = hour.astimezone(timezone.utc)
    return cache_dir / f"polymarket_orderbook_{h.strftime('%Y-%m-%dT%H')}.parquet"


def _decode_market(value) -> str:
    """Decode market column (fixed_size_binary[66]) to string.

    PyArrow returns bytes for fixed_size_binary; the v2 archive stores
    "0x" + 64 hex chars as 66 ASCII bytes.
    """
    if isinstance(value, bytes):
        return value.decode("ascii", errors="replace")
    if isinstance(value, str):
        return value
    return str(value)


def _decode_decimal(value) -> float | None:
    """Decode a decimal column (decimal(9,4) or decimal(18,6)) to float.

    PyArrow's `to_pylist()` returns `decimal.Decimal` objects; convert
    to float for our existing dataclasses. Returns None if value is None.
    """
    if value is None:
        return None
    return float(value)


def _decode_timestamp(value) -> float | None:
    """Decode a timestamp[ms, UTC] column to a float seconds-since-epoch.

    PyArrow returns datetime objects (or pd.Timestamp); convert to
    float seconds for compatibility with v1's L2Event timestamps.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if hasattr(value, "timestamp"):
        return float(value.timestamp())
    return float(value)


def _row_to_event(row: dict) -> L2Event | TradePrint | None:
    """Decode one v2 row to an L2Event (or TradePrint for trades).

    Returns None for unsupported event types (currently
    tick_size_change). Returns TradePrint for last_trade_price.
    """
    event_type = row.get("event_type", "")
    market_id = _decode_market(row.get("market", b""))
    asset_id = str(row.get("asset_id", ""))
    ts = _decode_timestamp(row.get("timestamp"))
    if ts is None:
        ts = _decode_timestamp(row.get("timestamp_received")) or 0.0

    if event_type == "book":
        # bids/asks columns hold JSON strings of [[price, size], ...]
        bids_json = row.get("bids") or "[]"
        asks_json = row.get("asks") or "[]"
        try:
            bids_raw = json.loads(bids_json) if isinstance(bids_json, str) else bids_json
            asks_raw = json.loads(asks_json) if isinstance(asks_json, str) else asks_json
        except json.JSONDecodeError:
            bids_raw, asks_raw = [], []
        bids = sorted(_parse_levels(bids_raw), key=lambda lv: -lv.price)
        asks = sorted(_parse_levels(asks_raw), key=lambda lv: lv.price)
        snap = BookSnapshot(
            market_id=market_id,
            token_id=asset_id,
            side="",  # v2 book events apply to whole book, no side
            best_bid=_decode_decimal(row.get("best_bid")) or 0.0,
            best_ask=_decode_decimal(row.get("best_ask")) or 0.0,
            timestamp=ts,
            bids=bids,
            asks=asks,
        )
        return L2Event(
            update_type="book_snapshot",
            market_id=market_id,
            timestamp=ts,
            snapshot=snap,
        )

    if event_type == "price_change":
        chg = PriceChange(
            market_id=market_id,
            token_id=asset_id,
            side=str(row.get("side") or ""),
            best_bid=_decode_decimal(row.get("best_bid")) or 0.0,
            best_ask=_decode_decimal(row.get("best_ask")) or 0.0,
            timestamp=ts,
            change_price=_decode_decimal(row.get("price")) or 0.0,
            change_size=_decode_decimal(row.get("size")) or 0.0,
            change_side=str(row.get("side") or ""),
        )
        return L2Event(
            update_type="price_change",
            market_id=market_id,
            timestamp=ts,
            change=chg,
        )

    if event_type == "last_trade_price":
        return TradePrint(
            market_id=market_id,
            asset_id=asset_id,
            side=str(row.get("side") or ""),
            price=_decode_decimal(row.get("price")) or 0.0,
            size=_decode_decimal(row.get("size")) or 0.0,
            timestamp=ts,
            transaction_hash=str(row.get("transaction_hash") or ""),
        )

    # tick_size_change events ignored (we don't act on tick changes today)
    return None


class PMXTv2Loader:
    """Fetches and parses v2 PMXT historical L2 archives.

    Files cached on disk; default location is `/opt/shared/pmxt_v2_cache`
    on VPS (multi-tenant — also readable by polyarbitrage), or
    `data/pmxt_v2_cache` for local development.

    Override via `PMXT_V2_CACHE_DIR` env var or `cache_dir` argument.
    """

    def __init__(self, cache_dir: str | Path | None = None):
        self._cache_dir = Path(cache_dir or DEFAULT_CACHE_DIR)
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def cache_path(self, hour: datetime) -> Path:
        return _cache_path_for_hour(self._cache_dir, hour)

    def is_cached(self, hour: datetime) -> bool:
        path = self.cache_path(hour)
        return path.exists() and path.stat().st_size > 0

    def download_hour(self, hour: datetime, force: bool = False) -> Path:
        """Download a single hour's parquet to the cache directory.

        Returns the cache path. Raises on network error.
        """
        import httpx

        path = self.cache_path(hour)
        if path.exists() and not force and path.stat().st_size > 0:
            return path

        url = _archive_url_for_hour(hour)
        log.info("Downloading %s → %s", url, path)
        tmp = path.with_suffix(".tmp")
        with httpx.stream("GET", url, timeout=60.0, follow_redirects=True) as r:
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=1024 * 1024):
                    f.write(chunk)
        tmp.rename(path)
        return path

    def load_hour(
        self,
        hour: datetime,
        condition_ids: set[str] | None = None,
        event_types: set[str] | None = None,
    ) -> list[L2Event | TradePrint]:
        """Read one hour's events from cache, optionally filtered.

        condition_ids: if provided, only return events for these markets.
        event_types: if provided, only return matching event_type rows
                     (e.g. {"book", "price_change"} to skip trades).
        """
        path = self.download_hour(hour)
        return self._read_parquet(path, condition_ids, event_types)

    def _read_parquet(
        self,
        path: Path,
        condition_ids: set[str] | None,
        event_types: set[str] | None,
    ) -> list[L2Event | TradePrint]:
        try:
            import pyarrow.parquet as pq
        except ImportError as e:
            raise RuntimeError(
                "PMXTv2Loader requires pyarrow — `uv pip install pyarrow`"
            ) from e

        # Filter at the parquet level when possible — sort order is
        # (market, asset_id, timestamp_received), so condition_ids
        # filter pushes down efficiently.
        filters = None
        if condition_ids:
            # market column is fixed_size_binary[66]; encode as bytes.
            cid_bytes = [c.encode("ascii") for c in condition_ids]
            filters = [("market", "in", cid_bytes)]

        table = pq.read_table(path, filters=filters)
        events: list[L2Event | TradePrint] = []
        for row in table.to_pylist():
            if event_types and row.get("event_type") not in event_types:
                continue
            ev = _row_to_event(row)
            if ev is not None:
                events.append(ev)
        return events
