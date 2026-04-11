"""PMXT historical L2 order book loader.

Fetches free historical Polymarket order book data from r2.pmxt.dev.
This is the same archive used by NautilusTrader's prediction-market
backtesting fork (evan-kolberg/prediction-market-backtesting).

Schema (each parquet file = 1 hour of L2 events):
    market_id    string
    update_type  string  -- "book_snapshot" or "price_change"
    data         string  -- JSON payload

book_snapshot payload:
    {
        "market_id":  "0x...",
        "token_id":   "12345...",
        "side":       "buy",
        "best_bid":   "0.45",
        "best_ask":   "0.47",
        "timestamp":  1710000000.123,
        "bids":       [["0.45", "100.0"], ...],
        "asks":       [["0.47", "120.0"], ...]
    }

price_change payload:
    {
        "market_id":     "0x...",
        "token_id":      "12345...",
        "side":          "buy",
        "best_bid":      "0.45",
        "best_ask":      "0.47",
        "timestamp":     1710000001.456,
        "change_price":  "0.46",
        "change_size":   "25.0",
        "change_side":   "buy"
    }

URL pattern:
    https://r2.pmxt.dev/polymarket_orderbook_YYYY-MM-DDTHH.parquet

Usage:
    loader = PMXTLoader(cache_dir="data/pmxt_cache")
    events = await loader.load_hour(datetime(2026, 4, 10, 14))
    book_snapshots = [e for e in events if e.update_type == "book_snapshot"]
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

log = logging.getLogger(__name__)

PMXT_BASE_URL = "https://r2.pmxt.dev"


@dataclass
class L2Level:
    """A single price level in an L2 book."""
    price: float
    size: float


@dataclass
class BookSnapshot:
    """Full L2 book state at a point in time."""
    market_id: str
    token_id: str
    side: str
    best_bid: float
    best_ask: float
    timestamp: float
    bids: list[L2Level]
    asks: list[L2Level]


@dataclass
class PriceChange:
    """An L2 incremental update."""
    market_id: str
    token_id: str
    side: str
    best_bid: float
    best_ask: float
    timestamp: float
    change_price: float
    change_size: float
    change_side: str


@dataclass
class L2Event:
    """A single L2 event from the PMXT archive."""
    update_type: str  # "book_snapshot" or "price_change"
    market_id: str
    timestamp: float
    snapshot: BookSnapshot | None = None
    change: PriceChange | None = None


def _archive_url_for_hour(hour: datetime) -> str:
    """Build the PMXT archive URL for a given UTC hour.

    Format: https://r2.pmxt.dev/polymarket_orderbook_YYYY-MM-DDTHH.parquet
    """
    if hour.tzinfo is None:
        hour = hour.replace(tzinfo=timezone.utc)
    h = hour.astimezone(timezone.utc)
    stamp = h.strftime("%Y-%m-%dT%H")
    return f"{PMXT_BASE_URL}/polymarket_orderbook_{stamp}.parquet"


def _cache_path_for_hour(cache_dir: Path, hour: datetime) -> Path:
    if hour.tzinfo is None:
        hour = hour.replace(tzinfo=timezone.utc)
    h = hour.astimezone(timezone.utc)
    return cache_dir / f"polymarket_orderbook_{h.strftime('%Y-%m-%dT%H')}.parquet"


def _parse_levels(raw: list) -> list[L2Level]:
    """Parse [["0.45", "100.0"], ...] into L2Level list."""
    out: list[L2Level] = []
    for entry in raw or []:
        if isinstance(entry, list) and len(entry) >= 2:
            try:
                out.append(L2Level(price=float(entry[0]), size=float(entry[1])))
            except (ValueError, TypeError):
                continue
        elif isinstance(entry, dict):
            try:
                out.append(L2Level(
                    price=float(entry.get("price", 0)),
                    size=float(entry.get("size", 0)),
                ))
            except (ValueError, TypeError):
                continue
    return out


def _parse_event(update_type: str, market_id: str, payload: str | dict) -> L2Event | None:
    """Decode a single PMXT event row."""
    if isinstance(payload, str):
        try:
            d = json.loads(payload)
        except json.JSONDecodeError:
            return None
    else:
        d = payload

    if not isinstance(d, dict):
        return None

    try:
        ts = float(d.get("timestamp", 0) or 0)
    except (ValueError, TypeError):
        return None

    if update_type == "book_snapshot":
        # Sort bids descending (best bid first), asks ascending (best ask first)
        # PMXT delivers them in arbitrary order; we normalize for the standard convention.
        bids = sorted(_parse_levels(d.get("bids", [])), key=lambda lv: -lv.price)
        asks = sorted(_parse_levels(d.get("asks", [])), key=lambda lv: lv.price)
        snap = BookSnapshot(
            market_id=str(d.get("market_id", market_id)),
            token_id=str(d.get("token_id", "")),
            side=str(d.get("side", "")),
            best_bid=float(d.get("best_bid", 0) or 0),
            best_ask=float(d.get("best_ask", 0) or 0),
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

    if update_type == "price_change":
        chg = PriceChange(
            market_id=str(d.get("market_id", market_id)),
            token_id=str(d.get("token_id", "")),
            side=str(d.get("side", "")),
            best_bid=float(d.get("best_bid", 0) or 0),
            best_ask=float(d.get("best_ask", 0) or 0),
            timestamp=ts,
            change_price=float(d.get("change_price", 0) or 0),
            change_size=float(d.get("change_size", 0) or 0),
            change_side=str(d.get("change_side", "")),
        )
        return L2Event(
            update_type="price_change",
            market_id=market_id,
            timestamp=ts,
            change=chg,
        )

    return None


class PMXTLoader:
    """Fetches and parses PMXT historical L2 archives.

    Files are cached on disk so repeated backtests don't re-download.
    """

    def __init__(
        self,
        cache_dir: str | Path = "data/pmxt_cache",
        relay_url: str | None = None,
    ):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._relay_url = relay_url or os.environ.get("PMXT_RELAY_URL", "")

    def _download_to_cache(self, url: str, dest: Path) -> bool:
        """Download a parquet file to disk. Returns True on success."""
        import httpx

        try:
            tmp = dest.with_suffix(dest.suffix + ".tmp")
            with httpx.Client(timeout=60.0, follow_redirects=True) as client:
                with client.stream("GET", url) as resp:
                    if resp.status_code != 200:
                        log.debug("PMXT %s -> HTTP %d", url, resp.status_code)
                        return False
                    with open(tmp, "wb") as f:
                        for chunk in resp.iter_bytes(chunk_size=65536):
                            f.write(chunk)
            tmp.rename(dest)
            return True
        except Exception as e:
            log.debug("PMXT download failed for %s: %s", url, str(e)[:80])
            return False

    def _ensure_hour_cached(self, hour: datetime) -> Path | None:
        """Ensure the parquet for the given hour is on disk."""
        cache = _cache_path_for_hour(self._cache_dir, hour)
        if cache.exists() and cache.stat().st_size > 0:
            return cache

        url = _archive_url_for_hour(hour)
        if self._download_to_cache(url, cache):
            return cache

        # Fall back to relay if configured
        if self._relay_url:
            relay_url = f"{self._relay_url.rstrip('/')}/polymarket_orderbook_{hour.astimezone(timezone.utc).strftime('%Y-%m-%dT%H')}.parquet"
            if self._download_to_cache(relay_url, cache):
                return cache

        return None

    def load_hour(
        self,
        hour: datetime,
        market_ids: set[str] | None = None,
        token_ids: set[str] | None = None,
        batch_size: int = 50_000,
    ) -> Iterator[L2Event]:
        """Load all events for a given UTC hour.

        Streams the parquet file in batches so memory stays bounded
        even for 700MB+ archives. Filters market_ids at the parquet
        scan layer (push-down) when possible.

        Args:
            hour: UTC datetime — the hour to load (minute/second ignored)
            market_ids: Optional filter — only emit these market IDs
            token_ids: Optional filter — only emit these token IDs
            batch_size: Number of rows per batch (default 50K)

        Yields:
            L2Event objects in chronological order WITHIN each batch.
            Use sort_events() for full ordering across batches.
        """
        path = self._ensure_hour_cached(hour)
        if not path:
            log.warning("PMXT data unavailable for %s", hour)
            return

        try:
            import pyarrow.parquet as pq
            import pyarrow.compute as pc
        except ImportError:
            raise RuntimeError(
                "pyarrow is required to load PMXT parquet files. "
                "Install with: uv pip install pyarrow"
            )

        t0 = time.time()
        parquet_file = pq.ParquetFile(path)

        # Pre-filter at the parquet level if market_ids supplied
        filter_expr = None
        if market_ids:
            filter_expr = pc.field("market_id").isin(list(market_ids))

        total_rows = 0
        emitted = 0

        for batch in parquet_file.iter_batches(
            batch_size=batch_size,
            columns=["market_id", "update_type", "data"],
        ):
            if filter_expr is not None:
                batch = batch.filter(filter_expr)
                if batch.num_rows == 0:
                    continue

            total_rows += batch.num_rows
            mids = batch.column("market_id").to_pylist()
            uts = batch.column("update_type").to_pylist()
            datas = batch.column("data").to_pylist()

            for mid, ut, d in zip(mids, uts, datas):
                event = _parse_event(str(ut), str(mid), d)
                if event is None:
                    continue
                if token_ids:
                    tid = ""
                    if event.snapshot:
                        tid = event.snapshot.token_id
                    elif event.change:
                        tid = event.change.token_id
                    if tid not in token_ids:
                        continue
                emitted += 1
                yield event

        log.debug(
            "PMXT load_hour %s: scanned %d rows, emitted %d events in %.1fs",
            path.name, total_rows, emitted, time.time() - t0,
        )

    @staticmethod
    def sort_events(events: Iterator[L2Event]) -> list[L2Event]:
        """Materialize and sort events by timestamp.

        Use this when you need fully chronological order (e.g. for
        a strategy callback). Costs memory proportional to event count.
        """
        materialized = list(events)
        materialized.sort(key=lambda e: e.timestamp)
        return materialized

    def load_range(
        self,
        start: datetime,
        end: datetime,
        market_ids: set[str] | None = None,
        token_ids: set[str] | None = None,
    ) -> Iterator[L2Event]:
        """Load events across an inclusive hour range.

        Iterates one hour at a time so memory stays bounded.
        """
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        cursor = start.replace(minute=0, second=0, microsecond=0)
        end_hour = end.replace(minute=0, second=0, microsecond=0)

        while cursor <= end_hour:
            yield from self.load_hour(cursor, market_ids=market_ids, token_ids=token_ids)
            cursor = cursor.replace(hour=(cursor.hour + 1) % 24) if cursor.hour < 23 else \
                     cursor.replace(day=cursor.day + 1, hour=0)

    def list_cached_hours(self) -> list[datetime]:
        """List all hours currently cached on disk."""
        result: list[datetime] = []
        for f in sorted(self._cache_dir.glob("polymarket_orderbook_*.parquet")):
            stamp = f.stem.replace("polymarket_orderbook_", "")
            try:
                dt = datetime.strptime(stamp, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
                result.append(dt)
            except ValueError:
                continue
        return result
