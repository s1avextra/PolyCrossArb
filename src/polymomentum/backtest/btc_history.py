"""Historical BTC price replay for backtesting.

Loads BTC kline / tick data and exposes a timestamped lookup so the
backtest can ask "what was BTC at time T?" without re-simulating the
exchange feeds.

Two data sources:
  1. Local CSV (collected via scripts/run_collector.py)
     - data/btcusdt_1s_7d.csv (Binance kline format: ts,o,h,l,c,v)
     - data/live/btc_ticks_YYYYMMDD.csv (collector format)
  2. Binance public REST (no auth, fetched on demand)

Once loaded, prices are kept in two parallel numpy arrays for fast
binary-search lookup. Volatility is computed from the price history
window for use by the BS fair value model.
"""
from __future__ import annotations

import bisect
import csv
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class BTCSnapshot:
    """A single BTC price observation."""
    timestamp_ms: int
    price: float


class BTCHistory:
    """In-memory BTC price history with O(log n) timestamped lookup.

    Usage:
        h = BTCHistory()
        h.load_csv("data/btcusdt_1s_7d.csv")
        price = h.price_at(timestamp_ms)
        vol = h.realized_vol_at(timestamp_ms, lookback_seconds=3600)
    """

    def __init__(self):
        self._timestamps: list[int] = []  # sorted ascending
        self._prices: list[float] = []

    @property
    def n_ticks(self) -> int:
        return len(self._timestamps)

    @property
    def time_span_seconds(self) -> float:
        if len(self._timestamps) < 2:
            return 0
        return (self._timestamps[-1] - self._timestamps[0]) / 1000.0

    def load_csv(self, path: str | Path) -> int:
        """Load BTC price data from a CSV.

        Auto-detects schema:
          - Binance kline:    timestamp,open,high,low,close,volume (timestamp = open_time)
          - Collector ticks:  timestamp_ms,source,price,... (timestamp = observation time)

        For kline schemas we index by close_time (= open_time + interval), so
        price_at(T) never returns information that wasn't observable at T.
        The interval is auto-detected from the min gap between consecutive
        rows. For tick schemas the timestamp IS the observation time so no
        shift is applied.

        Returns rows added.
        """
        p = Path(path)
        if not p.exists():
            log.warning("BTC history file not found: %s", p)
            return 0

        added = 0
        with open(p, newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return 0

            # Detect schema
            ts_idx = 0
            price_idx = -1
            schema = ""
            is_kline = False

            if "timestamp" in header and "close" in header:
                # Binance kline
                ts_idx = header.index("timestamp")
                price_idx = header.index("close")
                schema = "binance_kline"
                is_kline = True
            elif "timestamp_ms" in header and "price" in header:
                # Collector tick format
                ts_idx = header.index("timestamp_ms")
                price_idx = header.index("price")
                schema = "collector_tick"
            elif "timestamp" in header and len(header) >= 6:
                ts_idx = header.index("timestamp")
                price_idx = 4  # close column in standard kline
                schema = "kline_no_header_close"
                is_kline = True
            else:
                log.warning("Unknown CSV schema in %s: %s", p.name, header)
                return 0

            raw_rows: list[tuple[int, float]] = []
            for row in reader:
                if len(row) <= max(ts_idx, price_idx):
                    continue
                try:
                    ts = int(float(row[ts_idx]))
                    price = float(row[price_idx])
                except (ValueError, TypeError):
                    continue
                if price <= 0:
                    continue
                raw_rows.append((ts, price))

        if not raw_rows:
            return 0

        raw_rows.sort(key=lambda r: r[0])

        if is_kline:
            # Detect interval as the modal gap between consecutive open_times.
            diffs = [raw_rows[i + 1][0] - raw_rows[i][0] for i in range(len(raw_rows) - 1)]
            positive_diffs = [d for d in diffs if d > 0]
            if positive_diffs:
                interval_ms = min(positive_diffs)
            else:
                # Single row or constant ts — assume 1 second as safest default
                interval_ms = 1000
            # Shift to close_time so price_at(T) never returns a price from the
            # future: the kline's close price is only observable at close_time.
            for ts, price in raw_rows:
                self._timestamps.append(ts + interval_ms)
                self._prices.append(price)
            schema = f"{schema}(interval={interval_ms}ms)"
        else:
            for ts, price in raw_rows:
                self._timestamps.append(ts)
                self._prices.append(price)
            added = len(raw_rows)

        added = len(raw_rows)

        # Sort + dedupe if we had pre-existing entries from another load_*
        if self._timestamps != sorted(self._timestamps):
            paired = sorted(zip(self._timestamps, self._prices))
            seen = set()
            uniq_ts: list[int] = []
            uniq_p: list[float] = []
            for ts, pr in paired:
                if ts in seen:
                    continue
                seen.add(ts)
                uniq_ts.append(ts)
                uniq_p.append(pr)
            self._timestamps = uniq_ts
            self._prices = uniq_p

        log.info("Loaded %d BTC ticks from %s (%s)", added, p.name, schema)
        return added

    def load_from_binance(
        self,
        start: datetime,
        end: datetime,
        symbol: str = "BTCUSDT",
        interval: str = "1m",
    ) -> int:
        """Fetch BTC klines from Binance public REST API.

        No auth required. Limit 1000 klines per request — paginates as needed.
        Free tier rate limit is generous for backtesting.

        Returns number of new ticks added.
        """
        import httpx

        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        added = 0

        # Binance limit: 1000 klines per request
        # 1m interval = 1000 minutes ≈ 16.6 hours per request
        cursor = start_ms
        with httpx.Client(timeout=15.0) as client:
            while cursor < end_ms:
                try:
                    resp = client.get(
                        "https://api.binance.com/api/v3/klines",
                        params={
                            "symbol": symbol,
                            "interval": interval,
                            "startTime": cursor,
                            "endTime": end_ms,
                            "limit": 1000,
                        },
                    )
                    resp.raise_for_status()
                    klines = resp.json()
                except Exception as e:
                    log.warning("Binance kline fetch failed: %s", str(e)[:60])
                    break

                if not klines:
                    break

                for k in klines:
                    # Binance kline: [open_time, o, h, l, c, v, close_time, ...]
                    # Store at close_time so price_at(T) cannot return a value
                    # that wasn't yet observable at T.
                    try:
                        close_time_ms = int(k[6])
                        close = float(k[4])
                    except (ValueError, TypeError, IndexError):
                        continue
                    self._timestamps.append(close_time_ms)
                    self._prices.append(close)
                    added += 1

                last_ts = int(klines[-1][0])
                if last_ts <= cursor:
                    break
                cursor = last_ts + 1

                if len(klines) < 1000:
                    break  # final page

        # Re-sort after adding
        if added > 0:
            paired = sorted(zip(self._timestamps, self._prices))
            # Deduplicate by timestamp
            seen = set()
            uniq_ts = []
            uniq_p = []
            for ts, pr in paired:
                if ts in seen:
                    continue
                seen.add(ts)
                uniq_ts.append(ts)
                uniq_p.append(pr)
            self._timestamps = uniq_ts
            self._prices = uniq_p

        log.info("Fetched %d BTC %s klines from Binance (%s -> %s)",
                 added, interval, start, end)
        return added

    def price_at(self, timestamp_ms: int) -> float:
        """Return the most recent observable BTC price at timestamp T.

        Lookahead-free contract: the returned price is the last price whose
        observation timestamp is ≤ T. For kline data this means we only
        return a kline's close price once the close_time of that kline has
        passed (stored timestamps are close_times, not open_times).

        Returns 0 if we have no data observable at or before T, or if the
        history is empty.
        """
        if not self._timestamps:
            return 0.0
        idx = bisect.bisect_right(self._timestamps, timestamp_ms) - 1
        if idx < 0:
            return 0.0
        return self._prices[idx]

    def price_at_seconds(self, timestamp_s: float) -> float:
        """Convenience: timestamp in seconds."""
        return self.price_at(int(timestamp_s * 1000))

    def range_at(self, start_ms: int, end_ms: int) -> tuple[float, float, float, float]:
        """Get (open, high, low, close) for a time range.

        Returns (0,0,0,0) if no ticks in range.
        """
        lo = bisect.bisect_left(self._timestamps, start_ms)
        hi = bisect.bisect_right(self._timestamps, end_ms)
        if lo >= hi:
            return (0.0, 0.0, 0.0, 0.0)
        prices = self._prices[lo:hi]
        return (prices[0], max(prices), min(prices), prices[-1])

    def realized_vol_at(
        self,
        timestamp_ms: int,
        lookback_seconds: float = 3600.0,
    ) -> float:
        """Compute annualized realized volatility from prices in lookback window.

        Returns ~0.50 (default) if insufficient data.
        """
        if len(self._timestamps) < 50:
            return 0.50

        cutoff_lo = timestamp_ms - int(lookback_seconds * 1000)
        cutoff_hi = timestamp_ms

        lo = bisect.bisect_left(self._timestamps, cutoff_lo)
        hi = bisect.bisect_right(self._timestamps, cutoff_hi)
        window_prices = self._prices[lo:hi]
        window_ts = self._timestamps[lo:hi]

        if len(window_prices) < 30:
            return 0.50

        # Log returns
        log_returns: list[float] = []
        time_deltas: list[float] = []
        for i in range(1, len(window_prices)):
            if window_prices[i - 1] <= 0:
                continue
            r = math.log(window_prices[i] / window_prices[i - 1])
            dt = (window_ts[i] - window_ts[i - 1]) / 1000.0
            if dt > 0:
                log_returns.append(r)
                time_deltas.append(dt)

        if len(log_returns) < 20:
            return 0.50

        avg_dt = sum(time_deltas) / len(time_deltas)
        mean_r = sum(log_returns) / len(log_returns)
        var_r = sum((r - mean_r) ** 2 for r in log_returns) / len(log_returns)

        if avg_dt <= 0:
            return 0.50

        var_per_second = var_r / avg_dt
        annualized = math.sqrt(var_per_second * 365.25 * 86400)
        return min(5.0, max(0.05, annualized))

    def first_timestamp(self) -> int:
        return self._timestamps[0] if self._timestamps else 0

    def last_timestamp(self) -> int:
        return self._timestamps[-1] if self._timestamps else 0
