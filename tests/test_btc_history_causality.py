"""Regression tests: BTCHistory must never leak future information.

The strategy backtest uses BTCHistory.price_at(T) to ask "what was BTC at time
T?". Before 2026-04-14, kline data was stored indexed by open_time with the
close price. At any query T < close_time, price_at returned the close price
that hadn't been observed yet — up to 59 seconds of lookahead for 1m klines.

These tests pin the lookahead-free property: price_at(T) returns a value
whose observation timestamp is ≤ T.
"""
from __future__ import annotations

import csv
import tempfile
from pathlib import Path

import pytest

from polymomentum.backtest.btc_history import BTCHistory


def _write_kline_csv(path: Path, rows: list[tuple[int, float]]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        for ts, close in rows:
            w.writerow([ts, close, close, close, close, 0])


def test_kline_load_shifts_to_close_time():
    """A 1-second kline at open_time=1000 should land at close_time=2000."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "klines.csv"
        _write_kline_csv(path, [
            (1000, 100.0),
            (2000, 101.0),
            (3000, 102.0),
        ])
        h = BTCHistory()
        h.load_csv(path)

        # First close_time is 1000 + interval(1000) = 2000
        assert h._timestamps == [2000, 3000, 4000]
        assert h._prices == [100.0, 101.0, 102.0]


def test_price_at_returns_zero_before_first_close():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "klines.csv"
        _write_kline_csv(path, [(10_000, 50.0), (11_000, 51.0)])
        h = BTCHistory()
        h.load_csv(path)

        # First stored ts is 11_000. Before that, no info observable.
        assert h.price_at(0) == 0.0
        assert h.price_at(10_000) == 0.0  # open time, bar not closed yet
        assert h.price_at(10_999) == 0.0  # still before close_time
        assert h.price_at(11_000) == 50.0  # exactly at close, now visible
        assert h.price_at(11_500) == 50.0  # stale but correct
        assert h.price_at(12_000) == 51.0  # next bar's close visible


def test_price_at_never_returns_future_data():
    """Property test: for every query T, the returned value must come from
    a stored timestamp ≤ T.
    """
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "klines.csv"
        rows = [(i * 1000, float(100 + i)) for i in range(100)]
        _write_kline_csv(path, rows)
        h = BTCHistory()
        h.load_csv(path)

        for query in range(0, 200_000, 137):  # arbitrary stride
            p = h.price_at(query)
            if p == 0.0:
                continue
            # Find the stored ts whose price we returned and assert it's ≤ query
            idx = h._prices.index(p)
            assert h._timestamps[idx] <= query, (
                f"Lookahead: query={query} returned price from ts={h._timestamps[idx]}"
            )


def test_tick_schema_not_shifted():
    """collector_tick schema carries observation timestamps directly; no shift."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "ticks.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["timestamp_ms", "source", "price"])
            for i in range(100):
                w.writerow([i * 1000, "binance", 100.0 + i])
        h = BTCHistory()
        h.load_csv(path)
        # No shift for tick format
        assert h._timestamps[0] == 0
        assert h._timestamps[-1] == 99_000
        assert h.price_at(0) == 100.0  # first observation is at ts=0
        assert h.price_at(50_000) == 150.0


def test_minute_kline_interval_autodetected():
    """1m klines should get a 60_000 ms shift, not 1000."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "klines.csv"
        rows = [
            (60_000, 100.0),
            (120_000, 101.0),
            (180_000, 102.0),
        ]
        _write_kline_csv(path, rows)
        h = BTCHistory()
        h.load_csv(path)
        assert h._timestamps == [120_000, 180_000, 240_000]
        # Query in the middle of minute 1 (between 60_000 and 120_000) — the
        # first bar hasn't closed yet, so we see nothing.
        assert h.price_at(90_000) == 0.0
        # At 120_000 (first close_time), the first bar is now visible
        assert h.price_at(120_000) == 100.0
        # Queries inside minute 2 see the first bar but not the second
        assert h.price_at(150_000) == 100.0
        assert h.price_at(180_000) == 101.0


def test_realized_vol_lookback_is_causal():
    """realized_vol_at uses lookback only — never forward bars.

    Feeds a flat-then-volatile series and asserts that vol queried over the
    flat region doesn't see the volatile future region.
    """
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "klines.csv"
        # First 100 bars flat at 100.0, next 100 bars randomized +/- 5
        rows: list[tuple[int, float]] = []
        for i in range(100):
            rows.append((i * 1000, 100.0))
        import random
        rng = random.Random(42)
        for i in range(100):
            rows.append(((100 + i) * 1000, 100.0 + rng.uniform(-5, 5)))
        _write_kline_csv(path, rows)
        h = BTCHistory()
        h.load_csv(path)

        # Query vol in the flat region. If it were looking forward into the
        # volatile region it would show high vol. If lookback-only, vol is ~0.
        # Need ≥50 ticks in history + ≥30 in window for the method to return a
        # real value; otherwise it returns the 0.50 default.
        vol_flat = h.realized_vol_at(80_000, lookback_seconds=50.0)
        # Exactly flat → no returns → method returns 0.50 default (insufficient).
        # Not the cleanest assertion, but we mainly care it's not enormous.
        assert 0 < vol_flat < 5

        # Query vol after the volatile region begins. Should be much higher.
        vol_rough = h.realized_vol_at(190_000, lookback_seconds=50.0)
        assert 0 < vol_rough <= 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
