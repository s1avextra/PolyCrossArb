"""Reconstruct candle windows from 1-second tick data.

Takes CSV of 1-second klines and produces candle windows matching
Polymarket's "Bitcoin Up or Down" contracts:
  - 5-minute windows (e.g. 3:45AM-3:50AM)
  - 15-minute windows (e.g. 3:45AM-4:00AM)
  - 60-minute windows (e.g. 3AM-4AM)

Each window has a ground-truth outcome: "up" or "down".
"""
from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class CandleWindow:
    """A reconstructed candle window with ground truth."""
    start_ms: int
    end_ms: int
    open_price: float
    close_price: float
    high: float
    low: float
    outcome: str  # "up" or "down"
    window_minutes: int
    tick_count: int
    ticks: list[tuple[int, float]]  # (timestamp_ms, close_price)

    @property
    def price_change(self) -> float:
        return self.close_price - self.open_price

    @property
    def price_change_pct(self) -> float:
        if self.open_price == 0:
            return 0
        return self.price_change / self.open_price


def load_ticks(csv_path: str | Path) -> list[tuple[int, float, float, float, float]]:
    """Load ticks from CSV: (timestamp_ms, open, high, low, close)."""
    ticks = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticks.append((
                int(row["timestamp"]),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
            ))
    ticks.sort(key=lambda t: t[0])
    log.info("Loaded %d ticks from %s", len(ticks), csv_path)
    return ticks


def build_windows(
    ticks: list[tuple[int, float, float, float, float]],
    window_minutes: int = 5,
) -> list[CandleWindow]:
    """Build fixed-length candle windows from tick data.

    Args:
        ticks: List of (timestamp_ms, open, high, low, close) tuples.
        window_minutes: Window length in minutes (5, 15, or 60).

    Returns:
        List of CandleWindow with ground-truth outcomes.
    """
    if not ticks:
        return []

    window_ms = window_minutes * 60 * 1000
    first_ts = ticks[0][0]
    last_ts = ticks[-1][0]

    # Align to window boundaries
    window_start = first_ts - (first_ts % window_ms)

    windows: list[CandleWindow] = []
    tick_idx = 0

    while window_start + window_ms <= last_ts:
        window_end = window_start + window_ms
        window_ticks: list[tuple[int, float]] = []
        high = 0.0
        low = float("inf")

        # Collect ticks in this window
        while tick_idx < len(ticks) and ticks[tick_idx][0] < window_end:
            ts, o, h, l, c = ticks[tick_idx]
            if ts >= window_start:
                window_ticks.append((ts, c))
                high = max(high, h)
                low = min(low, l)
            tick_idx += 1

        # Back up index for overlapping windows
        # (not needed for non-overlapping, but safe)
        save_idx = tick_idx

        if len(window_ticks) >= 2:
            open_price = window_ticks[0][1]
            close_price = window_ticks[-1][1]
            outcome = "up" if close_price >= open_price else "down"

            windows.append(CandleWindow(
                start_ms=window_start,
                end_ms=window_end,
                open_price=open_price,
                close_price=close_price,
                high=high,
                low=low,
                outcome=outcome,
                window_minutes=window_minutes,
                tick_count=len(window_ticks),
                ticks=window_ticks,
            ))

        window_start = window_end

    log.info("Built %d %d-min windows", len(windows), window_minutes)
    return windows


def build_all_window_sizes(
    ticks: list[tuple[int, float, float, float, float]],
) -> dict[int, list[CandleWindow]]:
    """Build windows for all standard Polymarket candle sizes."""
    return {
        5: build_windows(ticks, 5),
        15: build_windows(ticks, 15),
        60: build_windows(ticks, 60),
    }
