"""Static latency model for backtests.

Adapted from StaticLatencyConfig in
evan-kolberg/prediction-market-backtesting/_execution_config.py.

Adds realistic per-action latencies to backtests so that signals
generated at time T don't fill until T + base + insert.

Typical Polymarket latencies (Dublin VPS):
    base_latency_ms:   1-3   (network hop)
    insert_latency_ms: 5-15  (CLOB order acceptance)
    update_latency_ms: 5-15
    cancel_latency_ms: 5-15

From a US MacBook add ~70ms one-way for transatlantic.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True)
class StaticLatencyConfig:
    """Per-action latency in milliseconds.

    base_latency_ms applies to every action (network round-trip).
    The action-specific latencies are added on top of base.

    Total time to insert an order = base + insert latency.
    """
    base_latency_ms: float = 0.0
    insert_latency_ms: float = 0.0
    update_latency_ms: float = 0.0
    cancel_latency_ms: float = 0.0

    def __post_init__(self) -> None:
        for name in ("base_latency_ms", "insert_latency_ms",
                     "update_latency_ms", "cancel_latency_ms"):
            value = getattr(self, name)
            if not isfinite(value):
                raise ValueError(f"{name} must be finite, got {value!r}")
            if value < 0:
                raise ValueError(f"{name} must be non-negative, got {value!r}")

    def total_insert_ms(self) -> float:
        """Total time from signal to order acceptance (ms)."""
        return self.base_latency_ms + self.insert_latency_ms

    def total_cancel_ms(self) -> float:
        return self.base_latency_ms + self.cancel_latency_ms

    def total_update_ms(self) -> float:
        return self.base_latency_ms + self.update_latency_ms

    def is_zero(self) -> bool:
        return (
            self.base_latency_ms == 0.0
            and self.insert_latency_ms == 0.0
            and self.update_latency_ms == 0.0
            and self.cancel_latency_ms == 0.0
        )


# ── Presets ──────────────────────────────────────────────────────────


def preset_local() -> StaticLatencyConfig:
    """Zero latency — useful as a baseline (overestimates profits)."""
    return StaticLatencyConfig()


def preset_macbook_us() -> StaticLatencyConfig:
    """US MacBook on home wifi — realistic for current setup."""
    return StaticLatencyConfig(
        base_latency_ms=80.0,    # transatlantic + wifi
        insert_latency_ms=15.0,  # CLOB processing
        update_latency_ms=15.0,
        cancel_latency_ms=15.0,
    )


def preset_dublin_vps() -> StaticLatencyConfig:
    """Dublin VPS to Polymarket London (~10ms RTT)."""
    return StaticLatencyConfig(
        base_latency_ms=10.0,
        insert_latency_ms=5.0,
        update_latency_ms=5.0,
        cancel_latency_ms=5.0,
    )


def preset_swiss_vps() -> StaticLatencyConfig:
    """Swiss VPS to Polymarket London (~23ms RTT)."""
    return StaticLatencyConfig(
        base_latency_ms=23.0,
        insert_latency_ms=5.0,
        update_latency_ms=5.0,
        cancel_latency_ms=5.0,
    )
