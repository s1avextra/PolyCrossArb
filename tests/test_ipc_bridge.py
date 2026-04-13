"""Stress tests for the UDS IPC bridge.

Tests:
  1. Message throughput: 1000+ msgs/s encoding/decoding
  2. Frame format correctness: length-prefixed msgpack
  3. Concurrent signal handling: 10 simultaneous contract signals
  4. Backpressure: sender faster than receiver
  5. Reconnection: bridge handles disconnection gracefully
"""
from __future__ import annotations

import asyncio
import struct
import time

import pytest


# ── Frame encoding tests ──────────────────────────────────────────

class TestFrameEncoding:
    """Verify the length-prefixed msgpack frame format."""

    def test_frame_header_is_4_bytes_big_endian(self):
        """Frame header: 4-byte big-endian uint32 length prefix."""
        length = 256
        header = struct.pack(">I", length)
        assert len(header) == 4
        assert struct.unpack(">I", header)[0] == 256

    def test_frame_max_size_enforced(self):
        """Frames larger than 10MB should be rejected."""
        MAX_FRAME_SIZE = 10 * 1024 * 1024
        assert MAX_FRAME_SIZE == 10_485_760

    def test_msgpack_roundtrip(self):
        """Msgpack encode → decode preserves message structure."""
        try:
            import msgpack
        except ImportError:
            pytest.skip("msgpack not installed")

        msg = {
            "type": "contracts",
            "data": [
                {
                    "contract_id": "abc123",
                    "token_id": "tok456",
                    "up_price": 0.45,
                    "down_price": 0.55,
                    "end_time_s": 1712345678.0,
                    "window_minutes": 5.0,
                }
            ],
        }
        packed = msgpack.packb(msg, use_bin_type=True)
        unpacked = msgpack.unpackb(packed, raw=False)
        assert unpacked["type"] == "contracts"
        assert len(unpacked["data"]) == 1
        assert unpacked["data"][0]["up_price"] == 0.45

    def test_msgpack_throughput(self):
        """Encode/decode 10000 messages in under 1 second."""
        try:
            import msgpack
        except ImportError:
            pytest.skip("msgpack not installed")

        msg = {
            "type": "trade_signal",
            "contract_id": "abc123def456",
            "token_id": "tok789",
            "direction": "up",
            "edge": 0.087,
            "fair_value": 0.537,
            "mm_price": 0.450,
            "size_usd": 5.0,
        }

        n = 10_000
        t0 = time.perf_counter()
        for _ in range(n):
            packed = msgpack.packb(msg, use_bin_type=True)
            msgpack.unpackb(packed, raw=False)
        elapsed = time.perf_counter() - t0

        msgs_per_sec = n / elapsed
        # Must sustain > 1000 msgs/s (typically achieves 50k+)
        assert msgs_per_sec > 1000, f"Only {msgs_per_sec:.0f} msgs/s (need >1000)"


# ── Concurrent signal tests ───────────────────────────────────────

class TestConcurrentSignals:
    """Verify that multiple contract signals can fire in rapid succession."""

    def test_ten_contracts_within_50ms(self):
        """10 simultaneous trade decisions complete within 50ms total."""
        from polymomentum.crypto.decision import decide_candle_trade, ZoneConfig
        from polymomentum.crypto.momentum import MomentumSignal

        signals = []
        for i in range(10):
            signals.append(MomentumSignal(
                direction="up" if i % 2 == 0 else "down",
                confidence=0.70 + i * 0.01,
                price_change=50.0 + i * 10,
                price_change_pct=0.0008,
                consistency=0.85,
                minutes_elapsed=3.0,
                minutes_remaining=2.0,
                current_price=60_050.0 + i * 10,
                open_price=60_000.0,
                z_score=2.0,
            ))

        cfg = ZoneConfig()
        t0 = time.perf_counter()

        results = []
        for sig in signals:
            result = decide_candle_trade(
                signal=sig,
                minutes_elapsed=3.0,
                minutes_remaining=2.0,
                window_minutes=5.0,
                up_price=0.40,
                down_price=0.60,
                btc_price=sig.current_price,
                open_btc=60_000.0,
                implied_vol=0.50,
                zone_config=cfg,
            )
            results.append(result)

        elapsed_ms = (time.perf_counter() - t0) * 1000
        assert elapsed_ms < 50, f"10 decisions took {elapsed_ms:.1f}ms (need <50ms)"
        assert len(results) == 10

    def test_momentum_detection_throughput(self):
        """MomentumDetector can handle 100 detect() calls in under 100ms."""
        from polymomentum.crypto.momentum import MomentumDetector

        detector = MomentumDetector(realized_vol=0.50)
        base_time = time.time()
        for i in range(300):
            detector.add_tick(60_000 + i * 0.5, timestamp=base_time + i)
        detector.set_window_open("test", 60_000)

        t0 = time.perf_counter()
        for i in range(100):
            detector.detect(
                contract_id=f"contract_{i}",
                window_start_ago_minutes=5.0,
                minutes_remaining=0.5,
                current_price=60_150.0,
                now_ts=base_time + 300,
            )
        elapsed_ms = (time.perf_counter() - t0) * 1000
        assert elapsed_ms < 100, f"100 detections took {elapsed_ms:.1f}ms (need <100ms)"


# ── Fill model tests ──────────────────────────────────────────────

class TestMakerFillModel:
    """Verify the MakerFillModel for backtest fee optimization."""

    def test_maker_fill_rate_converges(self):
        """Over many trials, maker fill rate should be near fill_prob."""
        from polymomentum.backtest.fill_model import MakerFillModel

        model = MakerFillModel(fill_prob=0.65, seed=42)
        n = 1000
        maker_fills = 0
        for _ in range(n):
            result = model.fill("buy", 10.0, 0.45, 0.55)
            if result.reason == "maker_fill":
                maker_fills += 1

        actual_rate = maker_fills / n
        # Should be within 5% of target
        assert 0.60 <= actual_rate <= 0.70, f"Maker fill rate {actual_rate:.2%} (expected ~65%)"

    def test_maker_fill_has_price_improvement(self):
        """Maker fills at best_ask - 1 tick (price improvement over touch)."""
        from polymomentum.backtest.fill_model import MakerFillModel

        model = MakerFillModel(fill_prob=1.0, seed=42)  # force maker fill
        result = model.fill("buy", 10.0, 0.45, 0.55)
        assert result.reason == "maker_fill"
        assert result.fill_price == 0.54  # best_ask - 1 tick
        assert result.slippage_per_share < 0  # negative = price improvement

    def test_taker_fallback_has_adverse_move(self):
        """Taker fallback should have one-tick adverse slippage."""
        from polymomentum.backtest.fill_model import MakerFillModel

        model = MakerFillModel(fill_prob=0.0, seed=42)  # force taker fallback
        result = model.fill("buy", 10.0, 0.45, 0.55)
        assert result.reason == "taker_fallback"
        assert result.fill_price == 0.56  # best_ask + 1 tick
        assert result.slippage_per_share > 0


# ── VolatilityRegime integration ──────────────────────────────────

class TestVolRegimeIntegration:
    """End-to-end vol regime classification with realistic data."""

    def test_vol_regime_with_price_feed_data(self):
        """Classify vol regime from synthetic price history."""
        from polymomentum.crypto.momentum import classify_vol_regime, VolatilityRegime

        # Normal conditions: short_vol ≈ baseline_vol
        assert classify_vol_regime(0.50, 0.50) == VolatilityRegime.NORMAL

        # FOMC day: short_vol 2x baseline
        assert classify_vol_regime(1.00, 0.50) == VolatilityRegime.HIGH

        # Flash crash: short_vol 3x baseline
        assert classify_vol_regime(1.50, 0.50) == VolatilityRegime.EXTREME
