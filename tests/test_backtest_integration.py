"""Tests for the PMXT-integrated backtest engine.

Verifies:
  - Polymarket fee formula matches NautilusTrader implementation
  - One-tick taker fill model behaves correctly
  - Book-walk fill model walks levels properly
  - Latency model validates inputs and computes totals
  - PMXT parser decodes book_snapshot and price_change records
  - L2 replay engine maintains book state across events
"""
from __future__ import annotations

import math

import pytest

from polycrossarb.backtest.fill_model import (
    BookWalkTakerFillModel,
    OneTickTakerFillModel,
    PerfectFillModel,
    one_tick_adverse_price,
)
from polycrossarb.backtest.latency_model import (
    StaticLatencyConfig,
    preset_dublin_vps,
    preset_macbook_us,
    preset_swiss_vps,
)
from polycrossarb.backtest.l2_replay import (
    BacktestOrder,
    L2BacktestEngine,
    TokenBook,
)
from polycrossarb.backtest.pmxt_loader import (
    BookSnapshot,
    L2Event,
    L2Level,
    PriceChange,
    _archive_url_for_hour,
    _parse_event,
    _parse_levels,
)
from polycrossarb.execution.fees import polymarket_fee
from datetime import datetime, timezone


# ── Fee formula ──────────────────────────────────────────────────────


class TestPolymarketFee:
    """The NautilusTrader-verified Polymarket fee formula:
    fee = qty * fee_rate * p * (1 - p)
    """

    def test_zero_at_extremes(self):
        # p=0: fee = 100 * 0.072 * 0 * 1 = 0
        assert polymarket_fee(100, 0.0, 0.072) == 0.0
        # p=1: fee = 100 * 0.072 * 1 * 0 = 0
        assert polymarket_fee(100, 1.0, 0.072) == 0.0

    def test_max_at_half(self):
        # Max fee at p=0.5
        f_half = polymarket_fee(100, 0.5, 0.072)
        f_low = polymarket_fee(100, 0.1, 0.072)
        f_high = polymarket_fee(100, 0.9, 0.072)
        assert f_half > f_low
        assert f_half > f_high

    def test_known_value(self):
        # qty=100, p=0.5, rate=0.072
        # fee = 100 * 0.072 * 0.5 * 0.5 = 1.8
        assert polymarket_fee(100, 0.5, 0.072) == pytest.approx(1.8, abs=1e-5)

    def test_zero_rate_yields_zero(self):
        assert polymarket_fee(100, 0.5, 0.0) == 0.0

    def test_zero_size_yields_zero(self):
        assert polymarket_fee(0, 0.5, 0.072) == 0.0

    def test_symmetric_around_half(self):
        # f(p) should equal f(1-p)
        for p in [0.1, 0.2, 0.3, 0.4]:
            f1 = polymarket_fee(100, p, 0.072)
            f2 = polymarket_fee(100, 1 - p, 0.072)
            assert f1 == pytest.approx(f2, abs=1e-5)


# ── Fill models ──────────────────────────────────────────────────────


class TestOneTickAdverse:
    def test_buy_pays_above_ask(self):
        # buy: pay best_ask + 1 tick
        p = one_tick_adverse_price("buy", best_bid=0.45, best_ask=0.47, tick_size=0.01)
        assert p == pytest.approx(0.48, abs=1e-9)

    def test_sell_receives_below_bid(self):
        p = one_tick_adverse_price("sell", best_bid=0.45, best_ask=0.47, tick_size=0.01)
        assert p == pytest.approx(0.44, abs=1e-9)

    def test_clamped_to_unit_interval(self):
        p = one_tick_adverse_price("buy", best_bid=0.98, best_ask=0.99, tick_size=0.01)
        assert p <= 0.99
        p = one_tick_adverse_price("sell", best_bid=0.01, best_ask=0.02, tick_size=0.01)
        assert p >= 0.01


class TestOneTickTakerFillModel:
    def test_market_buy_fills_with_slippage(self):
        m = OneTickTakerFillModel(tick_size=0.01)
        r = m.fill("buy", 50, best_bid=0.45, best_ask=0.47, order_type="market")
        assert r.success
        assert r.fill_price == pytest.approx(0.48, abs=1e-9)
        assert r.slippage_per_share == pytest.approx(0.01, abs=1e-9)
        assert r.fill_cost == pytest.approx(0.48 * 50, abs=1e-9)

    def test_market_sell_fills_with_slippage(self):
        m = OneTickTakerFillModel(tick_size=0.01)
        r = m.fill("sell", 50, best_bid=0.45, best_ask=0.47, order_type="market")
        assert r.success
        assert r.fill_price == pytest.approx(0.44, abs=1e-9)
        # cost is signed: negative for sells
        assert r.fill_cost < 0

    def test_limit_buy_at_touch_no_slippage(self):
        m = OneTickTakerFillModel(tick_size=0.01)
        r = m.fill("buy", 50, best_bid=0.45, best_ask=0.47,
                   order_type="limit", limit_price=0.47)
        assert r.success
        assert r.fill_price == 0.47
        assert r.slippage_per_share == 0

    def test_limit_buy_below_ask_does_not_fill(self):
        m = OneTickTakerFillModel(tick_size=0.01)
        r = m.fill("buy", 50, best_bid=0.45, best_ask=0.47,
                   order_type="limit", limit_price=0.46)
        assert not r.success

    def test_invalid_book_rejected(self):
        m = OneTickTakerFillModel()
        # crossed book
        r = m.fill("buy", 50, best_bid=0.50, best_ask=0.45, order_type="market")
        assert not r.success


class TestBookWalkFill:
    def test_consumes_top_of_book(self):
        m = BookWalkTakerFillModel(tick_size=0.01)
        asks = [(0.47, 30), (0.48, 50), (0.49, 100)]
        bids = [(0.45, 100)]
        r = m.fill("buy", 30, bids, asks)
        assert r.success
        assert r.fill_price == pytest.approx(0.47, abs=1e-9)

    def test_walks_multiple_levels(self):
        m = BookWalkTakerFillModel(tick_size=0.01)
        asks = [(0.47, 10), (0.48, 20), (0.49, 30)]
        bids = [(0.45, 100)]
        # 50 shares: 10 @ 0.47 + 20 @ 0.48 + 20 @ 0.49 = 4.7 + 9.6 + 9.8 = 24.1
        r = m.fill("buy", 50, bids, asks)
        assert r.success
        expected_vwap = (10 * 0.47 + 20 * 0.48 + 20 * 0.49) / 50
        assert r.fill_price == pytest.approx(expected_vwap, abs=1e-9)

    def test_synthetic_extension_when_depth_exhausted(self):
        m = BookWalkTakerFillModel(tick_size=0.01)
        asks = [(0.47, 5)]  # only 5 available
        bids = [(0.45, 100)]
        r = m.fill("buy", 10, bids, asks)
        assert r.success
        # Walks 5 @ 0.47, then 5 @ 0.48 (synthetic)
        expected = (5 * 0.47 + 5 * 0.48) / 10
        assert r.fill_price == pytest.approx(expected, abs=1e-9)


class TestPerfectFill:
    def test_no_slippage(self):
        m = PerfectFillModel()
        r = m.fill("buy", 100, best_bid=0.45, best_ask=0.47)
        assert r.success
        assert r.fill_price == 0.47
        assert r.slippage_per_share == 0


# ── Latency model ────────────────────────────────────────────────────


class TestStaticLatencyConfig:
    def test_default_is_zero(self):
        c = StaticLatencyConfig()
        assert c.is_zero()
        assert c.total_insert_ms() == 0

    def test_total_includes_base(self):
        c = StaticLatencyConfig(base_latency_ms=10, insert_latency_ms=5)
        assert c.total_insert_ms() == 15
        assert c.total_cancel_ms() == 10
        assert c.total_update_ms() == 10

    def test_negative_rejected(self):
        with pytest.raises(ValueError):
            StaticLatencyConfig(base_latency_ms=-1)

    def test_non_finite_rejected(self):
        with pytest.raises(ValueError):
            StaticLatencyConfig(base_latency_ms=float("inf"))

    def test_dublin_preset(self):
        c = preset_dublin_vps()
        assert c.total_insert_ms() == 15  # 10 base + 5 insert

    def test_swiss_preset(self):
        c = preset_swiss_vps()
        assert c.total_insert_ms() == 28  # 23 base + 5 insert

    def test_macbook_preset(self):
        c = preset_macbook_us()
        assert c.total_insert_ms() == 95  # 80 base + 15 insert


# ── PMXT parser ──────────────────────────────────────────────────────


class TestPMXTParser:
    def test_url_format(self):
        d = datetime(2026, 4, 10, 14, tzinfo=timezone.utc)
        url = _archive_url_for_hour(d)
        assert url == "https://r2.pmxt.dev/polymarket_orderbook_2026-04-10T14.parquet"

    def test_parse_levels_list_form(self):
        raw = [["0.45", "100.0"], ["0.44", "200.0"]]
        levels = _parse_levels(raw)
        assert len(levels) == 2
        assert levels[0].price == 0.45
        assert levels[0].size == 100.0

    def test_parse_levels_dict_form(self):
        raw = [{"price": "0.45", "size": "100.0"}]
        levels = _parse_levels(raw)
        assert len(levels) == 1
        assert levels[0].price == 0.45

    def test_parse_book_snapshot(self):
        payload = {
            "market_id": "0xabc",
            "token_id": "12345",
            "side": "buy",
            "best_bid": "0.45",
            "best_ask": "0.47",
            "timestamp": 1710000000.123,
            "bids": [["0.45", "100"]],
            "asks": [["0.47", "120"]],
        }
        ev = _parse_event("book_snapshot", "0xabc", payload)
        assert ev is not None
        assert ev.update_type == "book_snapshot"
        assert ev.snapshot is not None
        assert ev.snapshot.token_id == "12345"
        assert ev.snapshot.best_bid == 0.45
        assert len(ev.snapshot.bids) == 1

    def test_parse_price_change(self):
        payload = {
            "market_id": "0xabc",
            "token_id": "12345",
            "side": "buy",
            "best_bid": "0.46",
            "best_ask": "0.48",
            "timestamp": 1710000001.456,
            "change_price": "0.46",
            "change_size": "25.0",
            "change_side": "buy",
        }
        ev = _parse_event("price_change", "0xabc", payload)
        assert ev is not None
        assert ev.update_type == "price_change"
        assert ev.change is not None
        assert ev.change.change_price == 0.46
        assert ev.change.change_size == 25.0

    def test_parse_string_payload(self):
        # Real PMXT files have JSON-encoded string payloads
        import json
        payload = json.dumps({
            "market_id": "0xabc",
            "token_id": "12345",
            "side": "buy",
            "best_bid": "0.45",
            "best_ask": "0.47",
            "timestamp": 1710000000.123,
            "bids": [["0.45", "100"]],
            "asks": [["0.47", "120"]],
        })
        ev = _parse_event("book_snapshot", "0xabc", payload)
        assert ev is not None
        assert ev.snapshot.best_bid == 0.45

    def test_invalid_json_returns_none(self):
        ev = _parse_event("book_snapshot", "0xabc", "not json")
        assert ev is None


# ── L2 replay engine ─────────────────────────────────────────────────


class TestTokenBook:
    def test_apply_snapshot(self):
        snap = BookSnapshot(
            market_id="0xabc",
            token_id="t1",
            side="buy",
            best_bid=0.45,
            best_ask=0.47,
            timestamp=1.0,
            bids=[L2Level(0.45, 100), L2Level(0.44, 200)],
            asks=[L2Level(0.47, 120), L2Level(0.48, 80)],
        )
        b = TokenBook(token_id="t1")
        b.apply_snapshot(snap)
        assert b.best_bid == 0.45
        assert b.best_ask == 0.47
        assert b.bids[0.45] == 100
        assert len(b.asks) == 2

    def test_apply_change_add_level(self):
        b = TokenBook(token_id="t1")
        chg = PriceChange(
            market_id="0xabc",
            token_id="t1",
            side="buy",
            best_bid=0.46,
            best_ask=0.48,
            timestamp=2.0,
            change_price=0.46,
            change_size=50.0,
            change_side="buy",
        )
        b.apply_change(chg)
        assert b.bids[0.46] == 50.0
        assert b.best_bid == 0.46

    def test_apply_change_remove_level(self):
        b = TokenBook(token_id="t1")
        b.bids = {0.45: 100, 0.44: 200}
        chg = PriceChange(
            market_id="0xabc",
            token_id="t1",
            side="buy",
            best_bid=0.44,
            best_ask=0.47,
            timestamp=2.0,
            change_price=0.45,
            change_size=0.0,
            change_side="buy",
        )
        b.apply_change(chg)
        assert 0.45 not in b.bids
        assert b.best_bid == 0.44


class TestL2BacktestEngine:
    def test_pending_orders_respect_latency(self):
        engine = L2BacktestEngine(
            latency=StaticLatencyConfig(base_latency_ms=100, insert_latency_ms=0),
        )
        # Set up a book
        engine._books["t1"] = TokenBook(
            token_id="t1",
            best_bid=0.45,
            best_ask=0.47,
            bids={0.45: 100},
            asks={0.47: 120},
            last_update_ts=10.0,
        )
        order = BacktestOrder(
            timestamp=10.0,
            token_id="t1",
            side="buy",
            size=10,
            order_type="market",
            fee_rate=0.072,
        )
        engine._pending_orders.append(order)

        # Flush at t=10.05 — only 50ms elapsed, latency is 100ms — no fill
        engine._flush_pending_orders(10.05)
        assert len(engine.fills) == 0
        assert len(engine._pending_orders) == 1

        # Flush at t=10.15 — 150ms elapsed, latency satisfied
        engine._flush_pending_orders(10.15)
        assert len(engine.fills) == 1
        assert engine.fills[0].success
        assert engine.fills[0].fill_price == pytest.approx(0.48, abs=1e-9)

    def test_zero_latency_fills_immediately(self):
        engine = L2BacktestEngine(latency=StaticLatencyConfig())
        engine._books["t1"] = TokenBook(
            token_id="t1",
            best_bid=0.45,
            best_ask=0.47,
            bids={0.45: 100},
            asks={0.47: 120},
            last_update_ts=10.0,
        )
        order = BacktestOrder(
            timestamp=10.0,
            token_id="t1",
            side="buy",
            size=10,
            order_type="market",
            fee_rate=0.072,
        )
        engine._pending_orders.append(order)
        engine._flush_pending_orders(10.0)
        assert len(engine.fills) == 1

    def test_fee_applied_to_fill(self):
        engine = L2BacktestEngine()
        engine._books["t1"] = TokenBook(
            token_id="t1",
            best_bid=0.49,
            best_ask=0.51,
            bids={0.49: 100},
            asks={0.51: 120},
            last_update_ts=10.0,
        )
        order = BacktestOrder(
            timestamp=10.0,
            token_id="t1",
            side="buy",
            size=100,
            order_type="market",
            fee_rate=0.072,
        )
        engine._pending_orders.append(order)
        engine._flush_pending_orders(10.0)

        fill = engine.fills[0]
        # Fill price = 0.51 + 0.01 = 0.52
        # fee = 100 * 0.072 * 0.52 * 0.48 = 1.79712
        assert fill.fee == pytest.approx(1.79712, abs=1e-4)
