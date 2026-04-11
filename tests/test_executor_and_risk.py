"""Tests for SingleLegExecutor pre-flight, concurrency, and RiskManager.

These cover the live execution surface that previously had no test
coverage. We mock the CLOB client so no network or real wallet is touched.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

from polycrossarb.execution.executor import SingleLegExecutor, SingleLegResult
from polycrossarb.monitoring import alerter
from polycrossarb.risk.manager import RiskManager
from polycrossarb.solver.linear import SolverResult, TradeOrder


# ── Fakes ──────────────────────────────────────────────────────────


@dataclass
class FakeLevel:
    price: str
    size: str


@dataclass
class FakeBook:
    asks: list[FakeLevel]
    bids: list[FakeLevel]
    neg_risk: bool = False
    tick_size: str = "0.01"


class FakeClobClient:
    """Drop-in stand-in for py_clob_client.ClobClient."""

    def __init__(
        self,
        balance_usd: float = 50.0,
        book: FakeBook | None = None,
        order_id: str = "fake-order-id-1234567890abcdef",
        fill_size: float = 10.0,
        fail_balance: bool = False,
        fail_book: bool = False,
        fail_order: bool = False,
    ):
        self._balance = balance_usd
        self._book = book or FakeBook(
            asks=[FakeLevel("0.40", "100")],
            bids=[FakeLevel("0.39", "100")],
        )
        self._order_id = order_id
        self._fill_size = fill_size
        self._fail_balance = fail_balance
        self._fail_book = fail_book
        self._fail_order = fail_order
        self.calls: list[tuple[str, tuple]] = []

    def get_balance_allowance(self, _params):
        self.calls.append(("balance", ()))
        if self._fail_balance:
            raise RuntimeError("simulated balance failure")
        # py_clob_client returns USDC with 6 decimals as int string
        return {"balance": int(self._balance * 1e6)}

    def get_order_book(self, token_id):
        self.calls.append(("book", (token_id,)))
        if self._fail_book:
            raise RuntimeError("simulated book failure")
        return self._book

    def create_and_post_order(self, args, opts):
        self.calls.append(("order", (args.token_id, args.price, args.size)))
        if self._fail_order:
            return {"error": "simulated rejection"}
        return {"orderID": self._order_id}

    def get_order(self, oid):
        self.calls.append(("status", (oid,)))
        return {
            "size_matched": self._fill_size,
            "associate_trades": [{"price": "0.40", "size": str(self._fill_size)}],
        }

    def cancel(self, oid):
        self.calls.append(("cancel", (oid,)))


@pytest.fixture
def risk_manager(tmp_path) -> RiskManager:
    return RiskManager(
        initial_bankroll=100.0,
        exposure_ratio=0.80,
        max_per_market=20.0,
        cooldown_seconds=10.0,
        state_dir=str(tmp_path),
    )


@pytest.fixture
def fast_poll(monkeypatch):
    """Avoid the 2s fill-poll wait inside execute_single."""
    monkeypatch.setattr(SingleLegExecutor, "FILL_POLL_INTERVAL", 0.001)
    monkeypatch.setattr(SingleLegExecutor, "FILL_TIMEOUT", 0.5)


# ── SingleLegExecutor pre-flight ────────────────────────────────────


@pytest.mark.asyncio
async def test_executor_rejects_order_below_minimum(risk_manager):
    ex = SingleLegExecutor(risk_manager)
    # Stub _ensure_client so the real CLOB init never runs
    ex._initialized = True
    ex._client = FakeClobClient(balance_usd=50.0)
    res = await ex.execute_single(
        token_id="t1", side="buy", price=0.40, size=1.0, neg_risk=False, event_id="e1",
    )
    # 0.40 * 1.0 = $0.40 < $1 minimum
    assert res.success is False
    assert "minimum" in res.error.lower() or "< $1" in res.error


@pytest.mark.asyncio
async def test_executor_rejects_when_balance_insufficient(risk_manager, fast_poll):
    ex = SingleLegExecutor(risk_manager)
    ex._initialized = True
    ex._client = FakeClobClient(balance_usd=2.0)  # tiny wallet
    res = await ex.execute_single(
        token_id="t1", side="buy", price=0.40, size=20.0, neg_risk=False, event_id="e1",
    )
    # 0.40 * 20 = $8 > 95% of $2 balance
    assert res.success is False
    assert "balance" in res.error.lower()


@pytest.mark.asyncio
async def test_executor_rejects_thin_book(risk_manager, fast_poll):
    ex = SingleLegExecutor(risk_manager)
    ex._initialized = True
    # Book with $0.40 of ask depth — below $5 minimum
    thin_book = FakeBook(
        asks=[FakeLevel("0.40", "1")],
        bids=[FakeLevel("0.39", "1")],
    )
    ex._client = FakeClobClient(balance_usd=50.0, book=thin_book)
    res = await ex.execute_single(
        token_id="t1", side="buy", price=0.40, size=10.0, neg_risk=False, event_id="e1",
    )
    assert res.success is False
    assert "depth" in res.error.lower()


@pytest.mark.asyncio
async def test_executor_rejects_no_bids(risk_manager, fast_poll):
    ex = SingleLegExecutor(risk_manager)
    ex._initialized = True
    no_bids = FakeBook(
        asks=[FakeLevel("0.40", "100")],
        bids=[],
    )
    ex._client = FakeClobClient(balance_usd=50.0, book=no_bids)
    res = await ex.execute_single(
        token_id="t1", side="buy", price=0.40, size=10.0, neg_risk=False, event_id="e1",
    )
    assert res.success is False
    assert "exit" in res.error.lower() or "bids" in res.error.lower()


@pytest.mark.asyncio
async def test_executor_happy_path_records_fill(risk_manager, fast_poll):
    ex = SingleLegExecutor(risk_manager)
    ex._initialized = True
    ex._client = FakeClobClient(balance_usd=50.0, fill_size=10.0)
    res = await ex.execute_single(
        token_id="t1", side="buy", price=0.40, size=10.0, neg_risk=False, event_id="e1",
    )
    assert res.success, f"executor failed: {res.error}"
    assert res.filled_size == 10.0
    assert res.order_id  # order id round-tripped
    # Risk manager should have recorded the trade
    assert risk_manager.trade_count >= 1


# ── Concurrency lock ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_executor_serializes_concurrent_calls(risk_manager, fast_poll):
    """Two concurrent execute_single calls must run serially under the lock."""
    ex = SingleLegExecutor(risk_manager)
    ex._initialized = True
    ex._client = FakeClobClient(balance_usd=200.0, fill_size=10.0)

    in_flight = 0
    peak_in_flight = 0

    original_create = ex._client.create_and_post_order

    def tracking_create(args, opts):
        nonlocal in_flight, peak_in_flight
        in_flight += 1
        peak_in_flight = max(peak_in_flight, in_flight)
        try:
            return original_create(args, opts)
        finally:
            in_flight -= 1

    ex._client.create_and_post_order = tracking_create

    await asyncio.gather(
        ex.execute_single("t1", "buy", 0.40, 10.0, False, "e1"),
        ex.execute_single("t2", "buy", 0.40, 10.0, False, "e2"),
        ex.execute_single("t3", "buy", 0.40, 10.0, False, "e3"),
    )

    assert peak_in_flight == 1, (
        f"executor calls overlapped (peak {peak_in_flight}); "
        "the asyncio.Lock did not serialize execute_single"
    )


# ── RiskManager.check_trade ────────────────────────────────────────


def make_solver_result(profit: float = 0.50, cost: float = 5.0) -> SolverResult:
    """Build a minimal SolverResult that check_trade can evaluate."""
    order = TradeOrder(
        market_condition_id="cond1",
        outcome_idx=0,
        side="buy",
        size=10.0,
        price=0.40,
        expected_cost=cost,
        neg_risk=False,
    )
    return SolverResult(
        status="optimal",
        guaranteed_profit=profit,
        gross_profit=profit + 0.01,
        total_cost=cost,
        total_revenue=cost + profit,
        trading_fees=0.01,
        gas_fees=0.0,
        orders=[order],
    )


def test_risk_check_rejects_when_no_profit(risk_manager):
    res = make_solver_result(profit=0.0)
    ok, reason = risk_manager.check_trade(res, event_id="e1")
    assert not ok
    assert "profit" in reason


def test_risk_check_rejects_over_per_market_limit(risk_manager):
    # Per-market cap is min(20% × bankroll, max_per_market) = min($20, $20) = $20
    res = make_solver_result(profit=0.50, cost=25.0)
    ok, reason = risk_manager.check_trade(res, event_id="e1")
    assert not ok
    assert "per-market" in reason


def test_risk_check_happy_path(risk_manager):
    res = make_solver_result(profit=0.50, cost=5.0)
    ok, reason = risk_manager.check_trade(res, event_id="e1")
    assert ok, f"unexpected reject: {reason}"


def test_risk_check_cooldown_enforced(risk_manager):
    res = make_solver_result(profit=0.50, cost=5.0)
    # First trade allowed
    ok, _ = risk_manager.check_trade(res, event_id="evX")
    assert ok
    # Record it so the cooldown timestamp lands
    risk_manager.record_trade(res.orders, event_id="evX", paper=True)
    # Immediate retry should hit cooldown
    ok2, reason = risk_manager.check_trade(res, event_id="evX")
    assert not ok2
    assert "cooldown" in reason


# ── Meta + paper-position persistence ──────────────────────────────


def test_meta_round_trip(risk_manager):
    risk_manager.set_meta("candle_breaker_tripped", "1")
    assert risk_manager.get_meta("candle_breaker_tripped") == "1"
    risk_manager.clear_meta("candle_breaker_tripped")
    assert risk_manager.get_meta("candle_breaker_tripped") == ""


def test_paper_positions_persist_across_instances(tmp_path):
    rm1 = RiskManager(initial_bankroll=100.0, state_dir=str(tmp_path))
    positions = {
        "cond_abc": {
            "direction": "up",
            "entry_price": 0.42,
            "fee": 0.01,
            "size": 10.0,
            "open_btc": 60_000.0,
            "end_time": 1_700_000_000.0,
        },
    }
    rm1.save_paper_positions(positions)

    rm2 = RiskManager(initial_bankroll=100.0, state_dir=str(tmp_path))
    restored = rm2.load_paper_positions()
    assert restored == positions


# ── Alerter dry-run capture ────────────────────────────────────────


def test_alerter_required_raises_without_webhook(monkeypatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("ALERT_DRY_RUN", raising=False)
    with pytest.raises(alerter.AlerterNotConfigured):
        alerter.require_alerter_configured()


def test_alerter_required_passes_in_dry_run(monkeypatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("ALERT_DRY_RUN", "1")
    alerter.require_alerter_configured()  # must not raise


def test_alerter_dry_run_captures_alerts(monkeypatch):
    monkeypatch.setenv("ALERT_DRY_RUN", "1")
    alerter.reset_dry_run_buffer()
    # Force the cooldown to elapse for "circuit_breaker"
    alerter._last_alert.clear()
    alerter.alert_circuit_breaker(
        "candle", "test reason", {"win_rate": "70%", "drawdown": "$1.00", "trades": 21},
    )
    captured = alerter.get_dry_run_alerts()
    assert len(captured) == 1
    cat, msg = captured[0]
    assert cat == "circuit_breaker"
    assert "candle" in msg
