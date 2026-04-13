"""Single-leg trade executor for the candle strategy.

Historical: this module used to contain PaperExecutor, LiveExecutor,
and HybridExecutor for the arb pipeline (partition-arb multi-leg,
split/merge on-chain, iceberg). Those are gone. Only the simplified
single-token executor survives — the candle pipeline buys one token
per signal and holds to resolution.

Usage:
    risk = RiskManager(initial_bankroll=5.0)
    executor = SingleLegExecutor(risk)
    result = await executor.execute_single(
        token_id="0x...",
        side="buy",
        price=0.42,
        size=2.4,
        event_id="0x...",
        prefer_maker=True,
    )
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum

from polycrossarb.execution.orders import TradeOrder
from polycrossarb.risk.manager import RiskManager

log = logging.getLogger(__name__)


class ExecutionMode(str, Enum):
    PAPER = "paper"
    LIVE = "live"


@dataclass
class SingleLegResult:
    """Result of a single-leg trade."""
    success: bool
    order_id: str = ""
    filled_size: float = 0.0
    fill_price: float = 0.0
    cost: float = 0.0
    fee: float = 0.0
    slippage: float = 0.0
    error: str = ""


class SingleLegExecutor:
    """Buy one token at a limit price, hold to resolution.

    Pre-flight: check balance, order book depth, minimum order.
    Execute: place limit order, poll for fill, record in RiskManager.

    Maker mode: when ``prefer_maker=True``, posts a GTC limit at
    best_ask - 1 tick (0% fee). Falls back to taker (FOK at best_ask
    + 1 tick, 2% fee) after ``MAKER_TIMEOUT_S`` if unfilled. Saves
    ~$0.09 per $5 trade.

    Concurrency: ``execute_single`` is serialized via ``asyncio.Lock``
    so concurrent callbacks cannot race the shared CLOB client.
    """

    FILL_TIMEOUT = 30
    FILL_POLL_INTERVAL = 2
    MIN_ORDER_USD = 1.0
    MIN_DEPTH_USD = 5.0
    MAKER_TIMEOUT_S = 3.0

    def __init__(self, risk_manager: RiskManager):
        import asyncio as _asyncio
        self.risk_manager = risk_manager
        self._client = None
        self._initialized = False
        self._lock = _asyncio.Lock()

    def _ensure_client(self) -> bool:
        """Lazy-init CLOB client, or re-init after connection failure."""
        if self._initialized and self._client is not None:
            return True

        from polycrossarb.config import settings
        if not settings.poly_api_key or not settings.private_key:
            log.error("SingleLeg: API keys not configured")
            self._initialized = True
            return False

        try:
            from eth_account import Account
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            funder = Account.from_key(settings.private_key).address
            self._client = ClobClient(
                host=settings.poly_base_url,
                key=settings.private_key,
                chain_id=137,
                signature_type=0,
                funder=funder,
            )
            self._client.set_api_creds(ApiCreds(
                api_key=settings.poly_api_key,
                api_secret=settings.poly_api_secret,
                api_passphrase=settings.poly_api_passphrase,
            ))
            self._initialized = True
            log.info("SingleLeg CLOB client initialized")
            return True
        except Exception:
            log.exception("SingleLeg: failed to init CLOB client")
            self._initialized = True
            self._client = None
            return False

    def _reset_client(self):
        """Force re-init on next use after connection errors."""
        self._client = None
        self._initialized = False

    async def execute_single(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        neg_risk: bool = True,
        event_id: str = "",
        prefer_maker: bool = False,
    ) -> SingleLegResult:
        """Execute a single-token trade. Serialized via self._lock."""
        async with self._lock:
            return await self._execute_single_locked(
                token_id, side, price, size, neg_risk, event_id, prefer_maker,
            )

    async def _execute_single_locked(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        neg_risk: bool,
        event_id: str,
        prefer_maker: bool = False,
    ) -> SingleLegResult:
        import asyncio

        if not self._ensure_client():
            return SingleLegResult(success=False, error="CLOB client not configured")

        # ── Pre-flight ────────────────────────────────────────────

        order_value = price * size
        if order_value < self.MIN_ORDER_USD:
            return SingleLegResult(success=False, error=f"Order ${order_value:.2f} < ${self.MIN_ORDER_USD} minimum")

        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
            bal_params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=0)
            bal_data = self._client.get_balance_allowance(bal_params)
            usdc_balance = int(bal_data.get("balance", 0)) / 1e6
            if order_value > usdc_balance * 0.95:
                return SingleLegResult(success=False, error=f"Order ${order_value:.2f} > balance ${usdc_balance:.2f}")
        except Exception as e:
            return SingleLegResult(success=False, error=f"Balance check failed: {e}")

        try:
            book = self._client.get_order_book(token_id)
            neg_risk = bool(book.neg_risk)
            tick_size = str(book.tick_size) if book.tick_size else "0.01"

            if not book.asks:
                return SingleLegResult(success=False, error="No asks — can't buy")
            ask_depth = sum(float(a.price) * float(a.size) for a in book.asks)
            if ask_depth < self.MIN_DEPTH_USD:
                return SingleLegResult(success=False, error=f"Ask depth ${ask_depth:.2f} < ${self.MIN_DEPTH_USD}")

            if not book.bids:
                return SingleLegResult(success=False, error="No bids — can't exit")
        except Exception as e:
            return SingleLegResult(success=False, error=f"Book check failed: {e}")

        # ── Compute limit price ─────────────────────────────────
        try:
            best_ask = float(book.asks[0].price)
            tick = float(tick_size)
        except (IndexError, ValueError):
            best_ask = price
            tick = 0.01

        if prefer_maker:
            maker_price = round((best_ask - tick) / tick) * tick
            maker_price = max(0.01, min(0.99, maker_price))
            live_price = maker_price
        else:
            live_price = min(best_ask + tick, price + tick * 2)
            live_price = round(live_price / tick) * tick
            live_price = max(0.01, min(0.99, live_price))

        # ── Place order ─────────────────────────────────────────

        try:
            from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions
            from py_clob_client.order_builder.constants import BUY

            args = OrderArgs(
                token_id=token_id,
                price=live_price,
                size=size,
                side=BUY,
            )
            opts = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)
            resp = self._client.create_and_post_order(args, opts)

            if not resp or (isinstance(resp, dict) and resp.get("error")):
                error_msg = resp.get("error", "unknown") if isinstance(resp, dict) else "empty"
                return SingleLegResult(success=False, error=f"Order rejected: {error_msg}")

            order_id = resp.get("orderID") or resp.get("id") or ""
            if not order_id:
                return SingleLegResult(success=False, error="No order ID returned")

            order_mode = "maker" if prefer_maker else "taker"
            log.info("SingleLeg: placed %s %s %.1f @ $%.4f id=%s",
                     order_mode, side, size, live_price, order_id[:16])

        except Exception as e:
            return SingleLegResult(success=False, error=f"Order failed: {str(e)[:80]}")

        # ── Wait for fill ──────────────────────────────────────

        filled_size = 0.0
        fill_price = live_price

        if prefer_maker:
            maker_deadline = time.time() + self.MAKER_TIMEOUT_S
            while time.time() < maker_deadline:
                await asyncio.sleep(0.5)
                try:
                    status = self._client.get_order(order_id)
                    matched = float(status.get("size_matched", 0))
                    if matched >= size * 0.95:
                        filled_size = matched
                        fill_price = self._extract_vwap(status, live_price)
                        log.info("SingleLeg: maker fill %.1f @ $%.4f (0%% fee)",
                                 filled_size, fill_price)
                        break
                except Exception:
                    pass

            if filled_size < size * 0.50:
                try:
                    self._client.cancel(order_id)
                except Exception:
                    pass
                log.info("SingleLeg: maker timeout, falling back to taker")
                taker_price = round((best_ask + tick) / tick) * tick
                taker_price = max(0.01, min(0.99, taker_price))
                try:
                    args = OrderArgs(
                        token_id=token_id,
                        price=taker_price,
                        size=size,
                        side=BUY,
                    )
                    resp = self._client.create_and_post_order(args, opts)
                    order_id = (resp or {}).get("orderID") or (resp or {}).get("id") or ""
                    live_price = taker_price
                except Exception as e:
                    return SingleLegResult(success=False,
                                          error=f"Taker fallback failed: {str(e)[:80]}")

        if filled_size < size * 0.50:
            deadline = time.time() + self.FILL_TIMEOUT
            last_matched = 0.0

            while time.time() < deadline:
                await asyncio.sleep(self.FILL_POLL_INTERVAL)
                try:
                    status = self._client.get_order(order_id)
                    matched = float(status.get("size_matched", 0))
                    last_matched = matched
                    if matched >= size * 0.95:
                        filled_size = matched
                        fill_price = self._extract_vwap(status, live_price)
                        break
                except Exception:
                    pass

            if filled_size == 0.0 and last_matched > 0:
                filled_size = last_matched

        if filled_size < size * 0.50:
            try:
                self._client.cancel(order_id)
            except Exception:
                log.warning("SingleLeg: failed to cancel %s", order_id[:16])

            if filled_size > 0:
                log.warning("SingleLeg: partial fill %.1f/%.1f — recording orphaned shares",
                            filled_size, size)
                self._record_fill(
                    token_id, event_id, side, filled_size, fill_price,
                    live_price, neg_risk,
                )

            return SingleLegResult(
                success=False, order_id=order_id,
                filled_size=filled_size, fill_price=fill_price,
                error=f"Timeout: {filled_size:.1f}/{size:.1f} filled",
            )

        # ── Record trade ────────────────────────────────────────

        return self._record_fill(
            token_id, event_id, side, filled_size, fill_price,
            live_price, neg_risk, order_id,
        )

    @staticmethod
    def _extract_vwap(status: dict, fallback_price: float) -> float:
        trades = status.get("associate_trades") or []
        if not trades or not isinstance(trades, list):
            return fallback_price
        total_value = 0.0
        total_qty = 0.0
        for t in trades:
            try:
                tp = float(t.get("price", 0))
                ts = float(t.get("size", 0))
                if tp > 0 and ts > 0:
                    total_value += tp * ts
                    total_qty += ts
            except (ValueError, TypeError):
                pass
        return total_value / total_qty if total_qty > 0 else fallback_price

    def _record_fill(
        self,
        token_id: str,
        event_id: str,
        side: str,
        filled_size: float,
        fill_price: float,
        limit_price: float,
        neg_risk: bool,
        order_id: str = "",
    ) -> SingleLegResult:
        slippage = abs(fill_price - limit_price)
        cost = fill_price * filled_size

        from polycrossarb.execution.fees import calculate_taker_fee
        try:
            fee = calculate_taker_fee(filled_size, fill_price)
        except Exception:
            fee = cost * 0.02

        trade_order = TradeOrder(
            market_condition_id=event_id or token_id[:20],
            outcome_idx=0,
            side=side,
            size=filled_size,
            price=fill_price,
            expected_cost=cost,
            neg_risk=neg_risk,
        )
        self.risk_manager.record_trade([trade_order], event_id=event_id, paper=False)
        self.risk_manager.record_fees(fee)

        log.info(
            "SingleLeg: FILLED %.1f @ $%.4f (slip $%.4f, cost $%.2f, fee $%.3f)",
            filled_size, fill_price, slippage, cost, fee,
        )

        return SingleLegResult(
            success=True,
            order_id=order_id,
            filled_size=filled_size,
            fill_price=fill_price,
            cost=cost,
            fee=fee,
            slippage=slippage,
        )
