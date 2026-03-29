"""Trade executor: paper trading and live execution.

Paper mode simulates fills against order book snapshots.
Live mode places orders via Polymarket CLOB API.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum

from polycrossarb.data.models import Market
from polycrossarb.risk.manager import RiskManager
from polycrossarb.solver.linear import SolverResult, TradeOrder

log = logging.getLogger(__name__)


class ExecutionMode(str, Enum):
    PAPER = "paper"
    LIVE = "live"


@dataclass
class FillResult:
    """Result of executing a single order."""
    order: TradeOrder
    filled_size: float
    fill_price: float  # actual fill price (may differ from order due to slippage)
    fill_cost: float
    slippage: float  # fill_price - order.price
    timestamp: float = 0.0
    success: bool = True
    error: str = ""


@dataclass
class ExecutionResult:
    """Result of executing all orders in a solver result."""
    fills: list[FillResult] = field(default_factory=list)
    total_cost: float = 0.0
    total_slippage: float = 0.0
    all_filled: bool = True
    guaranteed_profit: float = 0.0
    actual_profit_estimate: float = 0.0

    @property
    def n_filled(self) -> int:
        return sum(1 for f in self.fills if f.success)


class PaperExecutor:
    """Simulates trade execution against order book snapshots."""

    def __init__(self, risk_manager: RiskManager):
        self.risk_manager = risk_manager
        self._execution_log: list[ExecutionResult] = []

    @property
    def execution_log(self) -> list[ExecutionResult]:
        return list(self._execution_log)

    def execute(
        self,
        solver_result: SolverResult,
        markets: list[Market],
        event_id: str = "",
    ) -> ExecutionResult:
        """Paper-execute all orders from a solver result.

        Simulates fills using order book data if available,
        otherwise assumes fill at the order price.
        """
        # Check risk controls
        allowed, reason = self.risk_manager.check_trade(solver_result, event_id)
        if not allowed:
            log.info("Trade blocked by risk: %s", reason)
            return ExecutionResult(all_filled=False)

        market_lookup = {m.condition_id: m for m in markets}
        fills: list[FillResult] = []
        total_cost = 0.0
        total_slippage = 0.0
        all_filled = True
        now = time.time()

        for order in solver_result.orders:
            market = market_lookup.get(order.market_condition_id)

            fill = self._simulate_fill(order, market, now)
            fills.append(fill)

            if fill.success:
                total_cost += fill.fill_cost
                total_slippage += abs(fill.slippage) * fill.filled_size
            else:
                all_filled = False

        if all_filled:
            # Record trades in risk manager
            self.risk_manager.record_trade(
                solver_result.orders, event_id=event_id, paper=True,
            )

        actual_profit = solver_result.guaranteed_profit - total_slippage

        result = ExecutionResult(
            fills=fills,
            total_cost=total_cost,
            total_slippage=total_slippage,
            all_filled=all_filled,
            guaranteed_profit=solver_result.guaranteed_profit,
            actual_profit_estimate=actual_profit,
        )
        self._execution_log.append(result)

        log.info(
            "Paper execution: %d/%d filled, gross $%.4f, fees $%.4f, slippage $%.4f, net $%.4f",
            result.n_filled, len(fills),
            solver_result.gross_profit, solver_result.total_fees,
            total_slippage, actual_profit,
        )
        return result

    def _simulate_fill(
        self,
        order: TradeOrder,
        market: Market | None,
        timestamp: float,
    ) -> FillResult:
        """Simulate a single order fill."""
        # Try to use order book for realistic fill
        if market and order.outcome_idx < len(market.outcomes):
            outcome = market.outcomes[order.outcome_idx]
            book = outcome.order_book
            if book:
                side = "bid" if order.side == "sell" else "ask"
                vwap = book.vwap(side, order.size)
                if vwap is not None:
                    slippage = abs(vwap - order.price)
                    return FillResult(
                        order=order,
                        filled_size=order.size,
                        fill_price=vwap,
                        fill_cost=vwap * order.size * (-1 if order.side == "sell" else 1),
                        slippage=slippage,
                        timestamp=timestamp,
                        success=True,
                    )

        # Fall back to order price (optimistic)
        return FillResult(
            order=order,
            filled_size=order.size,
            fill_price=order.price,
            fill_cost=order.expected_cost,
            slippage=0.0,
            timestamp=timestamp,
            success=True,
        )


class LiveExecutor:
    """Executes trades via Polymarket CLOB API.

    Leg-by-leg execution with abort: if any leg fails to fill within
    the timeout, all remaining/unfilled orders are cancelled.

    Requires: POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE, PRIVATE_KEY
    """

    FILL_TIMEOUT = 30  # seconds to wait for each leg to fill
    FILL_POLL_INTERVAL = 2  # seconds between fill status checks

    def __init__(self, risk_manager: RiskManager):
        self.risk_manager = risk_manager
        self._client = None
        self._initialized = False

    def _ensure_client(self):
        """Lazy-init the CLOB client on first use."""
        if self._initialized:
            return self._client is not None

        from polycrossarb.config import settings
        if not settings.poly_api_key or not settings.private_key:
            log.error("Live execution not configured — set API keys in .env")
            self._initialized = True
            return False

        try:
            from py_clob_client.client import ClobClient

            self._client = ClobClient(
                host=settings.poly_base_url,
                key=settings.private_key,
                chain_id=137,
                signature_type=0,  # EOA wallet
                funder=None,  # derived from private key
            )
            # Set API credentials
            from py_clob_client.clob_types import ApiCreds
            self._client.set_api_creds(ApiCreds(
                api_key=settings.poly_api_key,
                api_secret=settings.poly_api_secret,
                api_passphrase=settings.poly_api_passphrase,
            ))
            self._initialized = True
            log.info("Live CLOB client initialized")
            return True
        except Exception:
            log.exception("Failed to initialize CLOB client")
            self._initialized = True
            return False

    async def execute(
        self,
        solver_result: SolverResult,
        markets: list | None = None,
        event_id: str = "",
    ) -> ExecutionResult:
        """Execute trades via Polymarket CLOB API with leg-by-leg abort.

        Strategy:
          1. Check risk controls
          2. Place each leg as a limit order
          3. Wait for fill (with timeout per leg)
          4. If any leg fails → cancel all unfilled orders
          5. Record fills in risk manager
        """
        allowed, reason = self.risk_manager.check_trade(solver_result, event_id)
        if not allowed:
            log.warning("Live trade blocked: %s", reason)
            return ExecutionResult(all_filled=False)

        if not self._ensure_client():
            return ExecutionResult(all_filled=False)

        fills: list[FillResult] = []
        placed_order_ids: list[str] = []
        all_filled = True
        total_cost = 0.0
        total_slippage = 0.0
        now = time.time()

        # Build token_id lookup from markets
        token_lookup: dict[str, str] = {}
        if markets:
            for m in markets:
                for o in m.outcomes:
                    key = f"{m.condition_id}:{m.outcomes.index(o)}"
                    token_lookup[key] = o.token_id

        for order in solver_result.orders:
            token_id = token_lookup.get(order.var_key, "")
            if not token_id:
                log.error("No token_id for %s — aborting", order.var_key)
                all_filled = False
                break

            try:
                fill = await self._place_and_wait(order, token_id, now)
                fills.append(fill)

                if fill.success:
                    total_cost += fill.fill_cost
                    total_slippage += abs(fill.slippage) * fill.filled_size
                    placed_order_ids.append(getattr(fill, '_order_id', ''))
                else:
                    all_filled = False
                    log.warning("Leg failed: %s — aborting remaining", order.var_key)
                    break

            except Exception:
                log.exception("Order placement failed for %s", order.var_key)
                all_filled = False
                break

        # If any leg failed, cancel all placed orders
        if not all_filled and placed_order_ids:
            await self._cancel_orders(placed_order_ids)

        if all_filled:
            self.risk_manager.record_trade(
                solver_result.orders, event_id=event_id, paper=False,
            )

        actual_profit = solver_result.guaranteed_profit - total_slippage

        result = ExecutionResult(
            fills=fills,
            total_cost=total_cost,
            total_slippage=total_slippage,
            all_filled=all_filled,
            guaranteed_profit=solver_result.guaranteed_profit,
            actual_profit_estimate=actual_profit,
        )

        log.info(
            "Live execution: %d/%d filled, gross $%.4f, fees $%.4f, net $%.4f",
            result.n_filled, len(solver_result.orders),
            solver_result.gross_profit, solver_result.total_fees, actual_profit,
        )
        return result

    async def _place_and_wait(
        self,
        order: TradeOrder,
        token_id: str,
        timestamp: float,
    ) -> FillResult:
        """Place a single limit order and wait for fill."""
        import asyncio

        try:
            from py_clob_client.clob_types import OrderArgs
            from py_clob_client.order_builder.constants import BUY, SELL

            side = BUY if order.side == "buy" else SELL
            order_args = OrderArgs(
                token_id=token_id,
                price=order.price,
                size=order.size,
                side=side,
            )
            resp = self._client.create_and_post_order(order_args)

            # Validate response — abort if order wasn't placed
            if not resp or isinstance(resp, dict) and resp.get("error"):
                error_msg = resp.get("error", "unknown") if isinstance(resp, dict) else "empty response"
                log.error("Order placement failed: %s", error_msg)
                return FillResult(
                    order=order, filled_size=0, fill_price=order.price,
                    fill_cost=0, slippage=0, timestamp=timestamp,
                    success=False, error=f"Placement failed: {error_msg}",
                )

            order_id = resp.get("orderID") or resp.get("id") or ""
            if not order_id:
                log.error("No order ID returned — cannot track order")
                return FillResult(
                    order=order, filled_size=0, fill_price=order.price,
                    fill_cost=0, slippage=0, timestamp=timestamp,
                    success=False, error="No order ID in response",
                )

            log.info("Order placed: %s %s %.2f@%.4f id=%s",
                     order.side, token_id[:16], order.size, order.price, order_id[:16])

            # Poll for fill — require 95%+ fill for success
            filled_size = 0.0
            fill_price = order.price
            poll_failures = 0
            deadline = time.time() + self.FILL_TIMEOUT

            while time.time() < deadline:
                await asyncio.sleep(self.FILL_POLL_INTERVAL)
                try:
                    status = self._client.get_order(order_id)
                    filled = float(status.get("size_matched", 0))
                    if filled >= order.size * 0.95:
                        filled_size = filled
                        # Safely extract fill price
                        trades = status.get("associate_trades") or []
                        if trades and isinstance(trades, list) and len(trades) > 0:
                            try:
                                fill_price = float(trades[0].get("price", order.price))
                            except (ValueError, TypeError, AttributeError):
                                fill_price = order.price
                        break
                except Exception as e:
                    poll_failures += 1
                    log.debug("Poll failed (%d): %s", poll_failures, e)
                    if poll_failures >= 5:
                        log.warning("Too many poll failures — treating as timeout")
                        break

            if filled_size < order.size * 0.95:
                # Not enough filled — cancel and report failure
                try:
                    self._client.cancel(order_id)
                    log.info("Cancelled unfilled order %s", order_id[:16])
                except Exception:
                    log.warning("Failed to cancel order %s — MAY BE ORPHANED", order_id[:16])

                return FillResult(
                    order=order, filled_size=filled_size,
                    fill_price=fill_price, fill_cost=0,
                    slippage=0, timestamp=timestamp, success=False,
                    error=f"Insufficient fill: {filled_size:.2f}/{order.size:.2f}",
                )

            slippage = abs(fill_price - order.price)
            cost = fill_price * filled_size * (-1 if order.side == "sell" else 1)

            result = FillResult(
                order=order, filled_size=filled_size,
                fill_price=fill_price, fill_cost=cost,
                slippage=slippage, timestamp=timestamp, success=True,
            )
            result._order_id = order_id  # type: ignore[attr-defined]
            return result

        except Exception as e:
            log.exception("Order execution error for %s", order.var_key)
            return FillResult(
                order=order, filled_size=0, fill_price=order.price,
                fill_cost=0, slippage=0, timestamp=timestamp,
                success=False, error=str(e),
            )

    async def _cancel_orders(self, order_ids: list[str]) -> None:
        """Cancel all unfilled orders (emergency abort)."""
        for oid in order_ids:
            if not oid:
                continue
            try:
                self._client.cancel(oid)
                log.info("Cancelled order %s", oid[:16])
            except Exception:
                log.warning("FAILED to cancel order %s — CHECK EXCHANGE MANUALLY", oid[:16])
