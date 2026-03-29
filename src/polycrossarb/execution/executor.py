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

    Requires API credentials and wallet setup.
    Currently a stub — activate after paper trading validation.
    """

    def __init__(self, risk_manager: RiskManager):
        self.risk_manager = risk_manager
        self._client = None  # py_clob_client instance

    def is_configured(self) -> bool:
        """Check if live execution is properly configured."""
        from polycrossarb.config import settings
        return bool(settings.poly_api_key and settings.private_key)

    async def execute(
        self,
        solver_result: SolverResult,
        event_id: str = "",
    ) -> ExecutionResult:
        """Execute trades via Polymarket CLOB API.

        Steps:
        1. Check risk controls
        2. Place limit orders at computed prices
        3. Monitor fills (with timeout)
        4. Handle partial fills
        """
        allowed, reason = self.risk_manager.check_trade(solver_result, event_id)
        if not allowed:
            log.warning("Live trade blocked: %s", reason)
            return ExecutionResult(all_filled=False)

        if not self.is_configured():
            log.error("Live execution not configured — set API keys and private key")
            return ExecutionResult(all_filled=False)

        # TODO: Implement actual CLOB order placement
        # 1. Create py_clob_client with API keys
        # 2. For each order: create_order(token_id, side, price, size)
        # 3. Monitor fill status
        # 4. Cancel unfilled legs if any leg fails (risk mitigation)
        log.warning("Live execution not yet implemented — use paper mode")
        return ExecutionResult(all_filled=False)
