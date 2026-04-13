"""Shared order dataclasses.

These used to live in ``polycrossarb.solver.linear`` alongside the
partition-arb LP solver, but the solver module has been removed.
The candle pipeline, executor, and risk manager still need these
types, so they moved here.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TradeOrder:
    """A single order to be placed."""
    market_condition_id: str
    outcome_idx: int  # 0=YES, 1=NO
    side: str  # "buy" or "sell"
    size: float  # number of shares/contracts
    price: float  # limit price
    expected_cost: float  # price * size (negative if selling)
    neg_risk: bool = False  # is this a neg_risk market?
    outcome_name: str = ""  # "Yes" or "No"

    @property
    def var_key(self) -> str:
        return f"{self.market_condition_id}:{self.outcome_idx}"

    @property
    def effective_usdc_cost(self) -> float:
        """Actual USDC cost. Simply price × size."""
        return abs(self.price * self.size)


@dataclass
class SolverResult:
    """Result of a trade solve.

    Historical: this used to be the output of the LP partition-arb solver.
    Now only used as a simple container for the candle pipeline's
    single-leg order tracking — most fields stay at their defaults.
    """
    status: str  # "optimal", "infeasible", "unbounded", etc.
    guaranteed_profit: float  # profit AFTER fees
    gross_profit: float = 0.0  # profit before fees
    orders: list[TradeOrder] = field(default_factory=list)
    total_cost: float = 0.0
    total_revenue: float = 0.0
    trading_fees: float = 0.0
    gas_fees: float = 0.0
    execution_strategy: str = "clob_only"

    @property
    def is_optimal(self) -> bool:
        return self.status == "optimal"

    @property
    def total_fees(self) -> float:
        return self.trading_fees + self.gas_fees
