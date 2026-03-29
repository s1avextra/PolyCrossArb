"""Risk management: position limits, exposure tracking, cooldowns.

State is persisted to disk (state.json) so the bot can crash and
restart without losing track of open positions, P&L, or trade history.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from polycrossarb.config import settings
from polycrossarb.solver.linear import SolverResult, TradeOrder

log = logging.getLogger(__name__)


@dataclass
class Position:
    """A held position in a market outcome."""
    market_condition_id: str
    outcome_idx: int
    side: str  # "long" or "short"
    size: float
    entry_price: float
    entry_time: float
    event_id: str = ""

    @property
    def notional(self) -> float:
        return self.size * self.entry_price


@dataclass
class TradeRecord:
    """Record of an executed trade (paper or live)."""
    timestamp: float
    market_condition_id: str
    outcome_idx: int
    side: str
    size: float
    price: float
    cost: float
    event_id: str = ""
    pnl: float = 0.0
    paper: bool = True


class RiskManager:
    """Manages risk controls: dynamic bankroll, exposure, position tracking.

    Bankroll is dynamic:
      effective_bankroll = initial_bankroll + realized_pnl
      max_exposure = effective_bankroll * exposure_ratio

    This means profits compound and losses reduce exposure automatically.
    """

    STATE_FILE = "state.json"

    def __init__(
        self,
        initial_bankroll: float = settings.bankroll_usd,
        exposure_ratio: float = 0.80,
        max_per_market: float = settings.max_position_per_market_usd,
        cooldown_seconds: float = settings.cooldown_seconds,
        state_dir: str = "logs",
    ):
        self._initial_bankroll = initial_bankroll
        self._exposure_ratio = exposure_ratio
        self.max_per_market = max_per_market
        self.cooldown_seconds = cooldown_seconds
        self._state_path = Path(state_dir) / self.STATE_FILE

        self._positions: dict[str, Position] = {}
        self._trade_history: list[TradeRecord] = []
        self._last_trade_time: dict[str, float] = {}
        self._total_pnl: float = 0.0
        self._total_fees_paid: float = 0.0

        # Restore state from disk if available
        self._load_state()

    @property
    def effective_bankroll(self) -> float:
        """Bankroll adjusts with realized P&L — profits compound, losses shrink."""
        return max(0.0, self._initial_bankroll + self._total_pnl)

    @property
    def max_total_exposure(self) -> float:
        """Dynamic exposure limit based on current bankroll."""
        return self.effective_bankroll * self._exposure_ratio

    @property
    def total_exposure(self) -> float:
        return sum(p.notional for p in self._positions.values())

    @property
    def available_capital(self) -> float:
        return max(0.0, self.max_total_exposure - self.total_exposure)

    @property
    def total_pnl(self) -> float:
        return self._total_pnl

    @property
    def total_fees_paid(self) -> float:
        return self._total_fees_paid

    @property
    def trade_count(self) -> int:
        return len(self._trade_history)

    @property
    def positions(self) -> list[Position]:
        return list(self._positions.values())

    @property
    def trade_history(self) -> list[TradeRecord]:
        return list(self._trade_history)

    def check_trade(self, result: SolverResult, event_id: str = "") -> tuple[bool, str]:
        """Check if a trade passes all risk controls.

        Returns (allowed, reason).
        """
        if not result.is_optimal or result.guaranteed_profit <= 0:
            return False, "no profit"

        # Check cooldown
        if event_id and event_id in self._last_trade_time:
            elapsed = time.time() - self._last_trade_time[event_id]
            if elapsed < self.cooldown_seconds:
                return False, f"cooldown: {self.cooldown_seconds - elapsed:.0f}s remaining"

        # Check total exposure
        trade_cost = max(result.total_cost, result.total_revenue)
        if self.total_exposure + trade_cost > self.max_total_exposure:
            return False, f"exposure limit: {self.total_exposure:.2f} + {trade_cost:.2f} > {self.max_total_exposure:.2f}"

        # Check per-market limits
        for order in result.orders:
            key = order.var_key
            existing = self._positions.get(key)
            existing_notional = existing.notional if existing else 0
            if existing_notional + abs(order.expected_cost) > self.max_per_market:
                return False, f"per-market limit on {order.market_condition_id[:16]}"

        return True, "ok"

    def record_trade(
        self,
        orders: list[TradeOrder],
        event_id: str = "",
        paper: bool = True,
    ) -> None:
        """Record executed trades and update positions."""
        now = time.time()

        for order in orders:
            key = order.var_key

            record = TradeRecord(
                timestamp=now,
                market_condition_id=order.market_condition_id,
                outcome_idx=order.outcome_idx,
                side=order.side,
                size=order.size,
                price=order.price,
                cost=order.expected_cost,
                event_id=event_id,
                paper=paper,
            )
            self._trade_history.append(record)

            # Update position
            side = "long" if order.side == "buy" else "short"
            if key in self._positions:
                pos = self._positions[key]
                if pos.side == side:
                    # Add to position
                    total_notional = pos.notional + abs(order.expected_cost)
                    total_size = pos.size + order.size
                    pos.size = total_size
                    pos.entry_price = total_notional / total_size if total_size > 0 else 0
                else:
                    # Reduce/close position
                    pos.size -= order.size
                    if pos.size <= 0:
                        del self._positions[key]
            else:
                self._positions[key] = Position(
                    market_condition_id=order.market_condition_id,
                    outcome_idx=order.outcome_idx,
                    side=side,
                    size=order.size,
                    entry_price=order.price,
                    entry_time=now,
                    event_id=event_id,
                )

        if event_id:
            self._last_trade_time[event_id] = now

        self.save_state()

    def record_pnl(self, amount: float) -> None:
        """Record realised P&L — bankroll adjusts automatically."""
        self._total_pnl += amount
        self.save_state()

    def record_fees(self, amount: float) -> None:
        """Track cumulative fees paid."""
        self._total_fees_paid += amount

    def close_position(self, key: str, exit_price: float) -> float:
        """Close a position and return realised P&L."""
        pos = self._positions.pop(key, None)
        if pos is None:
            return 0.0

        if pos.side == "long":
            pnl = (exit_price - pos.entry_price) * pos.size
        else:
            pnl = (pos.entry_price - exit_price) * pos.size

        self._total_pnl += pnl
        self.save_state()
        return pnl

    def mark_to_market(self, current_prices: dict[str, float]) -> float:
        """Compute unrealized P&L based on current market prices.

        Args:
            current_prices: {condition_id:outcome_idx -> current_price}

        Returns:
            Total unrealized P&L.
        """
        unrealized = 0.0
        for key, pos in self._positions.items():
            current = current_prices.get(key, pos.entry_price)
            if pos.side == "long":
                unrealized += (current - pos.entry_price) * pos.size
            else:
                unrealized += (pos.entry_price - current) * pos.size
        return unrealized

    def summary(self) -> dict:
        """Return a summary of current risk state."""
        return {
            "initial_bankroll": self._initial_bankroll,
            "effective_bankroll": round(self.effective_bankroll, 2),
            "total_exposure": round(self.total_exposure, 2),
            "max_exposure": round(self.max_total_exposure, 2),
            "available_capital": round(self.available_capital, 2),
            "open_positions": len(self._positions),
            "total_trades": self.trade_count,
            "total_pnl": round(self._total_pnl, 4),
            "total_fees": round(self._total_fees_paid, 4),
        }

    # ── State persistence ─────────────────────────────────────────

    def save_state(self) -> None:
        """Persist current state to disk. Called after every trade."""
        state = {
            "version": 1,
            "saved_at": time.time(),
            "total_pnl": self._total_pnl,
            "total_fees_paid": self._total_fees_paid,
            "positions": {
                key: {
                    "market_condition_id": pos.market_condition_id,
                    "outcome_idx": pos.outcome_idx,
                    "side": pos.side,
                    "size": pos.size,
                    "entry_price": pos.entry_price,
                    "entry_time": pos.entry_time,
                    "event_id": pos.event_id,
                }
                for key, pos in self._positions.items()
            },
            "last_trade_time": self._last_trade_time,
            "trade_count": len(self._trade_history),
        }

        self._state_path.parent.mkdir(parents=True, exist_ok=True)

        # Atomic write: write to temp file then rename
        tmp = self._state_path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        tmp.rename(self._state_path)

        log.debug("State saved: %d positions, pnl=$%.4f", len(self._positions), self._total_pnl)

    def _load_state(self) -> None:
        """Restore state from disk on startup."""
        if not self._state_path.exists():
            log.info("No saved state found — starting fresh")
            return

        try:
            with open(self._state_path) as f:
                state = json.load(f)

            self._total_pnl = state.get("total_pnl", 0.0)
            self._total_fees_paid = state.get("total_fees_paid", 0.0)
            self._last_trade_time = state.get("last_trade_time", {})

            for key, pos_data in state.get("positions", {}).items():
                self._positions[key] = Position(
                    market_condition_id=pos_data["market_condition_id"],
                    outcome_idx=pos_data["outcome_idx"],
                    side=pos_data["side"],
                    size=pos_data["size"],
                    entry_price=pos_data["entry_price"],
                    entry_time=pos_data["entry_time"],
                    event_id=pos_data.get("event_id", ""),
                )

            saved_at = state.get("saved_at", 0)
            age = time.time() - saved_at if saved_at else 0
            log.info(
                "State restored: %d positions, pnl=$%.4f, fees=$%.4f (saved %.0fs ago)",
                len(self._positions), self._total_pnl, self._total_fees_paid, age,
            )
        except Exception:
            log.exception("Failed to restore state — starting fresh")
            self._positions.clear()
            self._total_pnl = 0.0
            self._total_fees_paid = 0.0
