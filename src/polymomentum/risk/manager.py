"""Risk management: position limits, exposure tracking, cooldowns.

State is persisted to SQLite so the bot can crash and restart without
losing track of open positions, P&L, or trade history. SQLite provides
ACID durability — no more corrupted JSON files.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from polymomentum.config import settings
from polymomentum.execution.orders import SolverResult, TradeOrder

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

    STATE_DB = "state.db"

    def __init__(
        self,
        initial_bankroll: float | None = None,
        exposure_ratio: float = 0.80,
        max_per_market: float = settings.max_position_per_market_usd,
        cooldown_seconds: float = settings.cooldown_seconds,
        state_dir: str = "logs",
    ):
        # Use config bankroll if set, otherwise auto-detect from wallet
        if initial_bankroll is None:
            initial_bankroll = settings.bankroll_usd
        if initial_bankroll <= 0:
            from polymomentum.execution.wallet import detect_bankroll
            initial_bankroll = detect_bankroll()
        self._initial_bankroll = initial_bankroll
        self._exposure_ratio = exposure_ratio
        self._max_per_market_ratio = 0.20  # 20% of bankroll per event
        self._max_per_market_override = max_per_market
        self.cooldown_seconds = cooldown_seconds

        self._state_dir = Path(state_dir)
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = self._state_dir / self.STATE_DB

        self._positions: dict[str, Position] = {}
        self._trade_history: list[TradeRecord] = []
        self._last_trade_time: dict[str, float] = {}
        self._total_pnl: float = 0.0
        self._total_fees_paid: float = 0.0

        # Initialize SQLite and restore state
        self._init_db()
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
    def max_per_market(self) -> float:
        """Dynamic per-event cap: 20% of effective bankroll."""
        return min(
            self.effective_bankroll * self._max_per_market_ratio,
            self._max_per_market_override,
        )

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
            self.record_trade_to_db(record)

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

    def check_early_exits(
        self,
        current_prices: dict[str, float],
        exit_threshold: float = 0.005,
    ) -> list[tuple[str, float]]:
        """Check positions that should be closed early to free capital.

        A position should exit early when the arb margin it was based on
        has shrunk below exit_threshold — holding it locks capital for a
        small/no reward. Better to free the capital for new arbs.

        Groups positions by event_id and checks if the partition's YES
        price sum has corrected back toward 1.0.

        Args:
            current_prices: {condition_id:outcome_idx -> current_price}
            exit_threshold: close if remaining margin < this (default 0.5%)

        Returns:
            List of (position_key, exit_price) to close.
        """
        # Group positions by event
        by_event: dict[str, list[tuple[str, Position]]] = {}
        for key, pos in self._positions.items():
            eid = pos.event_id or key
            by_event.setdefault(eid, []).append((key, pos))

        exits: list[tuple[str, float]] = []

        for event_id, positions in by_event.items():
            # Calculate current YES price sum for this event's positions
            price_sum = 0.0
            all_have_prices = True
            for key, pos in positions:
                current = current_prices.get(key)
                if current is None:
                    all_have_prices = False
                    break
                price_sum += current

            if not all_have_prices or len(positions) < 2:
                continue

            remaining_margin = abs(price_sum - 1.0)

            if remaining_margin < exit_threshold:
                # Arb has corrected — close all positions in this event
                for key, pos in positions:
                    current = current_prices.get(key, pos.entry_price)
                    exits.append((key, current))

        return exits

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

    # ── State persistence (SQLite) ──────────────────────────────

    def _init_db(self) -> None:
        """Create SQLite tables if they don't exist."""
        con = sqlite3.connect(self._db_path)
        con.execute("PRAGMA journal_mode=WAL")  # safe concurrent reads
        con.executescript("""
            CREATE TABLE IF NOT EXISTS state (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS positions (
                key                TEXT PRIMARY KEY,
                market_condition_id TEXT NOT NULL,
                outcome_idx        INTEGER NOT NULL,
                side               TEXT NOT NULL,
                size               REAL NOT NULL,
                entry_price        REAL NOT NULL,
                entry_time         REAL NOT NULL,
                event_id           TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS trades (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp           REAL NOT NULL,
                market_condition_id TEXT NOT NULL,
                outcome_idx         INTEGER NOT NULL,
                side                TEXT NOT NULL,
                size                REAL NOT NULL,
                price               REAL NOT NULL,
                cost                REAL NOT NULL,
                event_id            TEXT NOT NULL DEFAULT '',
                pnl                 REAL NOT NULL DEFAULT 0,
                paper               INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS cooldowns (
                event_id TEXT PRIMARY KEY,
                last_trade_time REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS paper_positions (
                contract_id TEXT PRIMARY KEY,
                payload     TEXT NOT NULL
            );
        """)
        con.close()

    # ── Generic key-value meta store (breaker state, kill flags, etc.) ──

    def set_meta(self, key: str, value: str) -> None:
        con = self._db()
        try:
            con.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                (key, value),
            )
            con.commit()
        finally:
            con.close()

    def get_meta(self, key: str, default: str = "") -> str:
        con = self._db()
        try:
            row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row[0] if row else default
        finally:
            con.close()

    def clear_meta(self, key: str) -> None:
        con = self._db()
        try:
            con.execute("DELETE FROM meta WHERE key=?", (key,))
            con.commit()
        finally:
            con.close()

    # ── Paper-position persistence (candle strategy) ──

    def save_paper_positions(self, positions: dict[str, dict]) -> None:
        """Replace stored paper positions with the given dict."""
        con = self._db()
        try:
            con.execute("BEGIN")
            con.execute("DELETE FROM paper_positions")
            for cid, pos in positions.items():
                con.execute(
                    "INSERT INTO paper_positions (contract_id, payload) VALUES (?, ?)",
                    (cid, json.dumps(pos)),
                )
            con.commit()
        except Exception:
            con.rollback()
            log.exception("Failed to save paper positions")
        finally:
            con.close()

    def load_paper_positions(self) -> dict[str, dict]:
        con = self._db()
        try:
            out: dict[str, dict] = {}
            for cid, payload in con.execute(
                "SELECT contract_id, payload FROM paper_positions"
            ):
                try:
                    out[cid] = json.loads(payload)
                except Exception:
                    continue
            return out
        finally:
            con.close()

    def _db(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path)

    def save_state(self) -> None:
        """Persist current state to SQLite. ACID — no corruption risk."""
        con = self._db()
        try:
            con.execute("BEGIN")

            # Upsert scalar state
            for k, v in [
                ("total_pnl", self._total_pnl),
                ("total_fees_paid", self._total_fees_paid),
                ("saved_at", time.time()),
            ]:
                con.execute(
                    "INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
                    (k, json.dumps(v)),
                )

            # Rebuild positions table
            con.execute("DELETE FROM positions")
            for key, pos in self._positions.items():
                con.execute(
                    "INSERT INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (key, pos.market_condition_id, pos.outcome_idx, pos.side,
                     pos.size, pos.entry_price, pos.entry_time, pos.event_id),
                )

            # Rebuild cooldowns table
            con.execute("DELETE FROM cooldowns")
            for eid, ts in self._last_trade_time.items():
                con.execute(
                    "INSERT INTO cooldowns VALUES (?, ?)", (eid, ts),
                )

            con.commit()
            log.debug("State saved: %d positions, pnl=$%.4f", len(self._positions), self._total_pnl)
        except Exception:
            con.rollback()
            log.exception("Failed to save state")
        finally:
            con.close()

    def _load_state(self) -> None:
        """Restore state from SQLite on startup.

        Also migrates from legacy state.json if it exists.
        """
        # Migrate from legacy JSON if DB is empty
        legacy_path = self._state_dir / "state.json"
        if legacy_path.exists():
            self._migrate_from_json(legacy_path)

        con = self._db()
        try:
            # Scalar state
            for row in con.execute("SELECT key, value FROM state"):
                k, v = row[0], json.loads(row[1])
                if k == "total_pnl":
                    self._total_pnl = v
                elif k == "total_fees_paid":
                    self._total_fees_paid = v

            # Positions
            for row in con.execute("SELECT * FROM positions"):
                key = row[0]
                self._positions[key] = Position(
                    market_condition_id=row[1],
                    outcome_idx=row[2],
                    side=row[3],
                    size=row[4],
                    entry_price=row[5],
                    entry_time=row[6],
                    event_id=row[7],
                )

            # Cooldowns
            for row in con.execute("SELECT event_id, last_trade_time FROM cooldowns"):
                self._last_trade_time[row[0]] = row[1]

            if self._positions or self._total_pnl != 0:
                log.info(
                    "State restored: %d positions, pnl=$%.4f, fees=$%.4f",
                    len(self._positions), self._total_pnl, self._total_fees_paid,
                )
            else:
                log.info("No saved state found — starting fresh")
        except Exception:
            log.exception("Failed to restore state — starting fresh")
            self._positions.clear()
            self._total_pnl = 0.0
            self._total_fees_paid = 0.0
        finally:
            con.close()

    def _migrate_from_json(self, json_path: Path) -> None:
        """One-time migration from legacy state.json to SQLite."""
        con = self._db()
        try:
            # Check if DB already has data
            row = con.execute("SELECT COUNT(*) FROM state").fetchone()
            if row and row[0] > 0:
                return  # already migrated

            with open(json_path) as f:
                state = json.load(f)

            con.execute("BEGIN")
            for k, v in [
                ("total_pnl", state.get("total_pnl", 0.0)),
                ("total_fees_paid", state.get("total_fees_paid", 0.0)),
                ("saved_at", state.get("saved_at", time.time())),
            ]:
                con.execute("INSERT OR REPLACE INTO state VALUES (?, ?)", (k, json.dumps(v)))

            for key, pos_data in state.get("positions", {}).items():
                con.execute(
                    "INSERT OR REPLACE INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (key, pos_data["market_condition_id"], pos_data["outcome_idx"],
                     pos_data["side"], pos_data["size"], pos_data["entry_price"],
                     pos_data["entry_time"], pos_data.get("event_id", "")),
                )

            for eid, ts in state.get("last_trade_time", {}).items():
                con.execute("INSERT OR REPLACE INTO cooldowns VALUES (?, ?)", (eid, ts))

            con.commit()
            log.info("Migrated state.json → state.db (%d positions)", len(state.get("positions", {})))

            # Rename old file so we don't re-migrate
            json_path.rename(json_path.with_suffix(".json.bak"))
        except Exception:
            con.rollback()
            log.exception("Failed to migrate state.json")
        finally:
            con.close()

    def record_trade_to_db(self, record: TradeRecord) -> None:
        """Append a trade to the persistent audit trail."""
        con = self._db()
        try:
            con.execute(
                "INSERT INTO trades (timestamp, market_condition_id, outcome_idx, "
                "side, size, price, cost, event_id, pnl, paper) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (record.timestamp, record.market_condition_id, record.outcome_idx,
                 record.side, record.size, record.price, record.cost,
                 record.event_id, record.pnl, int(record.paper)),
            )
            con.commit()
        except Exception:
            log.debug("Failed to write trade to DB")
        finally:
            con.close()
