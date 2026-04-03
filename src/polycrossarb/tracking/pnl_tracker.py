"""P&L tracker: monitors positions and detects market resolutions.

Compares predicted profit (at trade time) to actual P&L (at resolution).
Writes resolved trades to logs/resolutions.jsonl.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import httpx

from polycrossarb.risk.manager import RiskManager

log = logging.getLogger(__name__)


@dataclass
class Resolution:
    """A resolved trade."""
    event_id: str
    market_condition_id: str
    outcome: str  # "yes" or "no"
    entry_price: float
    resolution_price: float  # 1.0 or 0.0
    size: float
    actual_pnl: float
    timestamp: float


class PnLTracker:
    """Monitors open positions and detects resolutions."""

    CHECK_INTERVAL = 300  # check every 5 minutes

    def __init__(self, risk_manager: RiskManager, log_dir: str = "logs"):
        self._risk = risk_manager
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(exist_ok=True)
        self._running = False
        self._resolutions: list[Resolution] = []

    async def run_loop(self) -> None:
        """Periodically check if positions have resolved."""
        import asyncio
        self._running = True
        while self._running:
            await asyncio.sleep(self.CHECK_INTERVAL)
            if not self._running:
                break
            try:
                await self._check_resolutions()
            except Exception:
                log.exception("pnl_tracker.error")

    def stop(self):
        self._running = False

    async def _check_resolutions(self) -> None:
        """Check if any open positions have resolved."""
        positions = self._risk.positions
        if not positions:
            return

        async with httpx.AsyncClient(timeout=10) as client:
            for pos in positions:
                key = f"{pos.market_condition_id}:{pos.outcome_idx}"
                try:
                    # Check if market is closed via Gamma API
                    resp = await client.get(
                        "https://gamma-api.polymarket.com/markets",
                        params={"condition_id": pos.market_condition_id, "limit": 1},
                    )
                    if resp.status_code != 200:
                        continue

                    markets = resp.json()
                    if not markets:
                        continue

                    market_data = markets[0]
                    if not market_data.get("closed", False):
                        continue

                    # Market is closed — determine resolution
                    resolution_price = 0.0
                    # If YES price is near 1.0, YES won
                    try:
                        import json as json_mod
                        prices = json_mod.loads(market_data.get("outcomePrices", "[]"))
                        if prices and float(prices[0]) > 0.90:
                            # YES won
                            resolution_price = 1.0 if pos.outcome_idx == 0 else 0.0
                        else:
                            # NO won
                            resolution_price = 0.0 if pos.outcome_idx == 0 else 1.0
                    except (ValueError, IndexError):
                        continue

                    # Close the position
                    pnl = self._risk.close_position(key, resolution_price)

                    resolution = Resolution(
                        event_id=pos.event_id,
                        market_condition_id=pos.market_condition_id,
                        outcome="yes" if pos.outcome_idx == 0 else "no",
                        entry_price=pos.entry_price,
                        resolution_price=resolution_price,
                        size=pos.size,
                        actual_pnl=pnl,
                        timestamp=time.time(),
                    )
                    self._resolutions.append(resolution)

                    log.info(
                        "pnl_tracker.resolved",
                        market=pos.market_condition_id[:16],
                        entry=f"${pos.entry_price:.4f}",
                        resolution=f"${resolution_price:.2f}",
                        pnl=f"${pnl:+.4f}",
                    )

                    # Write to log
                    log_file = self._log_dir / "resolutions.jsonl"
                    with open(log_file, "a") as f:
                        f.write(json.dumps({
                            "timestamp": resolution.timestamp,
                            "event_id": resolution.event_id,
                            "market": resolution.market_condition_id,
                            "outcome": resolution.outcome,
                            "entry_price": resolution.entry_price,
                            "resolution_price": resolution.resolution_price,
                            "size": resolution.size,
                            "pnl": round(pnl, 6),
                        }) + "\n")

                except Exception:
                    log.debug("pnl_tracker: error checking %s", key[:16], exc_info=True)

    @property
    def total_resolved_pnl(self) -> float:
        return sum(r.actual_pnl for r in self._resolutions)

    @property
    def n_resolved(self) -> int:
        return len(self._resolutions)

    @property
    def win_rate(self) -> float:
        if not self._resolutions:
            return 0.0
        wins = sum(1 for r in self._resolutions if r.actual_pnl > 0)
        return wins / len(self._resolutions)
