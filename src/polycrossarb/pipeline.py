"""Main pipeline orchestrator: continuous arb scanning + paper trading loop.

Flow per cycle:
  1. Fetch/update market data
  2. Build dependency graph
  3. Detect arbitrage opportunities
  4. Solve optimal positions (LP)
  5. Check risk controls
  6. Execute (paper or live)
  7. Log results
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path

import structlog

from polycrossarb.arb.detector import detect_cross_market_arbs
from polycrossarb.execution.fees import prefetch_fee_rates
from polycrossarb.config import settings
from polycrossarb.data.client import PolymarketClient
from polycrossarb.data.models import Market
from polycrossarb.execution.executor import ExecutionMode, LiveExecutor, PaperExecutor
from polycrossarb.graph.dependency import DependencyGraph
from polycrossarb.risk.manager import RiskManager
from polycrossarb.solver.linear import solve_all_partitions

log = structlog.get_logger(__name__)


class Pipeline:
    """Main arbitrage pipeline."""

    def __init__(
        self,
        mode: ExecutionMode = ExecutionMode.PAPER,
        log_dir: str = "logs",
    ):
        self.mode = mode
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)

        self._client = PolymarketClient()
        self._risk = RiskManager()
        self._graph = DependencyGraph()
        self._paper_executor = PaperExecutor(self._risk)
        self._live_executor = LiveExecutor(self._risk)

        self._markets: list[Market] = []
        self._cycle_count = 0
        self._running = False

    async def run(self, max_cycles: int | None = None) -> None:
        """Run the pipeline loop."""
        self._running = True
        log.info(
            "pipeline.start",
            mode=self.mode,
            bankroll=settings.bankroll_usd,
            max_exposure=settings.max_total_exposure_usd,
            max_per_market=settings.max_position_per_market_usd,
            interval=settings.scan_interval_seconds,
        )

        try:
            while self._running:
                if max_cycles is not None and self._cycle_count >= max_cycles:
                    break

                await self._run_cycle()
                self._cycle_count += 1

                if max_cycles is not None and self._cycle_count >= max_cycles:
                    break

                await asyncio.sleep(settings.scan_interval_seconds)
        except asyncio.CancelledError:
            log.info("pipeline.cancelled")
        finally:
            await self._client.close()
            self._running = False
            log.info("pipeline.stopped", cycles=self._cycle_count, pnl=self._risk.total_pnl)

    def stop(self) -> None:
        self._running = False

    async def _run_cycle(self) -> None:
        """Execute one scan-detect-solve-execute cycle."""
        cycle_start = time.time()
        cycle_id = self._cycle_count + 1

        try:
            # ── 1. Fetch market data (min liquidity $100 to skip dead markets)
            self._markets = await self._client.fetch_all_active_markets(
                min_liquidity=100.0,
            )
            priced = self._markets  # already filtered by client

            log.info("cycle.data", cycle=cycle_id, markets=len(priced))

            # ── 2. Build dependency graph ─────────────────────────
            self._graph.build(priced)
            partitions = self._graph.neg_risk_partitions

            # ── 3. Detect arbitrage ───────────────────────────────
            cross_opps = detect_cross_market_arbs(priced, exclusive_only=True)

            log.info(
                "cycle.detect",
                cycle=cycle_id,
                cross_opps=len(cross_opps),
                partitions=len(partitions),
                bankroll=f"${self._risk.effective_bankroll:.2f}",
            )

            if not cross_opps:
                self._log_cycle(cycle_id, cycle_start, 0, 0)
                return

            # ── 3b. Enrich TOP arb candidates with order books + fees
            # Sort by margin, only fetch books for top 50 opportunities
            top_opps = sorted(cross_opps, key=lambda o: o.margin, reverse=True)[:50]
            top_market_ids = {m.condition_id for opp in top_opps for m in opp.markets}
            arb_markets = [m for m in priced if m.condition_id in top_market_ids]

            # Fetch order books for executable price accuracy
            await self._client.enrich_with_order_books(arb_markets, concurrency=30)

            # Prefetch live fee rates for candidates
            prefetch_fee_rates([
                o.token_id for m in arb_markets
                for o in m.outcomes if o.token_id
            ])

            # ── 4. Solve optimal positions (only arb partitions) ──
            # Only solve partitions that have detected arb opportunities
            arb_event_ids = {opp.markets[0].event_id for opp in top_opps if opp.markets}
            arb_partitions = [p for p in partitions if p.event_id in arb_event_ids]

            results = solve_all_partitions(
                partitions=arb_partitions,
                max_position_per_event=settings.max_position_per_market_usd,
                max_total_exposure=self._risk.available_capital,
                min_profit=settings.min_profit_usd,
            )

            log.info("cycle.solve", cycle=cycle_id, profitable=len(results))

            if not results:
                self._log_cycle(cycle_id, cycle_start, len(cross_opps), 0)
                return

            # ── 5 & 6. Risk check + Execute ───────────────────────
            executed = 0
            total_profit = 0.0

            for solver_result in results:
                # Find the event_id for this result
                event_id = ""
                if solver_result.orders:
                    cid = solver_result.orders[0].market_condition_id
                    for m in priced:
                        if m.condition_id == cid:
                            event_id = m.event_id
                            break

                # Find markets for execution context
                order_cids = {o.market_condition_id for o in solver_result.orders}
                trade_markets = [m for m in priced if m.condition_id in order_cids]

                if self.mode == ExecutionMode.PAPER:
                    exec_result = self._paper_executor.execute(
                        solver_result, trade_markets, event_id,
                    )
                else:
                    exec_result = await self._live_executor.execute(
                        solver_result, event_id,
                    )

                if exec_result.all_filled:
                    executed += 1
                    total_profit += exec_result.actual_profit_estimate
                    self._risk.record_fees(solver_result.total_fees)

            log.info(
                "cycle.execute",
                cycle=cycle_id,
                executed=executed,
                total_profit=f"${total_profit:.4f}",
            )

            # ── 7. Log results ────────────────────────────────────
            self._log_cycle(cycle_id, cycle_start, len(cross_opps), executed, total_profit)

        except Exception:
            log.exception("cycle.error", cycle=cycle_id)

    def _log_cycle(
        self,
        cycle_id: int,
        start_time: float,
        opps_found: int,
        trades_executed: int,
        profit: float = 0.0,
    ) -> None:
        """Write cycle results to log file."""
        elapsed = time.time() - start_time
        risk = self._risk.summary()
        entry = {
            "cycle": cycle_id,
            "timestamp": time.time(),
            "elapsed_s": round(elapsed, 2),
            "opportunities": opps_found,
            "trades": trades_executed,
            "profit": round(profit, 6),
            **risk,
        }

        log_file = self.log_dir / "cycles.jsonl"
        with open(log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")

        log.info(
            "cycle.complete",
            cycle=cycle_id,
            elapsed=f"{elapsed:.1f}s",
            opps=opps_found,
            trades=trades_executed,
            pnl=f"${self._risk.total_pnl:.4f}",
        )

    def status(self) -> dict:
        """Return current pipeline status."""
        return {
            "running": self._running,
            "mode": self.mode,
            "cycles": self._cycle_count,
            "markets_loaded": len(self._markets),
            **self._risk.summary(),
        }
