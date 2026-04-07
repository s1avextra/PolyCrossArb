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
from polycrossarb.arb.detector import detect_single_market_orderbook_arbs
from polycrossarb.execution.executor import ExecutionMode, LiveExecutor, PaperExecutor, HybridExecutor
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
        self._hybrid_executor = HybridExecutor(self._risk)

        self._markets: list[Market] = []
        self._cycle_count = 0
        self._running = False

        # Enable on-chain execution in live mode for split/merge arbs
        if mode == ExecutionMode.LIVE and not settings.enable_onchain_execution:
            import os
            os.environ["ENABLE_ONCHAIN_EXECUTION"] = "true"
            from polycrossarb import config
            config.settings = config.Settings()

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

            # ── 3a. Also scan binary markets for merge/split (vidarx pattern)
            # Find 2-outcome markets with mid-price sum near 1.0 (potential arbs)
            binary_candidates = [
                m for m in priced
                if m.num_outcomes == 2
                and not m.closed and m.active
                and abs(m.outcome_price_sum - 1.0) < 0.10  # within 10% of 1.0
                and m.volume >= 100  # minimum activity
            ][:100]  # cap at 100 to limit API calls

            # Fetch order books for binary candidates
            if binary_candidates:
                await self._client.enrich_with_order_books(binary_candidates, concurrency=30)

            # Detect real book-level arbs on binary markets
            book_opps = detect_single_market_orderbook_arbs(binary_candidates, min_margin=0.01)

            all_opps = cross_opps + book_opps

            log.info(
                "cycle.detect",
                cycle=cycle_id,
                cross_opps=len(cross_opps),
                book_opps=len(book_opps),
                partitions=len(partitions),
                bankroll=f"${self._risk.effective_bankroll:.2f}",
            )

            if not all_opps:
                self._log_cycle(cycle_id, cycle_start, 0, 0)
                return

            # ── 3b. Enrich TOP cross-arb candidates with order books + fees
            top_opps = sorted(cross_opps, key=lambda o: o.margin, reverse=True)[:50]
            top_market_ids = {m.condition_id for opp in top_opps for m in opp.markets}
            arb_markets = [m for m in priced if m.condition_id in top_market_ids]

            if arb_markets:
                await self._client.enrich_with_order_books(arb_markets, concurrency=30)

            # Prefetch live fee rates for all candidates
            all_candidate_markets = arb_markets + binary_candidates
            prefetch_fee_rates([
                o.token_id for m in all_candidate_markets
                for o in m.outcomes if o.token_id
            ])

            # ── 4. Solve optimal positions (only arb partitions) ──
            # Only solve partitions that have detected arb opportunities
            arb_event_ids = {opp.markets[0].event_id for opp in top_opps if opp.markets}
            arb_partitions = [p for p in partitions if p.event_id in arb_event_ids]

            results = solve_all_partitions(
                partitions=arb_partitions,
                max_position_per_event=self._risk.max_per_market,
                max_total_exposure=self._risk.available_capital,
                min_profit=settings.min_profit_usd,
            )

            log.info("cycle.solve", cycle=cycle_id, profitable=len(results),
                     book_opps=len(book_opps))

            # ── 4b. Convert book arbs to SolverResults for unified execution
            from polycrossarb.solver.linear import SolverResult, TradeOrder
            from polycrossarb.execution.fees import calculate_taker_fee, estimate_gas_cost

            for opp in book_opps:
                if self._risk.available_capital < 1.0:
                    break

                market = opp.markets[0]
                orders = []
                total_cost = 0.0

                for idx, outcome in enumerate(market.outcomes):
                    if not outcome.order_book:
                        continue
                    price = outcome.order_book.best_ask if opp.arb_type == "single_under_book" else outcome.order_book.best_bid
                    if not price:
                        continue

                    # Size: use available capital, capped by depth
                    max_usd = min(self._risk.available_capital / max(market.num_outcomes, 1), self._risk.max_per_market)
                    shares = max_usd / price if price > 0 else 0
                    if shares * price < 1.0:
                        shares = 0
                        continue

                    side = "buy" if opp.arb_type == "single_under_book" else "sell"
                    orders.append(TradeOrder(
                        market_condition_id=market.condition_id,
                        outcome_idx=idx,
                        side=side,
                        size=shares,
                        price=price,
                        expected_cost=shares * price,
                        neg_risk=market.neg_risk,
                    ))
                    total_cost += shares * price

                if len(orders) == market.num_outcomes and total_cost >= 1.0:
                    fees = sum(calculate_taker_fee(o.size, o.price, market.category or "other") for o in orders)
                    gas = estimate_gas_cost(len(orders) + 1)
                    profit = opp.margin * (total_cost / max(sum(o.price for o in orders), 0.01)) - fees - gas

                    if profit >= settings.min_profit_usd:
                        book_result = SolverResult(
                            status="optimal",
                            guaranteed_profit=profit,
                            gross_profit=opp.margin * total_cost,
                            orders=orders,
                            total_cost=total_cost,
                            trading_fees=fees,
                            gas_fees=gas,
                            execution_strategy=opp.execution_strategy,
                        )
                        results.append(book_result)

            if not results:
                self._log_cycle(cycle_id, cycle_start, len(all_opps), 0)
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
                elif solver_result.execution_strategy == "split_then_sell":
                    condition_id = solver_result.orders[0].market_condition_id if solver_result.orders else ""
                    num_outcomes = len(solver_result.orders)
                    set_size = solver_result.total_cost
                    exec_result = await self._hybrid_executor.execute_split_then_sell(
                        condition_id, num_outcomes, set_size,
                        solver_result.orders, trade_markets,
                    )
                elif solver_result.execution_strategy == "buy_then_merge":
                    condition_id = solver_result.orders[0].market_condition_id if solver_result.orders else ""
                    num_outcomes = len(solver_result.orders)
                    set_size = solver_result.total_cost / max(num_outcomes, 1)
                    exec_result = await self._hybrid_executor.execute_buy_then_merge(
                        condition_id, num_outcomes, set_size,
                        solver_result.orders, trade_markets,
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
