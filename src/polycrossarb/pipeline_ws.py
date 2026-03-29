"""Event-driven pipeline using WebSocket for real-time arb detection.

Instead of polling every 30s, this pipeline:
  1. Fetches all markets once via REST (cold start)
  2. Subscribes to WebSocket for real-time price updates on arb candidates
  3. Re-evaluates arbs instantly when prices change
  4. Executes trades within seconds of detecting an opportunity
  5. Handles market resolutions to free capital immediately

Typical latency: <1 second from price change to trade decision
(vs ~200s for REST polling pipeline).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from pathlib import Path

import structlog

from polycrossarb.arb.detector import detect_cross_market_arbs
from polycrossarb.config import settings
from polycrossarb.data.client import PolymarketClient
from polycrossarb.data.models import Market, OrderBook
from polycrossarb.data.websocket import (
    MarketResolution,
    PolymarketWebSocket,
    PriceUpdate,
)
from polycrossarb.execution.executor import ExecutionMode, PaperExecutor
from polycrossarb.execution.fees import prefetch_fee_rates
from polycrossarb.graph.dependency import DependencyGraph
from polycrossarb.graph.screener import EventGroup, find_event_partitions
from polycrossarb.risk.manager import RiskManager
from polycrossarb.solver.linear import solve_partition_arb

log = structlog.get_logger(__name__)


class WebSocketPipeline:
    """Event-driven arbitrage pipeline using WebSocket."""

    def __init__(
        self,
        mode: ExecutionMode = ExecutionMode.PAPER,
        log_dir: str = "logs",
    ):
        self.mode = mode
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)

        self._client = PolymarketClient()
        self._ws = PolymarketWebSocket()
        self._risk = RiskManager()
        self._executor = PaperExecutor(self._risk)

        # Market state
        self._markets: dict[str, Market] = {}  # condition_id -> Market
        self._partitions: dict[str, EventGroup] = {}  # event_id -> partition
        self._asset_to_event: dict[str, str] = {}  # asset_id -> event_id
        self._asset_to_market: dict[str, str] = {}  # asset_id -> condition_id

        # Arb tracking
        self._active_arbs: dict[str, float] = {}  # event_id -> last detected margin
        self._trade_count = 0
        self._arb_checks = 0

        # Debounce: don't re-evaluate the same event within 1s
        self._last_eval: dict[str, float] = {}
        self._EVAL_DEBOUNCE = 1.0

        # Periodic refresh interval for REST data
        self._REFRESH_INTERVAL = 300  # 5 minutes
        self._last_refresh = 0.0

        self._running = False

    async def run(self) -> None:
        """Run the event-driven pipeline."""
        self._running = True

        log.info(
            "ws_pipeline.start",
            mode=self.mode,
            bankroll=settings.bankroll_usd,
        )

        # Step 1: Cold start — fetch all markets via REST
        await self._full_refresh()

        # Step 2: Set up WebSocket callbacks
        self._ws.on_price_change = self._on_price_change
        self._ws.on_book_update = self._on_book_update
        self._ws.on_resolution = self._on_resolution
        self._ws.set_asset_market_map(self._asset_to_market)

        # Step 3: Run WebSocket and periodic refresh concurrently
        try:
            await asyncio.gather(
                self._ws.run(),
                self._periodic_refresh(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self._ws.close()
            await self._client.close()
            self._running = False

            log.info(
                "ws_pipeline.stopped",
                trades=self._trade_count,
                arb_checks=self._arb_checks,
                pnl=f"${self._risk.total_pnl:.4f}",
            )

    def stop(self) -> None:
        self._running = False
        self._ws.stop()

    async def _full_refresh(self) -> None:
        """Fetch all markets, detect arbs, subscribe to WebSocket."""
        t0 = time.time()

        markets = await self._client.fetch_all_active_markets(min_liquidity=100)
        self._markets = {m.condition_id: m for m in markets}

        # Build partitions
        partitions = find_event_partitions(markets)
        neg_risk = [p for p in partitions if p.is_neg_risk]
        self._partitions = {p.event_id: p for p in neg_risk}

        # Detect current arbs
        opps = detect_cross_market_arbs(markets, exclusive_only=True)
        top_opps = sorted(opps, key=lambda o: o.margin, reverse=True)[:100]

        # Build asset -> event/market mapping
        self._asset_to_event.clear()
        self._asset_to_market.clear()
        arb_token_ids: list[str] = []

        for opp in top_opps:
            if not opp.markets:
                continue
            event_id = opp.markets[0].event_id
            for m in opp.markets:
                for o in m.outcomes:
                    if o.token_id:
                        self._asset_to_event[o.token_id] = event_id
                        self._asset_to_market[o.token_id] = m.condition_id
                        arb_token_ids.append(o.token_id)

        # Prefetch fees
        prefetch_fee_rates(arb_token_ids)

        # Subscribe to WebSocket
        self._ws.set_asset_market_map(self._asset_to_market)
        self._ws.subscribe(arb_token_ids)

        self._last_refresh = time.time()
        elapsed = time.time() - t0

        log.info(
            "ws_pipeline.refresh",
            markets=len(markets),
            partitions=len(neg_risk),
            arb_opps=len(opps),
            subscriptions=len(arb_token_ids),
            elapsed=f"{elapsed:.1f}s",
        )

    async def _periodic_refresh(self) -> None:
        """Periodically refresh market data via REST to catch new markets."""
        while self._running:
            await asyncio.sleep(self._REFRESH_INTERVAL)
            if self._running:
                try:
                    await self._full_refresh()
                except Exception:
                    log.exception("ws_pipeline.refresh_error")

    def _on_price_change(self, update: PriceUpdate) -> None:
        """Handle real-time price change — trigger arb re-evaluation."""
        event_id = self._asset_to_event.get(update.asset_id)
        if not event_id:
            return

        # Update market price in memory
        market = self._markets.get(update.market_condition_id)
        if market:
            for o in market.outcomes:
                if o.token_id == update.asset_id:
                    # Update with order book prices from WebSocket
                    book = self._ws.get_book(update.asset_id)
                    if book:
                        o.order_book = book
                    if update.best_bid is not None and update.best_ask is not None:
                        o.price = (update.best_bid + update.best_ask) / 2
                    break

        # Check early exits — if arb margin has shrunk, close positions to free capital
        current_prices = {
            f"{m.condition_id}:0": m.outcomes[0].price
            for m in self._markets.values()
            if m.outcomes and m.outcomes[0].price > 0
        }
        exits = self._risk.check_early_exits(current_prices)
        for key, exit_price in exits:
            pnl = self._risk.close_position(key, exit_price)
            if abs(pnl) > 0.001:
                log.info("ws_pipeline.early_exit", position=key[:20], pnl=f"${pnl:.4f}",
                         bankroll=f"${self._risk.effective_bankroll:.2f}")

        # Debounce: skip if we just evaluated this event
        now = time.time()
        if event_id in self._last_eval and now - self._last_eval[event_id] < self._EVAL_DEBOUNCE:
            return
        self._last_eval[event_id] = now

        # Evaluate arb for this partition
        partition = self._partitions.get(event_id)
        if partition:
            asyncio.get_event_loop().call_soon(
                lambda: asyncio.ensure_future(self._evaluate_arb(partition))
            )

    def _on_book_update(self, asset_id: str, book: OrderBook) -> None:
        """Handle order book snapshot — update cached market data."""
        cid = self._asset_to_market.get(asset_id)
        if not cid:
            return
        market = self._markets.get(cid)
        if market:
            for o in market.outcomes:
                if o.token_id == asset_id:
                    o.order_book = book
                    if book.mid_price is not None:
                        o.price = book.mid_price
                    break

    def _on_resolution(self, resolution: MarketResolution) -> None:
        """Handle market resolution — close positions and free capital."""
        log.info(
            "ws_pipeline.resolution",
            market=resolution.market_condition_id[:16],
            winner=resolution.winning_outcome,
        )

        # Close any positions in this market
        for key in list(self._risk._positions.keys()):
            if key.startswith(resolution.market_condition_id):
                if resolution.winning_asset_id and key.endswith(":0"):
                    # YES position resolved
                    exit_price = 1.0 if resolution.winning_outcome == "Yes" else 0.0
                else:
                    exit_price = 0.0
                pnl = self._risk.close_position(key, exit_price)
                if abs(pnl) > 0.001:
                    log.info("ws_pipeline.position_closed", key=key[:20], pnl=f"${pnl:.4f}")

    async def _evaluate_arb(self, partition: EventGroup) -> None:
        """Evaluate and potentially execute an arb on a single partition."""
        self._arb_checks += 1

        prices = partition.yes_prices
        if not prices or any(p <= 0 for p in prices):
            return

        price_sum = sum(prices)
        margin = abs(price_sum - 1.0)

        if margin < settings.min_arb_margin:
            return

        # Solve
        result = solve_partition_arb(
            partition,
            max_position_usd=min(
                settings.max_position_per_market_usd,
                self._risk.available_capital,
            ),
        )

        if not result.is_optimal or result.guaranteed_profit < settings.min_profit_usd:
            return

        # Risk check
        allowed, reason = self._risk.check_trade(result, partition.event_id)
        if not allowed:
            return

        # Execute
        trade_markets = [self._markets[o.market_condition_id]
                         for o in result.orders
                         if o.market_condition_id in self._markets]

        exec_result = self._executor.execute(result, trade_markets, partition.event_id)

        if exec_result.all_filled:
            self._trade_count += 1
            self._risk.record_fees(result.total_fees)

            log.info(
                "ws_pipeline.trade",
                event_name=partition.event_title[:40],
                legs=len(result.orders),
                gross=f"${result.gross_profit:.4f}",
                fees=f"${result.total_fees:.4f}",
                net=f"${result.guaranteed_profit:.4f}",
                bankroll=f"${self._risk.effective_bankroll:.2f}",
            )

            # Log trade
            self._log_trade(partition, result)

    def _log_trade(self, partition: EventGroup, result) -> None:
        """Write trade to log file."""
        entry = {
            "timestamp": time.time(),
            "event_id": partition.event_id,
            "event_title": partition.event_title,
            "n_legs": len(result.orders),
            "gross_profit": round(result.gross_profit, 6),
            "trading_fees": round(result.trading_fees, 6),
            "gas_fees": round(result.gas_fees, 6),
            "net_profit": round(result.guaranteed_profit, 6),
            "bankroll": round(self._risk.effective_bankroll, 2),
        }
        log_file = self.log_dir / "ws_trades.jsonl"
        with open(log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def status(self) -> dict:
        """Return current pipeline status."""
        return {
            "mode": "websocket",
            "execution": self.mode,
            "ws_stats": self._ws.stats,
            "arb_checks": self._arb_checks,
            "trade_count": self._trade_count,
            **self._risk.summary(),
        }
