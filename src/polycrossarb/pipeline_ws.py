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
from datetime import datetime, timezone
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
from polycrossarb.execution.executor import ExecutionMode, LiveExecutor, PaperExecutor
from polycrossarb.execution.fees import prefetch_fee_rates
from polycrossarb.graph.dependency import DependencyGraph
from polycrossarb.graph.screener import EventGroup, find_event_partitions
from polycrossarb.risk.manager import RiskManager
from polycrossarb.solver.linear import solve_partition_arb
from polycrossarb.solver.market_maker import evaluate_mm_opportunities, MMStrategy, MMQuote

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
        self._paper_executor = PaperExecutor(self._risk)
        self._live_executor = LiveExecutor(self._risk)

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

        # Lock per event to prevent duplicate concurrent evaluations
        self._eval_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        # Global execution lock — only ONE live trade in-flight at a time
        self._execution_lock = asyncio.Lock()

        # Periodic refresh interval for REST data
        self._REFRESH_INTERVAL = 300  # 5 minutes
        self._last_refresh = 0.0

        # Market-making state
        self._mm_strategies: list[MMStrategy] = []
        self._mm_active_orders: dict[str, str] = {}  # var_key -> order_id
        self._mm_last_refresh = 0.0
        self._MM_REFRESH_INTERVAL = 60  # refresh MM quotes every 60s

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

        # Step 3: Run WebSocket, periodic refresh, and MM quote manager concurrently
        try:
            await asyncio.gather(
                self._ws.run(),
                self._periodic_refresh(),
                self._mm_quote_manager(),
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

        # Debounce: skip if we just evaluated this event
        now = time.time()
        if event_id in self._last_eval and now - self._last_eval[event_id] < self._EVAL_DEBOUNCE:
            return
        self._last_eval[event_id] = now

        # Check early exits — only scan markets we have positions in (not all 25k)
        if self._risk.positions:
            pos_cids = {p.market_condition_id for p in self._risk.positions}
            current_prices = {
                f"{m.condition_id}:0": m.outcomes[0].price
                for m in self._markets.values()
                if m.condition_id in pos_cids and m.outcomes and m.outcomes[0].price > 0
            }
            exits = self._risk.check_early_exits(current_prices)
            for key, exit_price in exits:
                pnl = self._risk.close_position(key, exit_price)
                if abs(pnl) > 0.001:
                    log.info("ws_pipeline.early_exit", position=key[:20], pnl=f"${pnl:.4f}",
                             bankroll=f"${self._risk.effective_bankroll:.2f}")

        # Evaluate arb — use lock to prevent duplicate concurrent evaluations
        partition = self._partitions.get(event_id)
        if partition:
            asyncio.ensure_future(self._locked_evaluate(event_id, partition))

    async def _locked_evaluate(self, event_id: str, partition: EventGroup) -> None:
        """Serialize arb evaluation per event to prevent duplicate trades."""
        lock = self._eval_locks[event_id]
        if lock.locked():
            return  # another evaluation is already running for this event
        async with lock:
            await self._evaluate_arb(partition)

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
        for pos in list(self._risk.positions):
            key = f"{pos.market_condition_id}:{pos.outcome_idx}"
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

        max_days = settings.max_resolution_days
        for m in partition.markets:
            if m.end_date:
                try:
                    end = datetime.fromisoformat(m.end_date.replace("Z", "+00:00"))
                    days = (end - datetime.now(timezone.utc)).total_seconds() / 86400
                    if days > max_days:
                        return  # too far out, skip
                except (ValueError, TypeError):
                    pass

        # Check available capital before solving
        available = self._risk.available_capital
        if available < 1.0:
            return

        # Solve
        result = solve_partition_arb(
            partition,
            max_position_usd=min(self._risk.max_per_market, available),
        )

        if not result.is_optimal or result.guaranteed_profit < settings.min_profit_usd:
            return

        # Risk check
        allowed, reason = self._risk.check_trade(result, partition.event_id)
        if not allowed:
            return

        # Verify all token IDs are present before attempting live execution
        trade_markets = [self._markets[o.market_condition_id]
                         for o in result.orders
                         if o.market_condition_id in self._markets]

        if len(trade_markets) != len(result.orders):
            log.warning("Missing markets for some legs — skipping")
            return

        if self.mode == ExecutionMode.PAPER:
            exec_result = self._paper_executor.execute(result, trade_markets, partition.event_id)
        else:
            # Live mode: global lock ensures only ONE trade executes at a time
            # This prevents multiple arbs from racing for the same USDC balance
            if self._execution_lock.locked():
                return  # another trade is in-flight
            async with self._execution_lock:
                exec_result = await self._live_executor.execute(result, trade_markets, partition.event_id)

        if exec_result.all_filled:
            # Set cooldown ONLY after successful execution
            self._risk._last_trade_time[partition.event_id] = time.time()
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

    # ── Market-Making ──────────────────────────────────────────────

    async def _mm_quote_manager(self) -> None:
        """Periodically evaluate and place market-making quotes.

        Runs alongside the arb pipeline. Every 60s:
        1. Evaluate overpriced events for MM opportunities
        2. Cancel stale quotes
        3. Place new quotes (buy NO at patient prices)
        """
        while self._running:
            await asyncio.sleep(self._MM_REFRESH_INTERVAL)
            if not self._running:
                break
            try:
                await self._refresh_mm_quotes()
            except Exception:
                log.exception("mm.error")

    async def _refresh_mm_quotes(self) -> None:
        """Refresh market-making quotes on overpriced events."""
        # Get current overpriced partitions with order books
        partitions_with_books = []
        for eid, p in self._partitions.items():
            if not p.is_neg_risk:
                continue
            yes_sum = sum(p.yes_prices)
            if yes_sum <= 1.02:  # only MM on events with 2%+ edge
                continue
            # Check all markets have books
            if all(m.outcomes[0].order_book for m in p.markets if m.outcomes):
                partitions_with_books.append(p)

        if not partitions_with_books:
            return

        # Determine quote size based on available capital
        available = self._risk.available_capital
        if available < 5:
            return

        quote_size = min(15.0, available * 0.10)  # 10% of available per quote

        # Evaluate MM strategies
        strategies = evaluate_mm_opportunities(
            partitions_with_books,
            bankroll=self._risk.effective_bankroll,
            quote_size=quote_size,
        )

        if not strategies:
            return

        self._mm_strategies = strategies

        # Cancel existing MM orders
        if self._mm_active_orders:
            await self._cancel_mm_orders()

        # Place new quotes (paper mode logs, live mode places orders)
        placed = 0
        for strategy in strategies:
            for quote in strategy.buy_quotes:
                if self.mode == ExecutionMode.PAPER:
                    log.info(
                        "mm.quote.paper",
                        event_name=strategy.event_title[:30],
                        side=quote.side,
                        price=f"${quote.price:.3f}",
                        size=f"{quote.size:.0f}",
                        edge=f"{strategy.edge:.3f}",
                    )
                    placed += 1
                else:
                    order_id = await self._place_mm_order(quote)
                    if order_id:
                        self._mm_active_orders[quote.var_key] = order_id
                        placed += 1

        if placed > 0:
            total_cost = sum(q.price * q.size for s in strategies for q in s.buy_quotes)
            total_edge_profit = sum(s.expected_profit_if_complete for s in strategies)
            log.info(
                "mm.refresh",
                events=len(strategies),
                quotes=placed,
                cost=f"${total_cost:.2f}",
                edge_profit=f"${total_edge_profit:.2f}",
            )

    async def _place_mm_order(self, quote: MMQuote) -> str | None:
        """Place a single market-making limit order."""
        if not self._live_executor._ensure_client():
            return None

        try:
            from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions
            from py_clob_client.order_builder.constants import BUY, SELL

            # Get token_id for the NO outcome
            market = self._markets.get(quote.market_condition_id)
            if not market or len(market.outcomes) < 2:
                return None
            token_id = market.outcomes[quote.outcome_idx].token_id
            if not token_id:
                return None

            # Get book for neg_risk and tick_size
            book = self._live_executor._client.get_order_book(token_id)

            side = BUY if quote.side == "buy" else SELL
            args = OrderArgs(
                token_id=token_id,
                price=quote.price,
                size=quote.size,
                side=side,
            )
            opts = PartialCreateOrderOptions(
                tick_size=str(book.tick_size) if book.tick_size else "0.01",
                neg_risk=bool(book.neg_risk),
            )
            resp = self._live_executor._client.create_and_post_order(args, opts)
            order_id = resp.get("orderID") or resp.get("id") or ""
            if order_id:
                log.info("mm.order.placed", side=quote.side, price=f"${quote.price:.3f}",
                         size=f"{quote.size:.0f}", order_id=order_id[:16])
                return order_id
        except Exception as e:
            log.debug("mm.order.failed: %s", str(e)[:60])
        return None

    async def _cancel_mm_orders(self) -> None:
        """Cancel all active market-making orders."""
        if not self._live_executor._ensure_client():
            return
        for var_key, oid in list(self._mm_active_orders.items()):
            try:
                self._live_executor._client.cancel(oid)
            except Exception:
                pass
        self._mm_active_orders.clear()

    def status(self) -> dict:
        """Return current pipeline status."""
        return {
            "mode": "websocket",
            "execution": self.mode,
            "ws_stats": self._ws.stats,
            "arb_checks": self._arb_checks,
            "trade_count": self._trade_count,
            "mm_strategies": len(self._mm_strategies),
            "mm_active_orders": len(self._mm_active_orders),
            **self._risk.summary(),
        }
