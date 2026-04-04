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
    """Realistic paper trading — mirrors ALL live execution constraints.

    Simulates:
      - 200ms API latency (price staleness)
      - Market impact (adverse selection penalty on VWAP)
      - Fee deduction from balance
      - Partial fill rejection
      - Order book depth consumption

    Paper mode must reject the same trades that live would reject.
    Otherwise paper P&L is fiction and can't predict live performance.
    """

    POLYMARKET_MIN_ORDER_USD = 1.0
    LATENCY_PENALTY_MS = 200       # simulated API round-trip delay
    MARKET_IMPACT_BPS = 30         # 30 basis points adverse selection
    FILL_DEGRADATION = 0.02        # 2% worse fill than theoretical VWAP

    def __init__(self, risk_manager: RiskManager):
        self.risk_manager = risk_manager
        self._execution_log: list[ExecutionResult] = []
        self._simulated_balance: float | None = None

    @property
    def execution_log(self) -> list[ExecutionResult]:
        return list(self._execution_log)

    def execute(
        self,
        solver_result: SolverResult,
        markets: list[Market],
        event_id: str = "",
    ) -> ExecutionResult:
        """Paper-execute with same constraints as live.

        ALL-OR-NOTHING: same as LiveExecutor.
          1. Pre-flight: balance, $1 min, bid liquidity, order book depth
          2. Simulate all fills against order books
          3. If ANY leg can't fill → reject entire trade
        """
        # Check risk controls
        allowed, reason = self.risk_manager.check_trade(solver_result, event_id)
        if not allowed:
            log.info("Trade blocked by risk: %s", reason)
            return ExecutionResult(all_filled=False)

        market_lookup = {m.condition_id: m for m in markets}
        now = time.time()

        # Initialize simulated balance from wallet on first use
        if self._simulated_balance is None:
            self._simulated_balance = self.risk_manager.effective_bankroll

        # ── PRE-FLIGHT (same checks as live) ──────────────────────

        # 1. Total EFFECTIVE cost must fit in simulated balance
        total_order_cost = sum(o.effective_usdc_cost for o in solver_result.orders)
        if total_order_cost > self._simulated_balance * 0.95:
            log.info("Paper pre-flight: cost $%.2f > balance $%.2f — rejected",
                     total_order_cost, self._simulated_balance)
            return ExecutionResult(all_filled=False)

        # 2. Check each leg
        for order in solver_result.orders:
            # Minimum order size
            order_value = order.effective_usdc_cost
            if order_value < self.POLYMARKET_MIN_ORDER_USD:
                log.info("Paper pre-flight: leg $%.2f < $1 min — rejected", order_value)
                return ExecutionResult(all_filled=False)

            # Must have order book with bids (exitable)
            market = market_lookup.get(order.market_condition_id)
            if market and order.outcome_idx < len(market.outcomes):
                book = market.outcomes[order.outcome_idx].order_book
                if book is not None and not book.bids:
                    log.info("Paper pre-flight: no bids for %s — rejected", order.var_key[:20])
                    return ExecutionResult(all_filled=False)

        # ── SIMULATE ALL FILLS ────────────────────────────────────

        fills: list[FillResult] = []
        total_cost = 0.0
        total_slippage = 0.0
        all_filled = True

        for order in solver_result.orders:
            market = market_lookup.get(order.market_condition_id)
            fill = self._simulate_fill(order, market, now)
            fills.append(fill)

            if fill.success:
                total_cost += fill.fill_cost
                total_slippage += abs(fill.slippage) * fill.filled_size
            else:
                all_filled = False

        # ALL-OR-NOTHING: if any leg fails, reject entire trade
        if not all_filled:
            log.info("Paper: %d/%d legs failed — entire trade rejected",
                     sum(1 for f in fills if not f.success), len(fills))
            return ExecutionResult(all_filled=False)

        # Record trade and update simulated balance (including fees)
        self.risk_manager.record_trade(
            solver_result.orders, event_id=event_id, paper=True,
        )
        fee_cost = solver_result.total_fees
        self._simulated_balance -= (total_order_cost + fee_cost)

        actual_profit = solver_result.guaranteed_profit - total_slippage

        result = ExecutionResult(
            fills=fills,
            total_cost=total_cost,
            total_slippage=total_slippage,
            all_filled=True,
            guaranteed_profit=solver_result.guaranteed_profit,
            actual_profit_estimate=actual_profit,
        )
        self._execution_log.append(result)

        log.info(
            "Paper ALL %d legs filled: gross $%.4f, fees $%.4f, slippage $%.4f, net $%.4f, balance $%.2f",
            len(fills), solver_result.gross_profit, solver_result.total_fees,
            total_slippage, actual_profit, self._simulated_balance,
        )
        return result

    def _simulate_fill(
        self,
        order: TradeOrder,
        market: Market | None,
        timestamp: float,
    ) -> FillResult:
        """Simulate a single order fill with realistic degradation.

        Applies:
          - Market impact: worsen VWAP by MARKET_IMPACT_BPS
          - Fill degradation: 2% worse than theoretical
          - Depth check: reject if < 50% of order size available
        """
        if market and order.outcome_idx < len(market.outcomes):
            outcome = market.outcomes[order.outcome_idx]
            book = outcome.order_book
            if book:
                side = "bid" if order.side == "sell" else "ask"

                levels = book.asks if side == "ask" else book.bids
                total_depth = sum(float(lvl.size) for lvl in levels) if levels else 0

                if total_depth < order.size * 0.5:
                    return FillResult(
                        order=order, filled_size=0, fill_price=order.price,
                        fill_cost=0, slippage=0, timestamp=timestamp,
                        success=False, error=f"Insufficient depth: {total_depth:.0f} < {order.size:.0f}",
                    )

                vwap = book.vwap(side, order.size)
                if vwap is not None:
                    # Apply market impact: worsen price by MARKET_IMPACT_BPS
                    impact = vwap * self.MARKET_IMPACT_BPS / 10000
                    if side == "ask":  # buying — price goes UP
                        degraded_vwap = vwap + impact
                    else:  # selling — price goes DOWN
                        degraded_vwap = vwap - impact

                    # Apply fill degradation
                    degraded_vwap *= (1 + self.FILL_DEGRADATION) if side == "ask" else (1 - self.FILL_DEGRADATION)

                    slippage = abs(degraded_vwap - order.price)
                    return FillResult(
                        order=order,
                        filled_size=order.size,
                        fill_price=degraded_vwap,
                        fill_cost=degraded_vwap * order.size * (-1 if order.side == "sell" else 1),
                        slippage=slippage,
                        timestamp=timestamp,
                        success=True,
                    )
                else:
                    return FillResult(
                        order=order, filled_size=0, fill_price=order.price,
                        fill_cost=0, slippage=0, timestamp=timestamp,
                        success=False, error="VWAP failed — insufficient liquidity",
                    )

        return FillResult(
            order=order, filled_size=0, fill_price=order.price,
            fill_cost=0, slippage=0, timestamp=timestamp,
            success=False, error="No order book data — can't verify fill",
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
            from eth_account import Account
            from py_clob_client.client import ClobClient  # noqa: F811

            funder = Account.from_key(settings.private_key).address
            self._client = ClobClient(
                host=settings.poly_base_url,
                key=settings.private_key,
                chain_id=137,
                signature_type=0,  # EOA wallet
                funder=funder,
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
        """Execute trades via Polymarket CLOB API — ALL-OR-NOTHING.

        The arb only works if ALL legs execute. A partial fill means
        we hold unhedged directional bets, not an arb.

        Strategy:
          1. Pre-flight: validate ALL legs can execute (balance, min size, liquidity)
          2. Place ALL orders simultaneously (not sequentially)
          3. Wait for ALL to fill (with timeout)
          4. If ANY leg fails → cancel ALL orders immediately
          5. Only record trade if 100% of legs filled
        """
        allowed, reason = self.risk_manager.check_trade(solver_result, event_id)
        if not allowed:
            log.warning("Live trade blocked: %s", reason)
            return ExecutionResult(all_filled=False)

        if not self._ensure_client():
            return ExecutionResult(all_filled=False)

        now = time.time()

        # Build token_id lookup from markets
        token_lookup: dict[str, str] = {}
        if markets:
            for m in markets:
                for idx, o in enumerate(m.outcomes):
                    token_lookup[f"{m.condition_id}:{idx}"] = o.token_id

        # ── PRE-FLIGHT: verify ALL legs before placing ANY orders ──

        # 1. Check total cost fits in balance
        from py_clob_client.clob_types import (
            AssetType,
            BalanceAllowanceParams,
            OrderArgs,
            PartialCreateOrderOptions,
        )
        from py_clob_client.order_builder.constants import BUY, SELL

        try:
            bal_params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=0)
            bal_data = self._client.get_balance_allowance(bal_params)
            usdc_balance = int(bal_data.get("balance", 0)) / 1e6
        except Exception:
            log.warning("Pre-flight: can't check balance — aborting")
            return ExecutionResult(all_filled=False)

        total_order_cost = sum(o.effective_usdc_cost for o in solver_result.orders)
        if total_order_cost > usdc_balance * 0.95:  # 5% safety margin
            log.warning("Pre-flight: total effective cost $%.2f > balance $%.2f — aborting",
                        total_order_cost, usdc_balance)
            return ExecutionResult(all_filled=False)

        # 2. Check each leg: token_id exists, order >= $1, has bids
        leg_details: list[tuple[TradeOrder, str, bool, str]] = []  # (order, token_id, neg_risk, tick_size)

        for order in solver_result.orders:
            token_id = token_lookup.get(order.var_key, "")
            if not token_id:
                log.warning("Pre-flight: no token_id for %s — aborting", order.var_key)
                return ExecutionResult(all_filled=False)

            order_value = order.price * order.size
            if order_value < 1.0:
                log.warning("Pre-flight: order $%.2f < $1 min for %s — aborting",
                            order_value, order.var_key)
                return ExecutionResult(all_filled=False)

            try:
                book = self._client.get_order_book(token_id)
                neg_risk = bool(book.neg_risk)
                tick_size = str(book.tick_size) if book.tick_size else "0.01"
                if not book.bids:
                    log.warning("Pre-flight: no bids for %s — can't exit, aborting", order.var_key)
                    return ExecutionResult(all_filled=False)
            except Exception:
                log.warning("Pre-flight: can't read book for %s — aborting", order.var_key)
                return ExecutionResult(all_filled=False)

            leg_details.append((order, token_id, neg_risk, tick_size))

        log.info("Pre-flight passed: %d legs, total cost $%.2f, balance $%.2f",
                 len(leg_details), total_order_cost, usdc_balance)

        # ── PROBE EXIT: buy tiny amount on first leg, try to sell back ──
        # This verifies we can actually exit positions on this market.
        # Cost: ~spread on $1 order = $0.01-0.05 per probe.
        probe_leg = leg_details[0]
        probe_ok = await self._probe_exitability(probe_leg[0], probe_leg[1], probe_leg[2], probe_leg[3])
        if not probe_ok:
            log.warning("Exit probe FAILED — positions on this market are NOT sellable. Aborting.")
            return ExecutionResult(all_filled=False)
        log.info("Exit probe passed — positions are sellable")

        # ── PLACE ALL ORDERS AT ONCE ──────────────────────────────

        order_ids: list[str] = []
        placement_failed = False

        for order, token_id, neg_risk, tick_size in leg_details:
            side = BUY if order.side == "buy" else SELL
            try:
                args = OrderArgs(
                    token_id=token_id,
                    price=order.price,
                    size=order.size,
                    side=side,
                )
                opts = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)
                resp = self._client.create_and_post_order(args, opts)

                # Validate response
                if not resp or (isinstance(resp, dict) and resp.get("error")):
                    error_msg = resp.get("error", "unknown") if isinstance(resp, dict) else "empty"
                    log.error("Order placement failed for %s: %s", order.var_key, error_msg)
                    placement_failed = True
                    break

                oid = resp.get("orderID") or resp.get("id") or ""
                if not oid:
                    log.error("No order ID for %s — aborting all", order.var_key)
                    placement_failed = True
                    break

                order_ids.append(oid)
                log.info("Placed: %s %s %.1f@%.4f id=%s",
                         order.side, token_id[:16], order.size, order.price, oid[:16])

            except Exception as e:
                log.error("Order failed for %s: %s", order.var_key, str(e)[:100])
                placement_failed = True
                break

        # If ANY placement failed, cancel ALL placed orders immediately
        if placement_failed:
            log.warning("Placement failed — cancelling %d placed orders", len(order_ids))
            await self._cancel_orders(order_ids)
            return ExecutionResult(all_filled=False)

        # ── WAIT FOR ALL FILLS ────────────────────────────────────

        import asyncio
        fills: list[FillResult] = []
        total_cost = 0.0
        total_slippage = 0.0
        all_filled = True
        deadline = time.time() + self.FILL_TIMEOUT

        while time.time() < deadline:
            await asyncio.sleep(self.FILL_POLL_INTERVAL)

            filled_count = 0
            for i, oid in enumerate(order_ids):
                try:
                    status = self._client.get_order(oid)
                    matched = float(status.get("size_matched", 0))
                    target = solver_result.orders[i].size
                    if matched >= target * 0.95:
                        filled_count += 1
                except Exception:
                    pass

            if filled_count == len(order_ids):
                break  # all filled

        # Check final fill status for each leg
        for i, oid in enumerate(order_ids):
            order = solver_result.orders[i]
            try:
                status = self._client.get_order(oid)
                matched = float(status.get("size_matched", 0))
                trades_list = status.get("associate_trades") or []

                if matched >= order.size * 0.95:
                    fill_price = order.price
                    if trades_list and isinstance(trades_list, list) and len(trades_list) > 0:
                        try:
                            fill_price = float(trades_list[0].get("price", order.price))
                        except (ValueError, TypeError):
                            pass

                    slippage = abs(fill_price - order.price)
                    cost = fill_price * matched * (-1 if order.side == "sell" else 1)
                    total_cost += cost
                    total_slippage += slippage * matched

                    fills.append(FillResult(
                        order=order, filled_size=matched, fill_price=fill_price,
                        fill_cost=cost, slippage=slippage, timestamp=now, success=True,
                    ))
                else:
                    all_filled = False
                    fills.append(FillResult(
                        order=order, filled_size=matched, fill_price=order.price,
                        fill_cost=0, slippage=0, timestamp=now, success=False,
                        error=f"Only {matched:.1f}/{order.size:.1f} filled",
                    ))
            except Exception as e:
                all_filled = False
                fills.append(FillResult(
                    order=order, filled_size=0, fill_price=order.price,
                    fill_cost=0, slippage=0, timestamp=now, success=False, error=str(e),
                ))

        # If NOT all filled → cancel everything (all-or-nothing)
        if not all_filled:
            log.warning("Not all legs filled — cancelling ALL %d orders", len(order_ids))
            await self._cancel_orders(order_ids)
            return ExecutionResult(
                fills=fills, total_cost=0, total_slippage=0,
                all_filled=False, guaranteed_profit=0, actual_profit_estimate=0,
            )

        # ALL legs filled — record the trade
        self.risk_manager.record_trade(
            solver_result.orders, event_id=event_id, paper=False,
        )

        actual_profit = solver_result.guaranteed_profit - total_slippage
        result = ExecutionResult(
            fills=fills, total_cost=total_cost, total_slippage=total_slippage,
            all_filled=True, guaranteed_profit=solver_result.guaranteed_profit,
            actual_profit_estimate=actual_profit,
        )

        log.info(
            "ALL %d legs filled! gross $%.4f, fees $%.4f, slippage $%.4f, net $%.4f",
            len(fills), solver_result.gross_profit, solver_result.total_fees,
            total_slippage, actual_profit,
        )
        return result

    async def _probe_exitability(
        self,
        order: TradeOrder,
        token_id: str,
        neg_risk: bool,
        tick_size: str,
    ) -> bool:
        """Probe whether a position can be exited by doing a tiny buy+sell.

        Buys minimum amount ($1 worth), then immediately sells it back.
        If the sell succeeds, the position is exitable. If it fails
        (e.g. neg_risk tokens trapped in contract), we know to abort.

        Cost: spread on ~$1 order ≈ $0.01-0.05.
        """
        import asyncio
        from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions
        from py_clob_client.order_builder.constants import BUY, SELL

        try:
            book = self._client.get_order_book(token_id)
            if not book.asks or not book.bids:
                return False

            best_ask = float(book.asks[0].price)
            best_bid = float(book.bids[0].price)
            if best_ask <= 0 or best_bid <= 0:
                return False

            # Buy minimum amount at best ask
            probe_size = max(1.0, round(1.5 / best_ask, 1))  # ~$1.50 worth
            opts = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)

            log.info("Probe BUY %.1f @ %.4f on %s...", probe_size, best_ask, token_id[:16])
            buy_args = OrderArgs(token_id=token_id, price=best_ask, size=probe_size, side=BUY)
            buy_resp = self._client.create_and_post_order(buy_args, opts)
            buy_oid = buy_resp.get("orderID") or buy_resp.get("id") or ""

            if not buy_oid:
                log.warning("Probe buy: no order ID returned")
                return False

            # Wait for buy to fill
            await asyncio.sleep(3)
            try:
                status = self._client.get_order(buy_oid)
                filled = float(status.get("size_matched", 0))
                if filled < probe_size * 0.5:
                    self._client.cancel(buy_oid)
                    log.warning("Probe buy didn't fill (%.1f/%.1f)", filled, probe_size)
                    return False
            except Exception:
                self._client.cancel(buy_oid)
                return False

            # Now try to sell it back at best bid
            log.info("Probe SELL %.1f @ %.4f...", filled, best_bid)
            sell_args = OrderArgs(token_id=token_id, price=best_bid, size=round(filled, 1), side=SELL)
            try:
                sell_resp = self._client.create_and_post_order(sell_args, opts)
                sell_oid = sell_resp.get("orderID") or sell_resp.get("id") or ""
                if sell_oid:
                    # Sell order accepted — position IS exitable
                    # Wait briefly for fill, then cancel if unfilled (we don't need it to fill)
                    await asyncio.sleep(2)
                    try:
                        self._client.cancel(sell_oid)
                    except Exception:
                        pass  # may have already filled — that's fine
                    log.info("Probe SELL accepted — exit confirmed")
                    return True
                else:
                    log.warning("Probe sell: no order ID — exit may not work")
                    return False
            except Exception as e:
                error_msg = str(e).lower()
                if "not enough balance" in error_msg or "allowance" in error_msg:
                    log.warning("Probe SELL FAILED: %s — tokens trapped in contract", str(e)[:80])
                    return False
                log.warning("Probe sell error: %s", str(e)[:80])
                return False

        except Exception as e:
            log.warning("Probe failed: %s", str(e)[:80])
            return False

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


# ── Single-Leg Executor ──────────────────────────────────────────


@dataclass
class SingleLegResult:
    """Result of a single-leg trade."""
    success: bool
    order_id: str = ""
    filled_size: float = 0.0
    fill_price: float = 0.0
    cost: float = 0.0
    fee: float = 0.0
    slippage: float = 0.0
    error: str = ""


class SingleLegExecutor:
    """Simplified executor for single-token trades.

    Used by weather and crypto strategies where we buy ONE token
    (not multi-leg arb). No all-or-nothing coordination needed.

    Pre-flight: check balance, order book depth, minimum order.
    Execute: place limit order, poll for fill, record in RiskManager.
    Hold to resolution — no exit needed.
    """

    FILL_TIMEOUT = 30
    FILL_POLL_INTERVAL = 2
    MIN_ORDER_USD = 1.0
    MIN_DEPTH_USD = 5.0

    def __init__(self, risk_manager: RiskManager):
        self.risk_manager = risk_manager
        self._client = None
        self._initialized = False

    def _ensure_client(self) -> bool:
        """Lazy-init the CLOB client. Same logic as LiveExecutor."""
        if self._initialized:
            return self._client is not None

        from polycrossarb.config import settings
        if not settings.poly_api_key or not settings.private_key:
            log.error("SingleLeg: API keys not configured")
            self._initialized = True
            return False

        try:
            from eth_account import Account
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            funder = Account.from_key(settings.private_key).address
            self._client = ClobClient(
                host=settings.poly_base_url,
                key=settings.private_key,
                chain_id=137,
                signature_type=0,
                funder=funder,
            )
            self._client.set_api_creds(ApiCreds(
                api_key=settings.poly_api_key,
                api_secret=settings.poly_api_secret,
                api_passphrase=settings.poly_api_passphrase,
            ))
            self._initialized = True
            log.info("SingleLeg CLOB client initialized")
            return True
        except Exception:
            log.exception("SingleLeg: failed to init CLOB client")
            self._initialized = True
            return False

    async def execute_single(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        neg_risk: bool = True,
        event_id: str = "",
    ) -> SingleLegResult:
        """Execute a single-token trade.

        Args:
            token_id: The outcome token to buy/sell.
            side: "buy" only (we hold to resolution).
            price: Limit price.
            size: Number of shares.
            neg_risk: Whether this is a neg_risk market.
            event_id: For cooldown and risk tracking.
        """
        import asyncio

        if not self._ensure_client():
            return SingleLegResult(success=False, error="CLOB client not configured")

        # ── Pre-flight ────────────────────────────────────────────

        order_value = price * size
        if order_value < self.MIN_ORDER_USD:
            return SingleLegResult(success=False, error=f"Order ${order_value:.2f} < ${self.MIN_ORDER_USD} minimum")

        # Check balance
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
            bal_params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=0)
            bal_data = self._client.get_balance_allowance(bal_params)
            usdc_balance = int(bal_data.get("balance", 0)) / 1e6
            if order_value > usdc_balance * 0.95:
                return SingleLegResult(success=False, error=f"Order ${order_value:.2f} > balance ${usdc_balance:.2f}")
        except Exception as e:
            return SingleLegResult(success=False, error=f"Balance check failed: {e}")

        # Check order book
        try:
            book = self._client.get_order_book(token_id)
            neg_risk = bool(book.neg_risk)
            tick_size = str(book.tick_size) if book.tick_size else "0.01"

            # Must have ask depth (we're buying)
            if not book.asks:
                return SingleLegResult(success=False, error="No asks — can't buy")
            ask_depth = sum(float(a.price) * float(a.size) for a in book.asks)
            if ask_depth < self.MIN_DEPTH_USD:
                return SingleLegResult(success=False, error=f"Ask depth ${ask_depth:.2f} < ${self.MIN_DEPTH_USD}")

            # Must have bids (exitability — can sell if needed)
            if not book.bids:
                return SingleLegResult(success=False, error="No bids — can't exit")
        except Exception as e:
            return SingleLegResult(success=False, error=f"Book check failed: {e}")

        # ── Place order ───────────────────────────────────────────

        try:
            from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions
            from py_clob_client.order_builder.constants import BUY

            args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=BUY,
            )
            opts = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)
            resp = self._client.create_and_post_order(args, opts)

            if not resp or (isinstance(resp, dict) and resp.get("error")):
                error_msg = resp.get("error", "unknown") if isinstance(resp, dict) else "empty"
                return SingleLegResult(success=False, error=f"Order rejected: {error_msg}")

            order_id = resp.get("orderID") or resp.get("id") or ""
            if not order_id:
                return SingleLegResult(success=False, error="No order ID returned")

            log.info("SingleLeg: placed %s %.1f @ $%.4f id=%s", side, size, price, order_id[:16])

        except Exception as e:
            return SingleLegResult(success=False, error=f"Order failed: {str(e)[:80]}")

        # ── Wait for fill ─────────────────────────────────────────

        filled_size = 0.0
        fill_price = price
        deadline = time.time() + self.FILL_TIMEOUT

        while time.time() < deadline:
            await asyncio.sleep(self.FILL_POLL_INTERVAL)
            try:
                status = self._client.get_order(order_id)
                matched = float(status.get("size_matched", 0))
                if matched >= size * 0.95:
                    filled_size = matched
                    trades = status.get("associate_trades") or []
                    if trades and isinstance(trades, list) and len(trades) > 0:
                        try:
                            fill_price = float(trades[0].get("price", price))
                        except (ValueError, TypeError):
                            pass
                    break
            except Exception:
                pass

        if filled_size < size * 0.50:
            # Not enough filled — cancel
            try:
                self._client.cancel(order_id)
            except Exception:
                log.warning("SingleLeg: failed to cancel %s", order_id[:16])
            return SingleLegResult(
                success=False, order_id=order_id,
                filled_size=filled_size, fill_price=fill_price,
                error=f"Timeout: {filled_size:.1f}/{size:.1f} filled",
            )

        # ── Record trade ──────────────────────────────────────────

        slippage = abs(fill_price - price)
        cost = fill_price * filled_size

        # Record in risk manager using a TradeOrder wrapper
        trade_order = TradeOrder(
            market_condition_id=event_id or token_id[:20],
            outcome_idx=0,
            side=side,
            size=filled_size,
            price=fill_price,
            expected_cost=cost,
            neg_risk=neg_risk,
        )
        self.risk_manager.record_trade([trade_order], event_id=event_id, paper=False)

        log.info(
            "SingleLeg: FILLED %.1f @ $%.4f (slippage $%.4f, cost $%.2f)",
            filled_size, fill_price, slippage, cost,
        )

        return SingleLegResult(
            success=True,
            order_id=order_id,
            filled_size=filled_size,
            fill_price=fill_price,
            cost=cost,
            slippage=slippage,
        )
