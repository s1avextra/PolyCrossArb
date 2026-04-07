"""BTC candle trading pipeline.

Trades "Bitcoin Up or Down" 5/15-minute candle markets by detecting
price momentum from 4 exchange feeds and buying the likely outcome
before resolution.

Flow (every 1 second):
  1. Get current BTC price from 4 exchanges
  2. Scan for candle contracts resolving within 30 minutes
  3. For each contract, detect momentum direction
  4. If confidence > threshold AND market price is mispriced, trade
  5. Hold until resolution (5-15 minutes)

The edge: we see BTC price forming the candle in real-time from
4 exchanges. The Polymarket market maker updates prices every few
seconds. In that gap, we buy the correct outcome.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import structlog

from polycrossarb.config import settings
from polycrossarb.data.client import PolymarketClient
from polycrossarb.execution.executor import ExecutionMode, SingleLegExecutor
from polycrossarb.risk.manager import RiskManager
from polycrossarb.solver.linear import TradeOrder
from polycrossarb.crypto.candle_scanner import CandleContract, scan_candle_markets
from polycrossarb.crypto.fair_value import compute_fair_value
from polycrossarb.crypto.momentum import MomentumDetector, MomentumSignal
from polycrossarb.crypto.price_feed import CryptoPriceFeed
from polycrossarb.monitoring.alerter import alert_circuit_breaker, alert_trade
from polycrossarb.monitoring.session_monitor import SessionMonitor

log = structlog.get_logger(__name__)


class CandlePipeline:
    """High-frequency BTC candle trading pipeline."""

    # ── Circuit breaker defaults ──────────────────────────────────
    BREAKER_MIN_TRADES = 20       # need N resolved trades before checking
    BREAKER_MIN_WIN_RATE = 0.65   # auto-stop below this win rate
    BREAKER_MAX_DRAWDOWN_PCT = 0.30  # auto-stop if drawdown > 30% of peak

    def __init__(
        self,
        mode: ExecutionMode = ExecutionMode.PAPER,
        log_dir: str = "logs",
        min_confidence: float = 0.60,
        min_edge: float | None = None,
        risk_manager: RiskManager | None = None,
    ):
        self.mode = mode
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)

        self._client = PolymarketClient()
        self._risk = risk_manager or RiskManager()
        self._price_feed = CryptoPriceFeed()
        self._momentum = MomentumDetector()

        if mode == ExecutionMode.LIVE:
            self._executor = SingleLegExecutor(self._risk)
        else:
            self._executor = None

        self._contracts: list[CandleContract] = []
        self._traded: set[str] = set()
        self._trade_count = 0
        self._wins = 0
        self._losses = 0
        self._total_profit = 0.0
        self._realized_profit = 0.0  # actual resolved P&L
        self._peak_profit = 0.0      # high-water mark for drawdown

        # Paper resolution tracking: {contract_id -> {direction, entry_price, size, open_btc, end_time}}
        self._paper_positions: dict[str, dict] = {}
        self._min_confidence = min_confidence
        self._min_edge = min_edge if min_edge is not None else settings.min_crypto_edge
        self._running = False
        self._breaker_tripped = False
        self._monitor = SessionMonitor()

    async def run(self) -> None:
        """Run the candle trading pipeline."""
        self._running = True
        log.info("candle.start", mode=self.mode,
                 min_conf=f"{self._min_confidence:.0%}",
                 min_edge=f"{self._min_edge:.0%}")

        await self._refresh_contracts()

        try:
            await asyncio.gather(
                self._price_feed.start(),
                self._scan_loop(),
                self._contract_refresh_loop(),
                self._paper_resolution_loop(),
                self._monitoring_loop(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            self._price_feed.stop()
            await self._client.close()
            self._running = False

            # Print and save monitoring report
            report = self._monitor.print_summary()
            print(report)
            self._monitor.close()

            log.info("candle.stopped",
                     trades=self._trade_count,
                     profit=f"${self._total_profit:.2f}")

    def stop(self):
        self._running = False
        self._price_feed.stop()

    def _check_circuit_breaker(self) -> bool:
        """Check if safety limits are breached. Returns True if tripped."""
        total_resolved = self._wins + self._losses
        if total_resolved < self.BREAKER_MIN_TRADES:
            return False

        # Win rate check
        win_rate = self._wins / total_resolved
        if win_rate < self.BREAKER_MIN_WIN_RATE:
            log.warning(
                "candle.circuit_breaker",
                reason="win_rate_low",
                win_rate=f"{win_rate:.0%}",
                threshold=f"{self.BREAKER_MIN_WIN_RATE:.0%}",
                trades=total_resolved,
            )
            return True

        # Drawdown check: works from both positive and negative peak
        self._peak_profit = max(self._peak_profit, self._realized_profit)
        drawdown = self._peak_profit - self._realized_profit
        # Use absolute drawdown against initial bankroll when peak is 0 or negative
        if self._peak_profit > 0:
            dd_pct = drawdown / self._peak_profit
        else:
            dd_pct = abs(self._realized_profit) / max(self._risk._initial_bankroll, 1.0)

        if dd_pct > self.BREAKER_MAX_DRAWDOWN_PCT:
            log.warning(
                "candle.circuit_breaker",
                reason="drawdown",
                drawdown=f"${drawdown:.2f}",
                drawdown_pct=f"{dd_pct:.0%}",
                threshold=f"{self.BREAKER_MAX_DRAWDOWN_PCT:.0%}",
            )
            return True

        # Bankroll depletion check
        if self._risk.effective_bankroll < 1.0:
            log.warning(
                "candle.circuit_breaker",
                reason="bankroll_depleted",
                bankroll=f"${self._risk.effective_bankroll:.2f}",
            )
            return True

        return False

    async def _refresh_contracts(self):
        """Fetch candle contracts."""
        markets = await self._client.fetch_all_active_markets(min_liquidity=0)
        self._contracts = scan_candle_markets(markets, max_hours=1.0, min_liquidity=50)
        log.info("candle.scan", contracts=len(self._contracts))

    async def _paper_resolution_loop(self):
        """Resolve paper positions when their candle window expires.

        Polls every 2 seconds for accurate resolution timing.
        Snapshots BTC price near window close for accurate resolution.
        """
        # Snapshot BTC prices near window close for accurate resolution
        _close_prices: dict[str, float] = {}

        while self._running:
            # Poll fast (1s) if positions nearing resolution, else slower (5s)
            now_ts = time.time()
            near_resolution = any(
                0 < pos["end_time"] - now_ts < 15
                for pos in self._paper_positions.values()
            ) if self._paper_positions else False
            await asyncio.sleep(1 if near_resolution else 5)

            if not self._paper_positions:
                continue

            now_ts = time.time()
            btc = self._price_feed.btc_price
            if btc <= 0:
                continue

            # Snapshot BTC price for positions within 2s of close
            for cid, pos in self._paper_positions.items():
                if cid not in _close_prices and abs(now_ts - pos["end_time"]) < 2:
                    _close_prices[cid] = btc

            resolved = []
            for cid, pos in list(self._paper_positions.items()):
                if now_ts < pos["end_time"]:
                    continue  # not yet resolved

                # Use snapshotted close price if available, else current price
                close_btc = _close_prices.pop(cid, btc)
                delay = now_ts - pos["end_time"]
                if delay > 30:
                    log.warning("candle.late_resolution", cid=cid[:16],
                                delay_s=f"{delay:.0f}")

                open_btc = pos["open_btc"]
                actual_direction = "up" if close_btc >= open_btc else "down"
                predicted = pos["direction"]
                won = actual_direction == predicted

                entry_price = pos["entry_price"]
                size = pos["size"]
                fee = pos.get("fee", 0.0)

                if won:
                    # Token resolves to $1.00
                    pnl = (1.0 - entry_price) * size - fee
                    self._wins += 1
                else:
                    # Token resolves to $0.00
                    pnl = -entry_price * size - fee
                    self._losses += 1

                self._realized_profit += pnl

                # Free up capital in risk manager
                key = f"{cid}:0"
                self._risk.close_position(key, 1.0 if won else 0.0)

                # Monitor resolution
                self._monitor.record_resolution(
                    contract_id=cid,
                    predicted=predicted,
                    actual=actual_direction,
                    won=won,
                    pnl=pnl,
                    entry_price=entry_price,
                    open_btc=open_btc,
                    close_btc=close_btc,
                )

                log.info(
                    "candle.resolved",
                    direction=predicted,
                    actual=actual_direction,
                    won=won,
                    pnl=f"${pnl:+.2f}",
                    open_btc=f"${open_btc:,.0f}",
                    close_btc=f"${close_btc:,.0f}",
                    total_wins=self._wins,
                    total_losses=self._losses,
                    realized=f"${self._realized_profit:.2f}",
                )

                resolved.append(cid)

            for cid in resolved:
                del self._paper_positions[cid]

            # Check circuit breaker after resolutions
            if resolved and self._check_circuit_breaker():
                self._breaker_tripped = True
                total_resolved = self._wins + self._losses
                wr = self._wins / max(total_resolved, 1)
                alert_circuit_breaker("candle", "auto-stop triggered", {
                    "win_rate": f"{wr:.0%}",
                    "drawdown": f"${self._peak_profit - self._realized_profit:.2f}",
                    "trades": total_resolved,
                })
                log.warning("candle.circuit_breaker.tripped",
                            wins=self._wins, losses=self._losses,
                            realized=f"${self._realized_profit:.2f}")
                self.stop()

    async def _monitoring_loop(self):
        """Record price feed and risk state every 15 seconds."""
        while self._running and self._price_feed.btc_price == 0:
            await asyncio.sleep(1)

        prev_sources: set[str] = set()

        while self._running:
            agg = self._price_feed.get_aggregated()

            # Price snapshot
            self._monitor.record_price_snapshot(
                btc_price=agg.mid,
                n_sources=agg.n_sources,
                spread=agg.spread,
                staleness_ms=agg.staleness_ms,
                sources=agg.sources,
            )

            # Detect source dropouts
            current_sources = set(agg.sources.keys())
            for src in prev_sources - current_sources:
                self._monitor.record_source_dropout(src, 0, agg.staleness_ms / 1000)
            prev_sources = current_sources

            # Risk state
            self._monitor.record_risk_state(
                bankroll=self._risk.effective_bankroll,
                exposure=self._risk.total_exposure,
                available=self._risk.available_capital,
                positions=len(self._paper_positions) + len(self._risk.positions),
                realized_pnl=self._realized_profit,
                wins=self._wins,
                losses=self._losses,
            )

            await asyncio.sleep(15)

    async def _contract_refresh_loop(self):
        """Refresh every 2 minutes to catch new candle windows."""
        while self._running:
            await asyncio.sleep(120)
            if self._running:
                try:
                    t0 = time.time()
                    await self._refresh_contracts()
                    latency = (time.time() - t0) * 1000
                    self._monitor.record_api_call("gamma/markets", latency, 200)
                    self._monitor.record_contract_scan(
                        total_markets=0,  # not tracked at this level
                        candle_contracts=len(self._contracts),
                        tradeable_contracts=sum(1 for c in self._contracts if c.liquidity >= 50),
                    )
                    # Log each contract detail
                    for c in self._contracts:
                        self._monitor.record_contract_detail(
                            condition_id=c.market.condition_id,
                            question=c.market.question,
                            up_price=c.up_price,
                            down_price=c.down_price,
                            spread=c.spread,
                            volume=c.volume,
                            liquidity=c.liquidity,
                            hours_left=c.hours_left,
                        )
                except Exception as e:
                    self._monitor.record_error("contract_refresh", str(e))
                    log.exception("candle.refresh_error")

    async def _scan_loop(self):
        """Main loop: detect momentum and trade every second."""
        # Wait for price feed
        while self._running and self._price_feed.btc_price == 0:
            await asyncio.sleep(0.5)

        log.info("candle.price_ready",
                 btc=f"${self._price_feed.btc_price:,.2f}",
                 sources=self._price_feed.n_live_sources)

        cycle = 0
        while self._running:
            cycle += 1
            t0 = time.time()

            btc = self._price_feed.btc_price
            if btc <= 0:
                await asyncio.sleep(1)
                continue

            # Feed tick to momentum detector with current volatility
            self._momentum.add_tick(btc)
            self._momentum.set_realized_vol(self._price_feed.volatility)

            # Skip trading if circuit breaker tripped
            if self._breaker_tripped:
                await asyncio.sleep(1)
                continue

            # Skip trading if insufficient price sources
            if not self._price_feed.is_reliable:
                if cycle % 30 == 0:
                    log.warning("candle.low_sources",
                                sources=self._price_feed.n_live_sources,
                                min_required=self._price_feed.MIN_SOURCES)
                continue

            # Evaluate each candle contract
            now = datetime.now(timezone.utc)

            # Track which time windows we've traded this cycle
            # to avoid betting on multiple contracts for the same candle
            traded_windows: set[str] = set()

            for contract in self._contracts:
                if contract.market.condition_id in self._traded:
                    continue

                # Recalculate hours left
                try:
                    end = datetime.fromisoformat(contract.end_date.replace("Z", "+00:00"))
                    minutes_left = (end - now).total_seconds() / 60
                except (ValueError, TypeError):
                    continue

                # Skip if already resolved or too far out
                if minutes_left <= 0.5 or minutes_left > 30:
                    continue

                # Deduplicate: only 1 trade per time window
                window_key = contract.end_date  # same end_date = same candle window
                if window_key in traded_windows:
                    continue

                # Calculate window elapsed
                window_minutes = self._estimate_window_minutes(contract)
                minutes_elapsed = max(0, window_minutes - minutes_left)

                if minutes_elapsed < 0.5:
                    continue  # too early in the window

                # Set open price if first time seeing this contract
                cid = contract.market.condition_id
                if self._momentum.get_open_price(cid) is None:
                    self._momentum.set_window_open(cid, btc)

                # Detect momentum
                signal = self._momentum.detect(
                    contract_id=cid,
                    window_start_ago_minutes=minutes_elapsed,
                    minutes_remaining=minutes_left,
                    current_price=btc,
                )

                if not signal:
                    continue

                # ── 3-Zone entry timing ────────────────────────────────
                # Zone determines confidence/edge thresholds based on
                # where we are in the window. Early entries get better
                # prices, late entries need stronger signals.
                elapsed_pct = minutes_elapsed / window_minutes if window_minutes > 0 else 1.0

                if elapsed_pct < 0.40:
                    # EARLY ZONE: only trade on very strong vol-adjusted signals
                    zone = "early"
                    zone_min_confidence = 0.55
                    zone_min_z_score = 2.0
                    zone_min_edge = 0.03
                elif elapsed_pct < 0.80:
                    # PRIMARY ZONE: sweet spot — decent signal + decent price
                    zone = "primary"
                    zone_min_confidence = self._min_confidence
                    zone_min_z_score = 1.0
                    zone_min_edge = self._min_edge
                else:
                    # LATE ZONE: only trade with strong edge (market is priced in)
                    zone = "late"
                    zone_min_confidence = 0.65
                    zone_min_z_score = 0.5
                    zone_min_edge = max(self._min_edge, 0.08)

                if signal.confidence < zone_min_confidence:
                    self._monitor.record_signal_skip(cid, f"low_confidence_{zone}_{signal.confidence:.2f}")
                    continue

                if signal.z_score < zone_min_z_score:
                    self._monitor.record_signal_skip(cid, f"low_zscore_{zone}_{signal.z_score:.2f}")
                    continue

                # Skip 80-90% confidence dead zone — backtesting shows
                # this bucket has 66% WR but net negative P&L
                if 0.80 <= signal.confidence < 0.90:
                    self._monitor.record_signal_skip(cid, "confidence_dead_zone_80_90")
                    continue

                # Check edge: is the market mispricing the direction?
                if signal.direction == "up":
                    market_price = contract.up_price
                    token_id = contract.up_token_id
                else:
                    market_price = contract.down_price
                    token_id = contract.down_token_id

                # Skip lottery tickets and near-certainties
                if market_price < 0.10 or market_price > 0.90:
                    self._monitor.record_signal_skip(cid, f"price_out_of_range_{market_price:.2f}")
                    continue

                # ── YES+NO mispricing check ────────────────────────────
                # If up_price + down_price != 1.0, there's a risk-free
                # arb on top of directional edge. Log it.
                vig = contract.up_price + contract.down_price - 1.0
                if abs(vig) > 0.02:
                    log.info("candle.mispricing",
                             cid=cid[:16],
                             up=f"${contract.up_price:.3f}",
                             down=f"${contract.down_price:.3f}",
                             vig=f"{vig:+.3f}")

                # ── Black-Scholes fair value ───────────────────────────
                # Use BS binary pricing with vol + time remaining instead
                # of the old heuristic dampening.
                vol = self._price_feed.implied_volatility
                days_remaining = minutes_left / 1440.0
                open_btc = signal.open_price

                if signal.direction == "up":
                    # P(BTC > open_price at expiry) via BS
                    fv_result = compute_fair_value(
                        btc_price=btc,
                        strike=open_btc,
                        days_to_expiry=days_remaining,
                        volatility=vol,
                        market_price=market_price,
                    )
                    fair_value = fv_result.fair_price
                else:
                    # P(BTC < open_price) = 1 - P(BTC > open_price)
                    fv_result = compute_fair_value(
                        btc_price=btc,
                        strike=open_btc,
                        days_to_expiry=days_remaining,
                        volatility=vol,
                        market_price=market_price,
                    )
                    fair_value = 1.0 - fv_result.fair_price

                edge = fair_value - market_price

                # Cap edge at realistic maximum — edges >25% signal stale prices
                if edge > 0.25:
                    self._monitor.record_signal_skip(cid, f"edge_too_high_{edge:.2f}")
                    continue

                if edge < zone_min_edge:
                    self._monitor.record_signal_skip(cid, f"low_edge_{zone}_{edge:.3f}")
                    continue

                # Mark this window as traded (even before execution)
                traded_windows.add(window_key)

                # Record signal (whether or not we trade)
                self._monitor.record_signal(
                    contract_id=cid,
                    direction=signal.direction,
                    confidence=signal.confidence,
                    edge=edge,
                    market_price=market_price,
                    fair_value=fair_value,
                    btc_price=btc,
                    btc_change=signal.price_change,
                    minutes_remaining=minutes_left,
                    traded=True,
                )

                # Execute trade
                await self._execute_candle_trade(contract, signal, token_id,
                                                  market_price, fair_value, edge, zone)

            # Log every 30 cycles
            if cycle % 30 == 0:
                agg = self._price_feed.get_aggregated()
                log.info("candle.cycle",
                         cycle=cycle,
                         btc=f"${btc:,.0f}",
                         sources=agg.n_sources,
                         spread=f"${agg.spread:.2f}",
                         contracts=len(self._contracts),
                         trades=self._trade_count)

            # 500ms interval — 2Hz evaluation for lower latency
            elapsed = time.time() - t0
            if elapsed < 0.5:
                await asyncio.sleep(0.5 - elapsed)

    def _estimate_window_minutes(self, contract: CandleContract) -> float:
        """Estimate the window length from the description."""
        desc = contract.window_description.lower()
        if "am-" in desc or "pm-" in desc:
            # Parse time range
            # "3:45AM-4:00AM" = 15 min
            # "4:00AM-4:05AM" = 5 min
            # "12:00AM-4:00AM" = 240 min
            parts = desc.replace("et", "").strip().split("-")
            if len(parts) == 2:
                try:
                    from datetime import datetime as dt
                    t1 = dt.strptime(parts[0].strip().split(",")[-1].strip(), "%I:%M%p")
                    t2 = dt.strptime(parts[1].strip(), "%I:%M%p")
                    diff = (t2 - t1).total_seconds() / 60
                    if diff < 0:
                        diff += 24 * 60
                    return diff
                except (ValueError, IndexError):
                    pass
        # Hourly contracts
        if any(f"{h}am" in desc or f"{h}pm" in desc for h in range(1, 13)):
            return 60
        return 15  # default

    async def _execute_candle_trade(
        self,
        contract: CandleContract,
        signal: MomentumSignal,
        token_id: str,
        market_price: float,
        fair_value: float,
        edge: float,
        zone: str = "late",
    ):
        """Execute a candle trade."""
        # Position sizing: $1 steps based on bankroll
        #   $5-14   → $1/trade
        #   $15-29  → $2/trade
        #   $30-49  → $3/trade
        #   $50-99  → $5/trade
        #   $100+   → $10/trade (capped by max_per_market)
        bankroll = self._risk.effective_bankroll
        if bankroll < 15:
            position = 1.0
        elif bankroll < 30:
            position = 2.0
        elif bankroll < 50:
            position = 3.0
        elif bankroll < 100:
            position = 5.0
        else:
            position = min(10.0, self._risk.max_per_market)

        # Cap by available capital (prevent overcommitting across concurrent trades)
        position = min(position, self._risk.available_capital)

        if position < 1.0:  # Polymarket rejects orders below $1
            return

        if market_price <= 0.01 or market_price >= 0.99:
            return  # price out of tradeable range

        shares = position / market_price

        if self.mode == ExecutionMode.PAPER:
            # Apply realistic slippage and fees for paper trading
            # Use maker order strategy: 0% fee + potential rebate
            # vs taker at 1.8%. Place limit 1 tick inside spread.
            slippage_bps = 10  # 10 bps for maker (less than 30 bps taker impact)
            fee_rate = 0.0     # 0% maker fee (Polymarket maker rebate)
            slipped_price = market_price * (1 + slippage_bps / 10000)
            slipped_price = min(slipped_price, 0.99)
            shares = position / slipped_price  # recalc with slipped price
            fee = position * fee_rate
            expected_profit = shares * (fair_value - slipped_price) - fee
            log.info("candle.trade.paper",
                     direction=signal.direction,
                     window=contract.window_description[:25],
                     confidence=f"{signal.confidence:.0%}",
                     price=f"${market_price:.3f}",
                     slipped=f"${slipped_price:.3f}",
                     fair=f"${fair_value:.3f}",
                     edge=f"{edge:+.1%}",
                     btc_change=f"${signal.price_change:+.0f}",
                     minutes_left=f"{signal.minutes_remaining:.1f}",
                     cost=f"${position:.2f}",
                     fee=f"${fee:.4f}",
                     profit=f"${expected_profit:.2f}")

            self._trade_count += 1
            self._total_profit += expected_profit
            self._traded.add(contract.market.condition_id)

            # Register position with risk manager so exposure is tracked
            paper_order = TradeOrder(
                market_condition_id=contract.market.condition_id,
                outcome_idx=0,
                side="buy",
                size=shares,
                price=slipped_price,
                expected_cost=position,
                neg_risk=False,
            )
            self._risk.record_trade([paper_order], event_id=contract.market.event_id, paper=True)

            # Track for paper resolution (use slipped price for realistic P&L)
            try:
                end = datetime.fromisoformat(contract.end_date.replace("Z", "+00:00"))
                self._paper_positions[contract.market.condition_id] = {
                    "direction": signal.direction,
                    "entry_price": slipped_price,
                    "fee": fee,
                    "size": shares,
                    "open_btc": signal.open_price,
                    "end_time": end.timestamp(),
                }
            except (ValueError, TypeError):
                pass

        elif self._executor:
            t0 = time.time()
            result = await self._executor.execute_single(
                token_id=token_id,
                side="buy",
                price=market_price,
                size=round(shares, 1),
                neg_risk=False,
                event_id=contract.market.event_id,
            )
            fill_time = time.time() - t0

            if result.success:
                self._trade_count += 1
                self._traded.add(contract.market.condition_id)

                self._monitor.record_fill(
                    order_id=result.order_id,
                    filled_size=result.filled_size,
                    requested_size=round(shares, 1),
                    fill_price=result.fill_price,
                    limit_price=market_price,
                    fill_time_s=fill_time,
                    fee=result.fee,
                    n_trades=1,
                )

                log.info("candle.trade.filled",
                         direction=signal.direction,
                         fill=f"${result.fill_price:.3f}",
                         cost=f"${result.cost:.2f}",
                         fee=f"${result.fee:.4f}",
                         fill_time=f"{fill_time:.1f}s")
            else:
                self._monitor.record_order_rejected(
                    token_id=token_id,
                    reason=result.error,
                    price=market_price,
                    size=round(shares, 1),
                )
                log.warning("candle.trade.failed", error=result.error)

        # Log to file
        entry = {
            "timestamp": time.time(),
            "direction": signal.direction,
            "window": contract.window_description,
            "zone": zone,
            "confidence": round(signal.confidence, 4),
            "z_score": round(signal.z_score, 3),
            "market_price": market_price,
            "fair_value": fair_value,
            "edge": round(edge, 4),
            "btc_price": signal.current_price,
            "btc_change": signal.price_change,
            "minutes_left": signal.minutes_remaining,
            "position": position,
            "mode": self.mode,
        }
        with open(self.log_dir / "candle_trades.jsonl", "a") as f:
            f.write(json.dumps(entry) + "\n")

    def status(self) -> dict:
        win_rate = self._wins / max(self._wins + self._losses, 1)
        return {
            "mode": self.mode,
            "contracts": len(self._contracts),
            "trades": self._trade_count,
            "expected_profit": round(self._total_profit, 2),
            "realized_profit": round(self._realized_profit, 2),
            "wins": self._wins,
            "losses": self._losses,
            "win_rate": f"{win_rate:.0%}",
            "open_positions": len(self._paper_positions),
            "btc_price": self._price_feed.btc_price,
            "sources": self._price_feed.n_live_sources,
            "circuit_breaker": self._breaker_tripped,
            "bankroll": round(self._risk.effective_bankroll, 2),
            "min_edge": self._min_edge,
        }
