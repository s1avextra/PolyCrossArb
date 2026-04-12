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
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import structlog

from polycrossarb.config import settings
from polycrossarb.crypto.candle_scanner import CandleContract, scan_candle_markets
from polycrossarb.crypto.decision import (
    CandleDecision,
    SkipReason,
    decide_candle_trade,
    zone_config_from_settings,
)
from polycrossarb.crypto.fair_value import compute_fair_value
from polycrossarb.crypto.momentum import (
    MomentumDetector,
    MomentumSignal,
    VolatilityRegime,
    classify_vol_regime,
)
from polycrossarb.crypto.price_feed import CryptoPriceFeed
from polycrossarb.data.client import PolymarketClient
from polycrossarb.execution.executor import ExecutionMode, SingleLegExecutor
from polycrossarb.monitoring.alerter import (
    alert_circuit_breaker,
    alert_shutdown,
    alert_trade,
    require_alerter_configured,
)
from polycrossarb.monitoring.session_monitor import SessionMonitor
from polycrossarb.risk.manager import RiskManager
from polycrossarb.solver.linear import TradeOrder

log = structlog.get_logger(__name__)


class CandlePipeline:
    """High-frequency BTC candle trading pipeline."""

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

        # Live mode requires alerter to be configured up front so a missing
        # webhook fails fast instead of silently dropping CIRCUIT BREAKER
        # alerts later.
        if mode == ExecutionMode.LIVE and settings.alert_required:
            require_alerter_configured()

        # Pull tunables from settings (config-driven, no code edits to tune)
        self.BREAKER_MIN_TRADES = settings.candle_breaker_min_trades
        self.BREAKER_MIN_WIN_RATE = settings.candle_breaker_min_win_rate
        self.BREAKER_MAX_DRAWDOWN_PCT = settings.candle_breaker_max_drawdown_pct
        self._zone_config = zone_config_from_settings()
        self._kill_switch_path = Path(settings.kill_switch_path)
        self._skip_dead_zone = settings.candle_skip_dead_zone

        self._client = PolymarketClient()
        self._risk = risk_manager or RiskManager()
        self._price_feed = CryptoPriceFeed()
        # Per-asset momentum detectors — BTC always present, ETH/SOL
        # created on first use. Each asset needs its own tick history.
        self._momentum_detectors: dict[str, MomentumDetector] = {
            "BTC": MomentumDetector(),
        }
        self._momentum = self._momentum_detectors["BTC"]  # backwards compat

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

        # Restore breaker state and any in-flight paper positions across restarts
        try:
            if self._risk.get_meta("candle_breaker_tripped") == "1":
                self._breaker_tripped = True
                log.warning("candle.breaker.restored",
                            note="circuit breaker was tripped before restart")
            stored_pp = self._risk.load_paper_positions()
            if stored_pp:
                self._paper_positions.update(stored_pp)
                log.info("candle.paper_positions.restored", n=len(stored_pp))
        except Exception:
            log.exception("candle.state_restore_failed")

    def _get_momentum(self, asset: str) -> MomentumDetector:
        """Get or create a momentum detector for the given asset."""
        if asset not in self._momentum_detectors:
            self._momentum_detectors[asset] = MomentumDetector()
        return self._momentum_detectors[asset]

    def _kill_switch_active(self) -> bool:
        """Operational halt — touch this file to stop trading instantly."""
        try:
            return self._kill_switch_path.exists()
        except Exception:
            return False

    def _persist_breaker_tripped(self) -> None:
        try:
            self._risk.set_meta("candle_breaker_tripped", "1")
        except Exception:
            log.exception("candle.breaker.persist_failed")

    def _persist_paper_positions(self) -> None:
        try:
            self._risk.save_paper_positions(self._paper_positions)
        except Exception:
            log.exception("candle.paper_positions.persist_failed")

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
        """Synchronous stop — used by signal handlers and the breaker."""
        self._running = False
        try:
            self._price_feed.stop()
        except Exception:
            pass

    async def shutdown(self, reason: str = "shutdown") -> None:
        """Graceful async shutdown.

        Persists state, fires a shutdown alert, and signals the run loop
        to exit. Safe to call multiple times — idempotent.

        Caller (signal handler / supervisor) should ``await`` this so the
        outstanding work has a chance to drain before the process exits.
        """
        if not self._running:
            return
        log.info("candle.shutdown.start", reason=reason)
        self._running = False
        try:
            self._price_feed.stop()
        except Exception:
            log.exception("candle.shutdown.price_feed_stop_failed")

        try:
            self._persist_paper_positions()
        except Exception:
            log.exception("candle.shutdown.persist_paper_failed")

        try:
            alert_shutdown(
                reason=reason,
                pnl=self._realized_profit,
                trades=self._wins + self._losses,
            )
        except Exception:
            log.exception("candle.shutdown.alert_failed")

        log.info("candle.shutdown.done",
                 reason=reason,
                 wins=self._wins, losses=self._losses,
                 realized=f"${self._realized_profit:.2f}")

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
        """Fetch candle contracts.

        The Gamma API returns 50k+ markets. Scanning them for candle
        patterns blocks the event loop. Run the filter in a thread.
        """
        markets = await self._client.fetch_all_active_markets(min_liquidity=0)
        # scan_candle_markets iterates 50k+ markets — run in thread to
        # avoid blocking the scan loop (measured: 36ms on VPS)
        self._contracts = await asyncio.to_thread(
            scan_candle_markets, markets, 1.0, 50,
        )
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
            if resolved:
                self._persist_paper_positions()

            # Check circuit breaker after resolutions
            if resolved and self._check_circuit_breaker():
                self._breaker_tripped = True
                self._persist_breaker_tripped()
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
        """Refresh every 2 minutes to catch new candle windows.

        Uses asyncio.create_task so the Gamma API pagination doesn't
        starve the scan loop. The refresh runs concurrently and swaps
        self._contracts atomically when done.
        """
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
        _last_btc = 0.0
        _unchanged_count = 0
        while self._running:
            cycle += 1
            t0 = time.time()

            btc = self._price_feed.btc_price
            if btc <= 0:
                await asyncio.sleep(1)
                continue

            # Skip evaluation when price hasn't moved — no new information.
            # Still yield to event loop every cycle for WS processing.
            # Force re-evaluation every 10 unchanged cycles (~1s) to catch
            # time-based zone transitions (late → terminal).
            if btc == _last_btc:
                _unchanged_count += 1
                if _unchanged_count < 10:
                    elapsed = time.time() - t0
                    if elapsed < 0.1:
                        await asyncio.sleep(0.1 - elapsed)
                    continue
                _unchanged_count = 0  # force eval every ~1s even if flat
            else:
                _unchanged_count = 0
            _last_btc = btc

            # Feed BTC tick to its momentum detector
            self._momentum.add_tick(btc)
            self._momentum.set_realized_vol(self._price_feed.volatility)

            # Feed ETH/SOL ticks to their detectors (only when cross-asset is on)
            if settings.candle_cross_asset_enabled:
                for alt in ("ETH", "SOL"):
                    alt_price = self._price_feed.get_price(alt)
                    if alt_price > 0:
                        det = self._get_momentum(alt)
                        det.add_tick(alt_price)
                        det.set_realized_vol(self._price_feed.volatility)

            # ── Hard kill switch (touch /tmp/polycrossarb/KILL) ──
            if self._kill_switch_active():
                log.warning("candle.kill_switch_active",
                            path=str(self._kill_switch_path))
                self._breaker_tripped = True
                self._persist_breaker_tripped()
                alert_circuit_breaker("candle", "kill switch", {
                    "path": str(self._kill_switch_path),
                })
                self.stop()
                continue

            # ── Eager circuit breaker (every cycle, not just on resolution) ──
            if not self._breaker_tripped and self._check_circuit_breaker():
                self._breaker_tripped = True
                self._persist_breaker_tripped()
                total_resolved = self._wins + self._losses
                wr = self._wins / max(total_resolved, 1)
                alert_circuit_breaker("candle", "auto-stop (eager)", {
                    "win_rate": f"{wr:.0%}",
                    "drawdown": f"${self._peak_profit - self._realized_profit:.2f}",
                    "trades": total_resolved,
                })
                self.stop()
                continue

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
                await asyncio.sleep(0.5)
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

                # Skip if already resolved or too far out.
                # Floor at 5s (0.083 min) — must be strictly below the
                # terminal zone threshold (5% of window_minutes) so that
                # terminal-zone trades can fire. For 5-min candles,
                # terminal at 95% = 0.25 min remaining; this floor at
                # 0.083 allows trades in the [0.083, 0.25] band.
                if minutes_left <= 0.083 or minutes_left > 30:
                    continue

                # Deduplicate: only 1 trade per time window
                window_key = contract.end_date  # same end_date = same candle window
                if window_key in traded_windows:
                    continue

                cid = contract.market.condition_id

                # Calculate window elapsed
                window_minutes = self._estimate_window_minutes(contract)
                if window_minutes <= 0:
                    # Unparseable description — skip rather than fall back
                    self._monitor.record_signal_skip(
                        cid, "window_parse_failed"
                    )
                    continue
                minutes_elapsed = max(0, window_minutes - minutes_left)

                if minutes_elapsed < 0.5:
                    continue  # too early in the window

                # Resolve the underlying asset price for this contract
                asset = getattr(contract, "asset", "BTC")
                asset_price = self._price_feed.get_price(asset) if asset != "BTC" else btc
                if asset_price <= 0:
                    continue

                # Use the correct per-asset momentum detector
                asset_momentum = self._get_momentum(asset)

                # Set open price if first time seeing this contract
                if asset_momentum.get_open_price(cid) is None:
                    asset_momentum.set_window_open(cid, asset_price)

                # ── Cross-asset reference signal ────────────────────
                # For non-BTC contracts, compute BTC momentum as a leading
                # indicator and pass it as a reference signal + a boost to
                # the decision function.
                btc_ref_signal = None
                cross_boost = 0.0
                if (
                    asset != "BTC"
                    and settings.candle_cross_asset_enabled
                    and btc > 0
                ):
                    # Correlation gate: skip cross-asset boost when BTC-asset
                    # correlation drops below threshold (idiosyncratic events)
                    corr = self._price_feed.rolling_correlation(asset, window_seconds=30.0)
                    if corr >= settings.candle_cross_asset_min_correlation:
                        btc_ref_signal = self._momentum.detect(
                            contract_id=f"__btc_ref_{cid}",
                            window_start_ago_minutes=minutes_elapsed,
                            minutes_remaining=minutes_left,
                            current_price=btc,
                        )
                        if btc_ref_signal and btc_ref_signal.confidence >= 0.60:
                            cross_boost = settings.candle_cross_asset_confidence_boost

                # Detect momentum for the contract's own asset
                signal = asset_momentum.detect(
                    contract_id=cid,
                    window_start_ago_minutes=minutes_elapsed,
                    minutes_remaining=minutes_left,
                    current_price=asset_price,
                    reference_signal=btc_ref_signal,
                )

                if not signal:
                    continue

                # ── Pure decision function (shared with backtest) ──
                # Same code path runs in live and L2 backtest — see
                # polycrossarb.crypto.decision.decide_candle_trade
                decision = decide_candle_trade(
                    signal=signal,
                    minutes_elapsed=minutes_elapsed,
                    minutes_remaining=minutes_left,
                    window_minutes=window_minutes,
                    up_price=contract.up_price,
                    down_price=contract.down_price,
                    btc_price=asset_price,
                    open_btc=signal.open_price,
                    implied_vol=self._price_feed.implied_volatility,
                    min_confidence=self._min_confidence,
                    min_edge=self._min_edge,
                    skip_dead_zone=self._skip_dead_zone,
                    zone_config=self._zone_config,
                    cross_asset_boost=cross_boost,
                )

                if isinstance(decision, SkipReason):
                    self._monitor.record_signal_skip(
                        cid, f"{decision.reason}_{decision.zone}_{decision.detail}"
                    )
                    continue

                # Decision is a CandleDecision — proceed to execute
                if abs(decision.yes_no_vig) > 0.02:
                    log.info("candle.mispricing",
                             cid=cid[:16],
                             up=f"${contract.up_price:.3f}",
                             down=f"${contract.down_price:.3f}",
                             vig=f"{decision.yes_no_vig:+.3f}")

                token_id = contract.up_token_id if decision.direction == "up" else contract.down_token_id
                market_price = decision.market_price
                fair_value = decision.fair_value
                edge = decision.edge
                zone = decision.zone

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

            # 100ms interval — 10Hz evaluation. Lowered from 500ms (2Hz) to
            # reduce detection lag. For paper mode this cuts avg detection
            # latency from 250ms to 50ms. Live mode uses Rust event-driven
            # path (CLOB_DIRECT=1) which has no polling latency at all.
            elapsed = time.time() - t0
            if elapsed < 0.1:
                await asyncio.sleep(0.1 - elapsed)

    def _estimate_window_minutes(self, contract: CandleContract) -> float:
        """Estimate the window length from the description.

        Returns the parsed minute count, or 0.0 if the description does
        not match a known shape. Callers must treat 0.0 as "skip this
        contract" rather than defaulting to a guessed window — silently
        bucketing every unparseable contract into 15m has caused stale
        windows in past traces.
        """
        desc = contract.window_description.lower()
        if "am-" in desc or "pm-" in desc:
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
        # Hourly contracts have a single hour token in the question
        if any(f"{h}am" in desc or f"{h}pm" in desc for h in range(1, 13)):
            return 60
        log.warning("candle.window_parse_failed",
                    cid=contract.market.condition_id[:16],
                    desc=contract.window_description[:60])
        return 0.0

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

        # ── Volatility regime sizing ────────────────────────────────
        # During HIGH/EXTREME vol, MM lag widens → more edge per trade.
        # Scale position up to capture the opportunity.
        short_vol = self._price_feed.short_term_vol(window_seconds=900.0)
        baseline_vol = self._price_feed.volatility
        vol_regime = classify_vol_regime(short_vol, baseline_vol)
        if vol_regime == VolatilityRegime.HIGH:
            position *= settings.candle_vol_high_multiplier
        elif vol_regime == VolatilityRegime.EXTREME:
            position *= settings.candle_vol_extreme_multiplier

        # Cap by available capital (prevent overcommitting across concurrent trades)
        position = min(position, self._risk.available_capital)

        if position < 1.0:  # Polymarket rejects orders below $1
            return

        if market_price <= 0.01 or market_price >= 0.99:
            return  # price out of tradeable range

        shares = position / market_price

        if self.mode == ExecutionMode.PAPER:
            # Realistic paper model: blend maker (0% fee, 65% fill) and
            # taker (2% effective fee, 35% fill) to match expected live
            # execution with prefer_maker enabled.
            if settings.candle_prefer_maker:
                # Maker fill: best_ask - 1 tick, 0% fee
                slippage_bps = -10  # negative = price improvement
                fee_rate = 0.0
            else:
                # Taker fill: 1 tick adverse, ~2% effective fee
                slippage_bps = 30  # 30 bps taker impact
                fee_rate = 0.02    # ~2% effective taker fee at typical prices
            slipped_price = market_price * (1 + slippage_bps / 10000)
            slipped_price = max(0.01, min(slipped_price, 0.99))
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
                self._persist_paper_positions()
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
            "vol_regime": vol_regime.value,
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
