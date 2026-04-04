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
from polycrossarb.crypto.candle_scanner import CandleContract, scan_candle_markets
from polycrossarb.crypto.momentum import MomentumDetector, MomentumSignal
from polycrossarb.crypto.price_feed import CryptoPriceFeed

log = structlog.get_logger(__name__)


class CandlePipeline:
    """High-frequency BTC candle trading pipeline."""

    def __init__(
        self,
        mode: ExecutionMode = ExecutionMode.PAPER,
        log_dir: str = "logs",
        min_confidence: float = 0.60,
        min_edge: float = 0.05,
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
        self._min_confidence = min_confidence
        self._min_edge = min_edge
        self._running = False

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
            )
        except asyncio.CancelledError:
            pass
        finally:
            self._price_feed.stop()
            await self._client.close()
            self._running = False
            log.info("candle.stopped",
                     trades=self._trade_count,
                     profit=f"${self._total_profit:.2f}")

    def stop(self):
        self._running = False
        self._price_feed.stop()

    async def _refresh_contracts(self):
        """Fetch candle contracts."""
        markets = await self._client.fetch_all_active_markets(min_liquidity=0)
        self._contracts = scan_candle_markets(markets, max_hours=1.0, min_liquidity=50)
        log.info("candle.scan", contracts=len(self._contracts))

    async def _contract_refresh_loop(self):
        """Refresh every 2 minutes to catch new candle windows."""
        while self._running:
            await asyncio.sleep(120)
            if self._running:
                try:
                    await self._refresh_contracts()
                except Exception:
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

            # Feed tick to momentum detector
            self._momentum.add_tick(btc)

            # Evaluate each candle contract
            now = datetime.now(timezone.utc)

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

                # Calculate window elapsed
                # Estimate window start from end time - typical window length
                # For 5min candle: window_start = end - 5min
                # For 15min: end - 15min
                # For 1hr: end - 60min
                window_minutes = self._estimate_window_minutes(contract)
                minutes_elapsed = max(0, window_minutes - minutes_left)

                if minutes_elapsed < 1:
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

                if signal.confidence < self._min_confidence:
                    continue

                # Check edge: is the market mispricing the direction?
                if signal.direction == "up":
                    market_price = contract.up_price
                    token_id = contract.up_token_id
                else:
                    market_price = contract.down_price
                    token_id = contract.down_token_id

                # Fair value: confidence that this direction wins
                fair_value = 0.5 + (signal.confidence - 0.5) * 0.8  # dampen confidence
                edge = fair_value - market_price

                if edge < self._min_edge:
                    continue

                # Execute trade
                await self._execute_candle_trade(contract, signal, token_id,
                                                  market_price, fair_value, edge)

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

            # 1-second interval
            elapsed = time.time() - t0
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)

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
    ):
        """Execute a candle trade."""
        # Position sizing
        bankroll = self._risk.effective_bankroll
        position = min(bankroll * 0.05, 10.0)  # max 5% or $10

        if position < 1.0:
            return

        shares = position / market_price

        if self.mode == ExecutionMode.PAPER:
            expected_profit = shares * (fair_value - market_price)
            log.info("candle.trade.paper",
                     direction=signal.direction,
                     window=contract.window_description[:25],
                     confidence=f"{signal.confidence:.0%}",
                     price=f"${market_price:.3f}",
                     fair=f"${fair_value:.3f}",
                     edge=f"{edge:+.1%}",
                     btc_change=f"${signal.price_change:+.0f}",
                     minutes_left=f"{signal.minutes_remaining:.1f}",
                     cost=f"${position:.2f}",
                     profit=f"${expected_profit:.2f}")

            self._trade_count += 1
            self._total_profit += expected_profit
            self._traded.add(contract.market.condition_id)

        elif self._executor:
            result = await self._executor.execute_single(
                token_id=token_id,
                side="buy",
                price=market_price,
                size=round(shares, 1),
                neg_risk=False,
                event_id=contract.market.event_id,
            )

            if result.success:
                self._trade_count += 1
                self._traded.add(contract.market.condition_id)
                log.info("candle.trade.filled",
                         direction=signal.direction,
                         fill=f"${result.fill_price:.3f}",
                         cost=f"${result.cost:.2f}")
            else:
                log.warning("candle.trade.failed", error=result.error)

        # Log to file
        entry = {
            "timestamp": time.time(),
            "direction": signal.direction,
            "window": contract.window_description,
            "confidence": round(signal.confidence, 4),
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
        return {
            "mode": self.mode,
            "contracts": len(self._contracts),
            "trades": self._trade_count,
            "total_profit": round(self._total_profit, 2),
            "btc_price": self._price_feed.btc_price,
            "sources": self._price_feed.n_live_sources,
        }
