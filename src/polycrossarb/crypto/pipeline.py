"""Crypto cross-exchange arbitrage pipeline.

Replicates the Rust engine's logic in Python:
  1. Real-time BTC price from Binance WebSocket
  2. Scan Polymarket for BTC milestone contracts
  3. Compute fair value using Black-Scholes
  4. When divergence > threshold, execute
  5. Repeat at high frequency

The edge: BTC price moves on exchanges ~100ms before Polymarket
contract prices adjust. We compute fair value faster than the
market makers can update their quotes.
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
from polycrossarb.execution.executor import ExecutionMode
from polycrossarb.risk.manager import RiskManager
from polycrossarb.crypto.fair_value import compute_fair_value
from polycrossarb.crypto.price_feed import CryptoPriceFeed
from polycrossarb.crypto.scanner import CryptoContract, scan_crypto_contracts

log = structlog.get_logger(__name__)


class CryptoPipeline:
    """Cross-exchange crypto arbitrage pipeline."""

    def __init__(
        self,
        mode: ExecutionMode = ExecutionMode.PAPER,
        log_dir: str = "logs",
        min_edge: float = 0.02,
        scan_interval: float = 1.0,
        risk_manager: RiskManager | None = None,
    ):
        self.mode = mode
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)

        self._client = PolymarketClient()
        self._risk = risk_manager or RiskManager()
        self._price_feed = CryptoPriceFeed()

        if mode == ExecutionMode.LIVE:
            from polycrossarb.execution.executor import SingleLegExecutor
            self._executor = SingleLegExecutor(self._risk)
        else:
            self._executor = None

        self._contracts: list[CryptoContract] = []
        self._min_edge = min_edge
        self._scan_interval = scan_interval
        self._traded: set[str] = set()

        self._trade_count = 0
        self._total_profit = 0.0
        self._running = False

        # Stats
        self._cycles = 0
        self._edges_found = 0

    async def run(self) -> None:
        """Run the crypto arb pipeline."""
        self._running = True
        log.info("crypto.start", mode=self.mode, min_edge=f"{self._min_edge:.1%}")

        # Fetch Polymarket crypto contracts
        await self._refresh_contracts()

        # Start price feed and arb loop concurrently
        try:
            await asyncio.gather(
                self._price_feed.start(),
                self._arb_loop(),
                self._contract_refresh_loop(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            self._price_feed.stop()
            await self._client.close()
            self._running = False

            log.info(
                "crypto.stopped",
                cycles=self._cycles,
                edges_found=self._edges_found,
                trades=self._trade_count,
                profit=f"${self._total_profit:.2f}",
            )

    def stop(self):
        self._running = False
        self._price_feed.stop()

    async def _refresh_contracts(self):
        """Fetch all Polymarket markets and find crypto contracts."""
        markets = await self._client.fetch_all_active_markets(min_liquidity=0)
        self._contracts = scan_crypto_contracts(markets)
        log.info("crypto.scan", contracts=len(self._contracts))

    async def _contract_refresh_loop(self):
        """Refresh contract list every 5 minutes."""
        while self._running:
            await asyncio.sleep(300)
            if self._running:
                try:
                    await self._refresh_contracts()
                except Exception:
                    log.exception("crypto.refresh_error")

    async def _arb_loop(self):
        """Main arb loop: evaluate all contracts against fair value."""
        # Wait for first price
        while self._running and self._price_feed.btc_price == 0:
            await asyncio.sleep(0.5)

        log.info("crypto.price_ready", btc=f"${self._price_feed.btc_price:,.2f}",
                 vol=f"{self._price_feed.volatility:.1%}")

        while self._running:
            self._cycles += 1
            btc = self._price_feed.btc_price
            vol = self._price_feed.implied_volatility

            if btc <= 0:
                await asyncio.sleep(self._scan_interval)
                continue

            now = datetime.now(timezone.utc)
            best_edge = 0.0
            best_contract: CryptoContract | None = None
            best_fair: float = 0.0

            for contract in self._contracts:
                if contract.market.condition_id in self._traded:
                    continue

                # FILTER: skip low volume
                if contract.volume < 100:
                    continue

                # Calculate time to expiry
                try:
                    if contract.market.end_date:
                        end = datetime.fromisoformat(
                            contract.market.end_date.replace("Z", "+00:00")
                        )
                        hours_left = (end - now).total_seconds() / 3600
                        days = max(0.001, hours_left / 24)
                    else:
                        continue  # no expiry = skip
                except (ValueError, TypeError):
                    continue

                # CORE FILTER: only trade contracts resolving within 24 hours
                # This is where the edge lives — fast capital turnover
                if hours_left > 24 or hours_left < 0.1:  # skip >24h and <6min
                    continue

                # Only BTC for now (we have the price feed)
                if contract.asset != "BTC":
                    continue

                strike = contract.strike
                price_ratio = strike / btc if btc > 0 else 999

                # STRATEGY: buy near-certain outcomes that the market underprices
                #
                # BTC at $66,666. Contract: "BTC above $60,000 in 4 hours?"
                # Fair value: ~99% (BTC won't drop 10% in 4 hours)
                # Market price: maybe 92-95% (stale or risk-adjusted)
                # Edge: 4-7% on a near-certain outcome
                #
                # We ONLY buy contracts where:
                #   - BTC is already well above the strike (ratio < 0.95)
                #   - OR BTC is well below the strike and we buy NO (ratio > 1.05)
                # These are the "obvious" outcomes the market is slow to price

                if contract.direction == "above":
                    if price_ratio < 0.95:
                        # BTC is 5%+ above strike — very likely to stay above
                        # Fair value is high (90-99%)
                        pass  # eligible
                    elif price_ratio > 1.05:
                        # BTC is 5%+ below strike — unlikely to reach it
                        # Fair value is low — buy NO if market overprices YES
                        pass  # eligible
                    else:
                        # Strike is near current price — coin flip, skip
                        continue

                # Compute fair value
                fv = compute_fair_value(
                    btc_price=btc,
                    strike=strike,
                    days_to_expiry=days,
                    volatility=vol,
                    market_price=contract.yes_price,
                )

                # FILTER: skip if fair value is in the uncertain zone (30-70%)
                # We only want near-certain outcomes
                if 0.30 < fv.fair_price < 0.70:
                    continue

                # FILTER: minimum edge 2%
                if abs(fv.edge_pct) < self._min_edge:
                    continue

                # FILTER: skip stale extremes (already priced in)
                if contract.yes_price < 0.02 or contract.yes_price > 0.98:
                    continue

                if abs(fv.edge_pct) > abs(best_edge):
                    best_edge = fv.edge_pct
                    best_contract = contract
                    best_fair = fv.fair_price

            # Log best opportunity every 10 cycles
            if self._cycles % 10 == 0:
                agg = self._price_feed.get_aggregated()
                log_data = {
                    "cycle": self._cycles,
                    "btc": f"${btc:,.0f}",
                    "sources": agg.n_sources,
                    "spread": f"${agg.spread:.2f}",
                    "vol": f"{vol:.1%}",
                    "latency": f"{agg.staleness_ms:.0f}ms",
                }
                if best_contract:
                    log_data.update({
                        "best_edge": f"{best_edge:+.2%}",
                        "contract": best_contract.market.question[:45],
                        "mkt": f"${best_contract.yes_price:.3f}",
                        "fair": f"${best_fair:.3f}",
                    })
                log.info("crypto.scan", **log_data)

            # Execute if edge exceeds threshold
            if best_contract and abs(best_edge) >= self._min_edge:
                self._edges_found += 1
                await self._execute_trade(best_contract, best_fair, best_edge)

            await asyncio.sleep(self._scan_interval)

    async def _execute_trade(self, contract: CryptoContract, fair_value: float, edge: float):
        """Execute a trade on a mispriced contract."""
        # Determine direction
        if edge > 0:
            # Fair value > market price → contract is undervalued → BUY YES
            side = "buy"
            price = contract.yes_price
            expected_profit_per_share = fair_value - price
        else:
            # Fair value < market price → contract is overvalued → BUY NO
            side = "buy_no"
            price = 1 - contract.yes_price
            expected_profit_per_share = contract.yes_price - fair_value

        # Size using Kelly
        confidence = min(0.95, 0.5 + abs(edge))  # map edge to confidence
        kelly_raw = (confidence - price) / max(1 - price, 0.01) if price < 0.99 else 0
        kelly_adj = max(0, kelly_raw * settings.kelly_fraction)
        position = min(
            kelly_adj * self._risk.effective_bankroll,
            self._risk.effective_bankroll * 0.10,  # max 10% per trade
        )

        if position < 1.0:
            return

        shares = position / price
        expected_profit = shares * expected_profit_per_share

        if self.mode == ExecutionMode.PAPER:
            log.info(
                "crypto.trade.paper",
                side=side,
                contract=contract.market.question[:45],
                strike=f"${contract.strike:,.0f}",
                market_price=f"${contract.yes_price:.3f}",
                fair=f"${fair_value:.3f}",
                edge=f"{edge:+.2%}",
                shares=f"{shares:.1f}",
                cost=f"${position:.2f}",
                profit=f"${expected_profit:.2f}",
            )
            self._trade_count += 1
            self._total_profit += expected_profit
            self._traded.add(contract.market.condition_id)
        else:
            if not self._executor:
                log.error("crypto: no executor for live mode")
                return

            # Determine token_id based on direction
            token_id = contract.token_id  # YES token
            if side == "buy_no" and len(contract.market.outcomes) > 1:
                token_id = contract.market.outcomes[1].token_id

            result = await self._executor.execute_single(
                token_id=token_id,
                side="buy",
                price=price,
                size=round(shares, 1),
                neg_risk=contract.market.neg_risk,
                event_id=contract.event_id,
            )

            if result.success:
                self._trade_count += 1
                self._total_profit += expected_profit
                self._traded.add(contract.market.condition_id)
                log.info("crypto.trade.filled", contract=contract.market.question[:40],
                         fill_price=f"${result.fill_price:.4f}", cost=f"${result.cost:.2f}")
            else:
                log.warning("crypto.trade.failed", error=result.error)

        # Log to file
        entry = {
            "timestamp": time.time(),
            "asset": contract.asset,
            "strike": contract.strike,
            "direction": contract.direction,
            "side": side,
            "market_price": contract.yes_price,
            "fair_value": fair_value,
            "edge": edge,
            "shares": shares,
            "cost": position,
            "expected_profit": expected_profit,
            "btc_price": self._price_feed.btc_price,
            "volatility": self._price_feed.volatility,
        }
        log_file = self.log_dir / "crypto_trades.jsonl"
        with open(log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def status(self) -> dict:
        return {
            "mode": self.mode,
            "btc_price": self._price_feed.btc_price,
            "volatility": f"{self._price_feed.volatility:.1%}",
            "contracts": len(self._contracts),
            "cycles": self._cycles,
            "edges_found": self._edges_found,
            "trades": self._trade_count,
            "total_profit": round(self._total_profit, 2),
            "price_sources": self._price_feed.sources,
        }
