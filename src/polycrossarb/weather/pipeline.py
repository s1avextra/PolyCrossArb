"""Weather information-edge trading pipeline.

Parallel pipeline that:
  1. Scans Polymarket for weather temperature markets
  2. Polls free weather APIs for actual temperatures
  3. Predicts winning brackets with confidence scoring
  4. Executes trades when confidence exceeds threshold
  5. Cycles through timezones 24/7

Runs independently of the arb pipeline. Shares RiskManager and executor.
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
from polycrossarb.execution.executor import ExecutionMode, PaperExecutor, LiveExecutor
from polycrossarb.risk.manager import RiskManager
from polycrossarb.weather.data_sources import WeatherAggregator
from polycrossarb.weather.predictor import BracketPrediction, predict_outcome
from polycrossarb.weather.scanner import WeatherEvent, scan_weather_markets
from polycrossarb.weather.solver import WeatherTradeOrder, size_weather_trade
from polycrossarb.weather.timing import get_trading_priority

log = structlog.get_logger(__name__)


class WeatherPipeline:
    """Information-edge trading pipeline for weather markets."""

    def __init__(
        self,
        mode: ExecutionMode = ExecutionMode.PAPER,
        log_dir: str = "logs",
        risk_manager: RiskManager | None = None,
    ):
        self.mode = mode
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)

        self._client = PolymarketClient()
        self._risk = risk_manager or RiskManager()

        # Live executor for single-leg trades
        if mode == ExecutionMode.LIVE:
            from polycrossarb.execution.executor import SingleLegExecutor
            self._executor = SingleLegExecutor(self._risk)
        else:
            self._executor = None
        self._weather = WeatherAggregator(
            owm_api_key=getattr(settings, 'openweathermap_api_key', ''),
        )

        self._events: list[WeatherEvent] = []
        self._predictions: dict[str, BracketPrediction] = {}
        self._traded_events: set[str] = set()  # don't trade same event twice
        self._trade_count = 0
        self._total_profit = 0.0

        self._running = False
        self._MARKET_REFRESH = 300    # refresh market list every 5 min
        self._WEATHER_POLL = 30       # poll weather every 30s
        self._PREDICT_INTERVAL = 60   # re-evaluate predictions every 60s

    async def run(self) -> None:
        """Run the weather pipeline."""
        self._running = True
        log.info("weather.start", mode=self.mode, bankroll=f"${self._risk.effective_bankroll:.2f}")

        await self._refresh_markets()

        try:
            await asyncio.gather(
                self._weather_poll_loop(),
                self._prediction_loop(),
                self._market_refresh_loop(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self._weather.close()
            await self._client.close()
            self._running = False

            log.info(
                "weather.stopped",
                trades=self._trade_count,
                predictions=len(self._predictions),
                profit=f"${self._total_profit:.2f}",
            )

    def stop(self):
        self._running = False

    async def _refresh_markets(self) -> None:
        """Fetch all markets and identify weather events."""
        markets = await self._client.fetch_all_active_markets(min_liquidity=0)
        self._events = scan_weather_markets(markets)
        log.info("weather.scan", events=len(self._events),
                 brackets=sum(e.n_brackets for e in self._events))

    async def _market_refresh_loop(self) -> None:
        """Periodically refresh market list to catch new events."""
        while self._running:
            await asyncio.sleep(self._MARKET_REFRESH)
            if self._running:
                try:
                    await self._refresh_markets()
                except Exception:
                    log.exception("weather.refresh_error")

    async def _weather_poll_loop(self) -> None:
        """Poll weather data for active cities."""
        while self._running:
            try:
                priority = get_trading_priority(self._events)
                # Only poll top 10 most urgent cities
                for event in priority[:10]:
                    if not self._running:
                        break
                    if event.city_config:
                        reading = await self._weather.fetch(event.city_config)
                        if reading:
                            log.debug("weather.reading", city=event.city,
                                      temp=f"{reading.current_temp}°{reading.unit}",
                                      max_today=f"{reading.max_today}°{reading.unit}")
            except Exception:
                log.exception("weather.poll_error")

            await asyncio.sleep(self._WEATHER_POLL)

    async def _prediction_loop(self) -> None:
        """Evaluate predictions and execute trades."""
        while self._running:
            await asyncio.sleep(self._PREDICT_INTERVAL)
            if not self._running:
                break

            try:
                await self._evaluate_all()
            except Exception:
                log.exception("weather.predict_error")

    async def _evaluate_all(self) -> None:
        """Evaluate all active weather events for trading opportunities."""
        priority = get_trading_priority(self._events)

        for event in priority[:20]:
            if event.event_id in self._traded_events:
                continue

            if not event.city_config:
                continue

            # ONLY trade events resolving within 12 hours
            # This is where the edge lives — we know the temperature,
            # the market is slow to update, capital returns fast
            if event.resolution_utc:
                hours_left = (event.resolution_utc - datetime.now(timezone.utc)).total_seconds() / 3600
                if hours_left > 12 or hours_left < 0:
                    continue

            # Get latest temperature reading
            reading = await self._weather.fetch(event.city_config)
            if not reading:
                continue

            # Predict winning bracket
            prediction = predict_outcome(event, reading)
            if not prediction:
                continue

            self._predictions[event.event_id] = prediction
            min_confidence = getattr(settings, 'min_weather_confidence', 0.70)

            if prediction.confidence < min_confidence:
                continue

            # Size the trade
            max_pct = getattr(settings, 'max_weather_position_pct', 0.20)
            order = size_weather_trade(
                prediction,
                bankroll=self._risk.effective_bankroll,
                max_position_pct=max_pct,
                kelly_fraction=settings.kelly_fraction,
            )

            if not order:
                continue

            # Execute
            await self._execute_weather_trade(event, prediction, order)

    async def _execute_weather_trade(
        self,
        event: WeatherEvent,
        prediction: BracketPrediction,
        order: WeatherTradeOrder,
    ) -> None:
        """Execute a weather trade (paper or live)."""
        if self.mode == ExecutionMode.PAPER:
            # Paper: log the trade
            log.info(
                "weather.trade.paper",
                city=event.city,
                date=event.date,
                bracket=order.bracket,
                confidence=f"{order.confidence:.0%}",
                price=f"${order.price:.3f}",
                size=f"{order.size:.0f}",
                profit=f"${order.expected_profit:.2f}",
                reason=prediction.reason,
            )
            self._trade_count += 1
            self._total_profit += order.expected_profit
            self._traded_events.add(event.event_id)

        else:
            # Live: execute via SingleLegExecutor
            if not self._executor:
                log.error("weather: no executor configured for live mode")
                return

            result = await self._executor.execute_single(
                token_id=order.token_id,
                side="buy",
                price=order.price,
                size=order.size,
                neg_risk=order.neg_risk,
                event_id=event.event_id,
            )

            if result.success:
                self._trade_count += 1
                self._total_profit += order.expected_profit
                self._traded_events.add(event.event_id)
                log.info(
                    "weather.trade.filled",
                    city=event.city,
                    bracket=order.bracket,
                    fill_price=f"${result.fill_price:.4f}",
                    slippage=f"${result.slippage:.4f}",
                    cost=f"${result.cost:.2f}",
                    confidence=f"{order.confidence:.0%}",
                )
            else:
                log.warning("weather.trade.failed", city=event.city,
                            bracket=order.bracket, error=result.error)

        # Log to file
        self._log_trade(event, prediction, order)

    def _log_trade(self, event, prediction, order):
        """Write trade to log file."""
        entry = {
            "timestamp": time.time(),
            "city": event.city,
            "date": event.date,
            "bracket": order.bracket,
            "confidence": round(order.confidence, 4),
            "price": round(order.price, 4),
            "size": round(order.size, 1),
            "expected_profit": round(order.expected_profit, 4),
            "max_temp": round(prediction.current_max_temp, 1),
            "hours_past_peak": round(prediction.hours_past_peak, 1),
            "hours_until_resolution": round(prediction.hours_until_resolution, 1),
            "reason": prediction.reason,
            "mode": self.mode,
        }
        log_file = self.log_dir / "weather_trades.jsonl"
        with open(log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def status(self) -> dict:
        return {
            "mode": self.mode,
            "events": len(self._events),
            "predictions": len(self._predictions),
            "trades": self._trade_count,
            "traded_events": len(self._traded_events),
            "total_profit": round(self._total_profit, 2),
            **self._risk.summary(),
        }
