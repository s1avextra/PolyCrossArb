"""Candle strategy adapter for L2 backtest engine.

Bridges:
  - L2BacktestEngine event stream (real PMXT order book updates)
  - BTCHistory (real historical BTC ticks)
  - CandleRegistry (token_id -> candle window metadata)
  - MomentumDetector (live strategy code, unchanged)
  - decide_candle_trade (live decision logic, unchanged)

The result is a strategy callback that the L2 engine can plug in to
replay any historical period and report what the live bot would have
done — using the SAME code paths as production.

This is the "code parity" pattern that gives 1:1 correspondence between
live and backtest, modulo (a) the fill model and (b) the latency model.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

from polymomentum.backtest.btc_history import BTCHistory
from polymomentum.backtest.candle_registry import CandleContract, CandleRegistry
from polymomentum.backtest.l2_replay import BacktestOrder, TokenBook
from polymomentum.crypto.decision import (
    CandleDecision,
    SkipReason,
    ZoneConfig,
    decide_candle_trade,
)
from polymomentum.crypto.momentum import MomentumDetector

log = logging.getLogger(__name__)


@dataclass
class WindowState:
    """Per-candle-window tracking state."""
    contract: CandleContract
    open_btc: float = 0.0
    last_btc: float = 0.0
    traded: bool = False
    decision: CandleDecision | None = None
    skip_history: list[SkipReason] = field(default_factory=list)


@dataclass
class StrategyConfig:
    """Tunable parameters for the candle backtest.

    ``zone_config`` parity: if None at construction, the adapter snaps to
    the *current* settings.candle_zone_* values via
    ``zone_config_from_settings()``. This guarantees a backtest run uses
    the same zone gates as the live bot would in the same .env, so .env
    tweaks can be A/B-tested in the backtest before deploying.
    """
    min_confidence: float = 0.60
    min_edge: float = 0.07
    noise_z_threshold: float = 0.3
    realized_vol: float = 0.50
    position_size_usd: float = 5.0
    fee_rate: float = 0.072       # taker fee (7.2%)
    maker_fee_rate: float = 0.0   # maker fee (0% on Polymarket)
    prefer_maker: bool = False    # if True, orders carry maker_fee_rate for MakerFillModel
    use_implied_vol: bool = False  # if True, vol is recomputed from BTC history
    vol_lookback_seconds: float = 3600.0
    skip_dead_zone: bool = True
    zone_config: ZoneConfig | None = None
    cross_asset_enabled: bool = False
    cross_asset_confidence_boost: float = 0.10


class CandleStrategyAdapter:
    """Wraps the live candle decision logic for the L2 backtest engine.

    The adapter is stateful per strategy instance:
      - Tracks per-window open BTC prices
      - Maintains a MomentumDetector instance
      - Records skip reasons per window for post-mortem analysis

    Usage:
        registry = CandleRegistry()
        registry.fetch_range(start, end)

        btc = BTCHistory()
        btc.load_csv("data/btcusdt_1s_7d.csv")

        adapter = CandleStrategyAdapter(
            registry=registry,
            btc_history=btc,
            config=StrategyConfig(min_edge=0.07),
        )

        engine = L2BacktestEngine(latency=preset_dublin_vps())
        engine.replay(
            start=start,
            end=end,
            token_ids=registry.all_token_ids(),
            strategy=adapter.on_event,
            fee_rate=config.fee_rate,
        )

        for window_id, state in adapter.windows.items():
            print(state.contract.window_label, state.decision, len(state.skip_history))
    """

    def __init__(
        self,
        registry: CandleRegistry,
        btc_history: BTCHistory,
        config: StrategyConfig | None = None,
        asset_histories: dict[str, BTCHistory] | None = None,
        strategy=None,
    ):
        self.registry = registry
        self.btc_history = btc_history
        self.config = config or StrategyConfig()
        # Optional swappable strategy — when None, uses decide_candle_trade
        # with the adapter's config. Strategies are defined in
        # polymomentum.backtest.strategies and plugged in by the harness.
        self.strategy = strategy

        # Optional per-asset histories for cross-asset backtesting.
        # Keys are asset names ("ETH", "SOL"). When present, non-BTC
        # contracts use their own asset's history for momentum instead
        # of BTC prices. Falls back to btc_history if not provided.
        self._asset_histories: dict[str, BTCHistory] = asset_histories or {}

        # Snap to live settings.candle_zone_* if the caller didn't pin a
        # ZoneConfig — keeps backtest semantics aligned with live .env.
        if self.config.zone_config is None:
            from polymomentum.crypto.decision import zone_config_from_settings
            self._zone_config = zone_config_from_settings()
        else:
            self._zone_config = self.config.zone_config

        # Per-asset momentum detectors — mirrors the live pipeline pattern
        self._momentum_detectors: dict[str, MomentumDetector] = {}
        self._noise_z = self.config.noise_z_threshold
        self._momentum = MomentumDetector(
            realized_vol=self.config.realized_vol,
            noise_z_threshold=self._noise_z,
        )
        self._momentum.set_realized_vol(self.config.realized_vol)
        self._momentum_detectors["BTC"] = self._momentum

        # Window state, keyed by condition_id
        self.windows: dict[str, WindowState] = {}

        # Throttle BTC tick feed to once per second per condition
        self._last_tick_ts: dict[str, float] = {}

        # Track last-seen mid for each token_id so we can use real
        # prices for both sides instead of inferring via 1.0 - x.
        self._token_mids: dict[str, float] = {}

        # Skip reason counter for analysis
        self.skip_counts: dict[str, int] = defaultdict(int)

        # Trade decisions (one per window — first signal that fires)
        self.decisions: list[CandleDecision] = []
        self.decisions_by_window: dict[str, CandleDecision] = {}

        # Stats
        self.events_processed = 0
        self.signals_evaluated = 0
        self.btc_lookups = 0

    def _get_window_state(self, contract: CandleContract) -> WindowState:
        cid = contract.condition_id
        if cid not in self.windows:
            self.windows[cid] = WindowState(contract=contract)
        return self.windows[cid]

    def _btc_at(self, ts_s: float) -> float:
        """Lookup BTC price from history, falling back to 0 if missing."""
        self.btc_lookups += 1
        return self.btc_history.price_at_seconds(ts_s)

    def _get_momentum(self, asset: str) -> MomentumDetector:
        """Get or create a momentum detector for the given asset."""
        if asset not in self._momentum_detectors:
            det = MomentumDetector(
                realized_vol=self.config.realized_vol,
                noise_z_threshold=self._noise_z,
            )
            det.set_realized_vol(self.config.realized_vol)
            self._momentum_detectors[asset] = det
        return self._momentum_detectors[asset]

    def _asset_price_at(self, asset: str, ts_s: float) -> float:
        """Get asset price at timestamp. Uses asset-specific history if available."""
        if asset != "BTC" and asset in self._asset_histories:
            return self._asset_histories[asset].price_at_seconds(ts_s)
        return self.btc_history.price_at_seconds(ts_s)

    def _vol_at(self, ts_s: float) -> float:
        """Get vol estimate at timestamp."""
        if self.config.use_implied_vol:
            return self.btc_history.realized_vol_at(
                int(ts_s * 1000),
                lookback_seconds=self.config.vol_lookback_seconds,
            )
        return self.config.realized_vol

    def on_event(
        self,
        ts_s: float,
        token_id: str,
        book: TokenBook,
        history: dict[str, list[tuple[float, float]]],
    ) -> list[BacktestOrder]:
        """Strategy callback for L2BacktestEngine.

        Called on every PMXT event. Decides whether to place a trade.
        """
        self.events_processed += 1

        contract = self.registry.lookup(token_id)
        if contract is None:
            return []

        # Skip if we've already traded this window
        window = self._get_window_state(contract)
        if window.traded:
            return []

        # Skip if outside the window
        if ts_s >= contract.end_time_s:
            return []
        if ts_s < contract.start_time_s:
            return []

        # Skip if book has no touch
        if book.best_bid <= 0 or book.best_ask <= 0:
            return []

        minutes_remaining = (contract.end_time_s - ts_s) / 60.0
        minutes_elapsed = contract.window_minutes - minutes_remaining

        # Need at least 30 seconds of window elapsed for momentum to work.
        # Floor at 5s remaining (0.083 min) — must be strictly below the
        # terminal zone threshold so terminal-zone trades can fire.
        if minutes_elapsed < 0.5:
            return []
        if minutes_remaining <= 0.083:
            return []

        # Resolve the asset for this contract
        asset = getattr(contract, "asset", "BTC")

        # Get BTC price (always needed for reference / fallback)
        btc_price = self._btc_at(ts_s)
        if btc_price <= 0:
            return []

        # Get the asset's own price (ETH/SOL from their history, or BTC)
        asset_price = self._asset_price_at(asset, ts_s)
        if asset_price <= 0:
            asset_price = btc_price  # fallback

        # Initialize window open price from the correct asset on first sight
        if window.open_btc == 0:
            open_p = self._asset_price_at(asset, contract.start_time_s)
            if open_p == 0:
                open_p = asset_price
            window.open_btc = open_p
            asset_det = self._get_momentum(asset)
            asset_det.set_window_open(contract.condition_id, open_p)

        window.last_btc = asset_price

        # Throttle the tick feed + decision pass at 10Hz per condition to
        # match the live scan loop (100ms interval). Without this gate,
        # decide_candle_trade gets called on every L2 event (16M+ per
        # replay-hour) — 99% wasted work.
        last_tick = self._last_tick_ts.get(contract.condition_id, 0.0)
        if ts_s - last_tick < 0.1:
            return []
        # Feed the correct asset's tick to its own detector
        asset_det = self._get_momentum(asset)
        asset_det.add_tick(asset_price, timestamp=ts_s)
        # Feed BTC ticks for cross-asset reference (only when enabled)
        if asset != "BTC" and self.config.cross_asset_enabled:
            self._momentum.add_tick(btc_price, timestamp=ts_s)
        self._last_tick_ts[contract.condition_id] = ts_s

        # Update swappable strategy's internal state (e.g. EWMA vol).
        # Only fed BTC ticks since the strategy is regime-classifying BTC.
        # After on_tick, if the strategy exposes a current_vol, push it
        # into the momentum detector so z-score uses the fresh estimate.
        if self.strategy is not None and asset == "BTC":
            self.strategy.on_tick(ts_s, asset_price)
            get_vol = getattr(self.strategy, "get_current_vol", None)
            if get_vol is not None:
                v = get_vol()
                if v is not None and v > 0:
                    asset_det.set_realized_vol(v)

        # ── Cross-asset: compute BTC reference signal for non-BTC ──
        btc_ref_signal = None
        cross_boost = 0.0
        if asset != "BTC" and self.config.cross_asset_enabled and btc_price > 0:
            btc_ref_signal = self._momentum.detect(
                contract_id=f"__btc_ref_{contract.condition_id}",
                window_start_ago_minutes=minutes_elapsed,
                minutes_remaining=minutes_remaining,
                current_price=btc_price,
                now_ts=ts_s,
            )
            if btc_ref_signal and btc_ref_signal.confidence >= 0.60:
                cross_boost = self.config.cross_asset_confidence_boost

        # Compute the momentum signal from the asset's own detector
        signal = asset_det.detect(
            contract_id=contract.condition_id,
            window_start_ago_minutes=minutes_elapsed,
            minutes_remaining=minutes_remaining,
            current_price=asset_price,
            now_ts=ts_s,
            reference_signal=btc_ref_signal,
        )
        if signal is None:
            return []

        self.signals_evaluated += 1

        # Get current Up/Down prices from books.
        # Track both sides independently so we use real prices (with vig)
        # instead of inferring via 1.0 - x (which ignores ~4% vig).
        self._token_mids[token_id] = book.mid()
        if token_id == contract.up_token_id:
            up_mid = book.mid()
            down_mid = self._token_mids.get(contract.down_token_id, 1.0 - up_mid)
        else:
            down_mid = book.mid()
            up_mid = self._token_mids.get(contract.up_token_id, 1.0 - down_mid)

        if up_mid <= 0 or down_mid <= 0:
            return []

        # Run either the swappable strategy or the default decision function.
        if self.strategy is not None:
            decision = self.strategy.decide(
                signal=signal,
                minutes_elapsed=minutes_elapsed,
                minutes_remaining=minutes_remaining,
                window_minutes=contract.window_minutes,
                up_price=up_mid,
                down_price=down_mid,
                btc_price=asset_price,
                open_btc=window.open_btc,
                implied_vol=self._vol_at(ts_s),
                cross_asset_boost=cross_boost,
            )
        else:
            decision = decide_candle_trade(
                signal=signal,
                minutes_elapsed=minutes_elapsed,
                minutes_remaining=minutes_remaining,
                window_minutes=contract.window_minutes,
                up_price=up_mid,
                down_price=down_mid,
                btc_price=asset_price,
                open_btc=window.open_btc,
                implied_vol=self._vol_at(ts_s),
                min_confidence=self.config.min_confidence,
                min_edge=self.config.min_edge,
                skip_dead_zone=self.config.skip_dead_zone,
                zone_config=self._zone_config,
                cross_asset_boost=cross_boost,
            )

        if isinstance(decision, SkipReason):
            window.skip_history.append(decision)
            self.skip_counts[f"{decision.reason}_{decision.zone}"] += 1
            return []

        # We have a trade signal — pick the right token and place an order
        if decision.direction == "up":
            chosen_token = contract.up_token_id
        else:
            chosen_token = contract.down_token_id

        size = self.config.position_size_usd / max(decision.market_price, 0.01)

        order = BacktestOrder(
            timestamp=ts_s,
            token_id=chosen_token,
            side="buy",
            size=round(size, 1),
            order_type="market",
            fee_rate=self.config.fee_rate,
            maker_fee_rate=self.config.maker_fee_rate if self.config.prefer_maker else self.config.fee_rate,
        )

        window.traded = True
        window.decision = decision
        self.decisions.append(decision)
        self.decisions_by_window[contract.condition_id] = decision

        return [order]

    def summary(self) -> dict:
        """Pre-resolution summary (counts only — call resolve_pnl for P&L)."""
        n_traded = sum(1 for w in self.windows.values() if w.traded)
        skip_top = sorted(self.skip_counts.items(), key=lambda x: -x[1])[:10]
        zones = defaultdict(int)
        for d in self.decisions:
            zones[d.zone] += 1
        return {
            "events_processed": self.events_processed,
            "signals_evaluated": self.signals_evaluated,
            "btc_lookups": self.btc_lookups,
            "windows_seen": len(self.windows),
            "windows_traded": n_traded,
            "decisions": len(self.decisions),
            "by_zone": dict(zones),
            "top_skip_reasons": skip_top,
        }
