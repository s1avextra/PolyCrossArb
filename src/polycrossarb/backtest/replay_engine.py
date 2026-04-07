"""Replay engine: run volatility-normalized momentum on historical candle windows.

For each window, feeds ticks one-by-one into the detector and checks
if/when a trade signal is generated. Uses z-score (vol-adjusted magnitude)
and 3-zone entry timing. Simulates entry at the market price implied by
the confidence level, then evaluates against ground truth.

NOTE: market price simulation is still synthetic (lag_factor model).
      For accurate backtesting, collect real Polymarket bid/ask data
      and replace _simulate_market_price() with actual order book prices.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from scipy.stats import norm

from polycrossarb.backtest.window_builder import CandleWindow
from polycrossarb.crypto.momentum import MomentumSignal

log = logging.getLogger(__name__)


@dataclass
class BacktestTrade:
    """A simulated trade from backtesting."""
    window_start_ms: int
    window_end_ms: int
    window_minutes: int
    entry_tick_ms: int
    direction: str          # predicted: "up" or "down"
    actual_outcome: str     # ground truth: "up" or "down"
    won: bool
    confidence: float
    entry_price: float      # simulated market price for the token
    fair_value: float       # our estimate of true probability
    edge: float             # fair_value - entry_price
    pnl: float              # win: (1 - entry_price) * size, lose: -entry_price * size
    btc_open: float
    btc_at_entry: float
    btc_close: float
    btc_change: float
    minutes_remaining: float
    consistency: float
    z_score: float = 0.0
    zone: str = ""


@dataclass
class BacktestConfig:
    """Parameters for the backtest."""
    min_confidence: float = 0.60
    min_edge: float = 0.05
    position_size: float = 10.0    # USD per trade
    market_impact_bps: float = 30  # 30 bps slippage
    fee_rate: float = 0.002        # 20 bps fee
    min_minutes_elapsed: float = 0.5  # reduced for early-zone trades
    max_minutes_remaining: float = 30.0
    confidence_dampen: float = 0.8  # dampen confidence → fair value
    realized_vol: float = 0.50     # annualized BTC volatility
    # Zone-specific thresholds
    early_min_z_score: float = 2.0
    early_min_confidence: float = 0.55
    primary_min_z_score: float = 1.0
    late_min_z_score: float = 0.5
    late_min_confidence: float = 0.65
    late_min_edge: float = 0.08
    # Market price model
    use_bs_market_price: bool = True  # Black-Scholes binary pricing
    mm_lag_seconds: float = 15.0      # market maker update delay


@dataclass
class BacktestResult:
    """Full backtest results."""
    trades: list[BacktestTrade] = field(default_factory=list)
    config: BacktestConfig = field(default_factory=BacktestConfig)

    @property
    def n_trades(self) -> int:
        return len(self.trades)

    @property
    def n_wins(self) -> int:
        return sum(1 for t in self.trades if t.won)

    @property
    def n_losses(self) -> int:
        return self.n_trades - self.n_wins

    @property
    def win_rate(self) -> float:
        return self.n_wins / max(self.n_trades, 1)

    @property
    def total_pnl(self) -> float:
        return sum(t.pnl for t in self.trades)

    @property
    def avg_edge(self) -> float:
        if not self.trades:
            return 0
        return sum(t.edge for t in self.trades) / len(self.trades)

    @property
    def avg_confidence(self) -> float:
        if not self.trades:
            return 0
        return sum(t.confidence for t in self.trades) / len(self.trades)


def _bs_binary_price(
    price_change: float,
    open_price: float,
    minutes_remaining: float,
    realized_vol: float,
) -> float:
    """Black-Scholes binary option fair price: P(BTC finishes above open).

    P(up) = Phi(drift / sigma_remaining)
    where drift = current BTC move, sigma_remaining = vol * sqrt(time_remaining).
    """
    if open_price <= 0 or minutes_remaining <= 0:
        return 0.50
    sigma_remaining = open_price * realized_vol * math.sqrt(minutes_remaining / 525600)
    sigma_remaining = max(sigma_remaining, 0.50)
    z = price_change / sigma_remaining
    return norm.cdf(z)


def _simulate_market_price_bs(
    ticks_in_window: list[tuple[int, float]],
    open_price: float,
    current_ts: int,
    minutes_remaining: float,
    realized_vol: float,
    mm_lag_seconds: float = 15.0,
    mm_spread: float = 0.04,
) -> float:
    """Simulate Polymarket market price using Black-Scholes with market maker lag.

    The edge in candle trading comes from the market maker being SLOW to update,
    not from them being wrong in their pricing model. They use the same BS math
    but see a delayed BTC price (they update every 5-30 seconds while we see
    prices every 100ms from 4 exchanges).

    This model:
      1. Finds what BTC price was mm_lag_seconds ago
      2. Prices the token using BS on that lagged price
      3. Adds market maker spread

    Args:
        ticks_in_window: Historical ticks [(timestamp_ms, price), ...]
        open_price: BTC price at window open
        current_ts: Current tick timestamp (ms)
        minutes_remaining: Minutes until resolution
        realized_vol: Annualized BTC volatility
        mm_lag_seconds: How many seconds behind the market maker is
        mm_spread: Market maker half-spread (adverse to taker)
    """
    if open_price <= 0 or minutes_remaining <= 0:
        return 0.50

    # Find BTC price from mm_lag_seconds ago
    lag_cutoff_ms = current_ts - int(mm_lag_seconds * 1000)
    lagged_price = open_price  # default to open if no tick found
    for ts, price in reversed(ticks_in_window):
        if ts <= lag_cutoff_ms:
            lagged_price = price
            break

    lagged_change = lagged_price - open_price
    fair_prob_up = _bs_binary_price(lagged_change, open_price, minutes_remaining, realized_vol)

    # Market maker adds spread
    market_price = fair_prob_up + mm_spread
    return max(0.05, min(0.95, market_price))


def _simulate_market_price_legacy(confidence: float) -> float:
    """Legacy market price model (kept for comparison).

    WARNING: This model is self-referential — it generates market price
    from the same confidence used to compute edge, making the backtest
    tautological. Use _simulate_market_price_bs() instead.
    """
    lag_factor = 0.6
    market_price = 0.50 + (confidence - 0.50) * lag_factor
    return max(0.05, min(0.95, market_price))


def _compute_momentum(
    ticks_in_window: list[tuple[int, float]],
    open_price: float,
    current_price: float,
    minutes_elapsed: float,
    minutes_remaining: float,
    realized_vol: float = 0.50,
) -> MomentumSignal | None:
    """Compute vol-normalized momentum signal from historical ticks.

    Uses the same algorithm as the live MomentumDetector but operates
    on tick timestamps directly instead of wall-clock time.
    """
    if len(ticks_in_window) < 3 or minutes_remaining <= 0:
        return None

    price_change = current_price - open_price
    price_change_pct = price_change / open_price if open_price > 0 else 0
    direction = "up" if price_change >= 0 else "down"

    # Consistency + reversion count
    consistent = 0
    reversion_count = 0
    prev_side = None
    for i in range(1, len(ticks_in_window)):
        tick_dir = ticks_in_window[i][1] - ticks_in_window[i - 1][1]
        if (direction == "up" and tick_dir >= 0) or (direction == "down" and tick_dir <= 0):
            consistent += 1
        curr_side = ticks_in_window[i][1] >= open_price
        if prev_side is not None and curr_side != prev_side:
            reversion_count += 1
        prev_side = curr_side

    consistency = consistent / max(len(ticks_in_window) - 1, 1)

    total_window = minutes_elapsed + minutes_remaining

    # Volatility-normalized magnitude (z-score)
    sigma_window = open_price * realized_vol * math.sqrt(total_window / 525600)
    sigma_window = max(sigma_window, 1.0)
    z_score = abs(price_change) / sigma_window

    # Time factor
    time_factor = min(1.0, minutes_elapsed / total_window) if total_window > 0 else 0

    # Reversion penalty
    reversion_penalty = max(0.0, 1.0 - reversion_count * 0.05)

    # Z-score factor (saturate at z=3.0)
    z_factor = min(1.0, z_score / 3.0)

    # Confidence: reweighted from old 50/30/20 to 35/35/15/15
    confidence = (
        0.35 * time_factor +
        0.35 * z_factor +
        0.15 * consistency +
        0.15 * reversion_penalty
    )
    confidence = max(0.10, min(0.95, confidence))

    # Boost near resolution with strong vol-adjusted move
    if minutes_remaining < 2.0 and z_score > 1.0:
        confidence = min(0.95, confidence + 0.15)
    elif minutes_remaining < 1.0 and z_score > 0.5:
        confidence = min(0.95, confidence + 0.20)

    # Reduce if move is sub-0.3 sigma (noise)
    if z_score < 0.3:
        confidence *= 0.4

    return MomentumSignal(
        direction=direction,
        confidence=confidence,
        price_change=price_change,
        price_change_pct=price_change_pct,
        consistency=consistency,
        minutes_elapsed=minutes_elapsed,
        minutes_remaining=minutes_remaining,
        current_price=current_price,
        open_price=open_price,
        z_score=z_score,
        reversion_count=reversion_count,
    )


def replay_window(
    window: CandleWindow,
    config: BacktestConfig,
) -> BacktestTrade | None:
    """Replay momentum detection on a single candle window.

    Uses 3-zone entry timing and vol-normalized signals.
    """
    ticks = window.ticks
    if len(ticks) < 10:
        return None

    open_price = ticks[0][1]
    total_window = window.window_minutes

    for idx, (ts, price) in enumerate(ticks):
        elapsed_ms = ts - window.start_ms
        remaining_ms = window.end_ms - ts
        minutes_elapsed = elapsed_ms / 60000.0
        minutes_remaining = remaining_ms / 60000.0

        if minutes_elapsed < config.min_minutes_elapsed:
            continue
        if minutes_remaining > config.max_minutes_remaining:
            continue
        if minutes_remaining <= 0.5:
            continue

        ticks_so_far = [(t, p) for t, p in ticks[:idx + 1] if t >= window.start_ms]

        signal = _compute_momentum(
            ticks_in_window=ticks_so_far,
            open_price=open_price,
            current_price=price,
            minutes_elapsed=minutes_elapsed,
            minutes_remaining=minutes_remaining,
            realized_vol=config.realized_vol,
        )

        if not signal:
            continue

        # ── 3-Zone entry timing ────────────────────────────────
        elapsed_pct = minutes_elapsed / total_window if total_window > 0 else 1.0

        if elapsed_pct < 0.40:
            zone = "early"
            zone_min_conf = config.early_min_confidence
            zone_min_z = config.early_min_z_score
            zone_min_edge = config.min_edge
        elif elapsed_pct < 0.80:
            zone = "primary"
            zone_min_conf = config.min_confidence
            zone_min_z = config.primary_min_z_score
            zone_min_edge = config.min_edge
        else:
            zone = "late"
            zone_min_conf = config.late_min_confidence
            zone_min_z = config.late_min_z_score
            zone_min_edge = config.late_min_edge

        if signal.confidence < zone_min_conf:
            continue
        if signal.z_score < zone_min_z:
            continue
        # Skip 80-90% confidence dead zone
        if 0.80 <= signal.confidence < 0.90:
            continue

        # Simulate market price using BS with market maker lag
        if config.use_bs_market_price:
            mm_up_price = _simulate_market_price_bs(
                ticks_so_far, open_price, ts, minutes_remaining,
                config.realized_vol, mm_lag_seconds=config.mm_lag_seconds)
            if signal.direction == "up":
                market_price = mm_up_price
            else:
                # "down" token price = 1 - P(up) at lagged price, + spread
                market_price = 1.0 - (mm_up_price - 0.04) + 0.04  # undo spread, flip, re-add spread
                market_price = max(0.05, min(0.95, market_price))
        else:
            market_price = _simulate_market_price_legacy(signal.confidence)

        # Apply market impact
        market_price += config.market_impact_bps / 10000.0
        market_price = min(market_price, 0.95)

        # Fair value
        fair_value = 0.5 + (signal.confidence - 0.5) * config.confidence_dampen
        edge = fair_value - market_price

        if edge < zone_min_edge:
            continue

        # Execute trade
        shares = config.position_size / market_price
        fee = config.position_size * config.fee_rate

        won = signal.direction == window.outcome

        if won:
            pnl = (1.0 - market_price) * shares - fee
        else:
            pnl = -market_price * shares - fee

        return BacktestTrade(
            window_start_ms=window.start_ms,
            window_end_ms=window.end_ms,
            window_minutes=window.window_minutes,
            entry_tick_ms=ts,
            direction=signal.direction,
            actual_outcome=window.outcome,
            won=won,
            confidence=signal.confidence,
            entry_price=market_price,
            fair_value=fair_value,
            edge=edge,
            pnl=pnl,
            btc_open=window.open_price,
            btc_at_entry=price,
            btc_close=window.close_price,
            btc_change=window.price_change,
            minutes_remaining=minutes_remaining,
            consistency=signal.consistency,
            z_score=signal.z_score,
            zone=zone,
        )

    return None


def run_backtest(
    windows: list[CandleWindow],
    config: BacktestConfig | None = None,
) -> BacktestResult:
    """Run full backtest across all candle windows."""
    if config is None:
        config = BacktestConfig()

    result = BacktestResult(config=config)

    for i, window in enumerate(windows):
        trade = replay_window(window, config)
        if trade:
            result.trades.append(trade)

        if (i + 1) % 500 == 0:
            log.info("Replayed %d/%d windows, %d trades so far",
                     i + 1, len(windows), len(result.trades))

    log.info("Backtest complete: %d windows → %d trades, %.0f%% win rate, $%.2f P&L",
             len(windows), result.n_trades, result.win_rate * 100, result.total_pnl)

    return result


def grid_search(
    windows: list[CandleWindow],
    confidence_range: list[float] | None = None,
    edge_range: list[float] | None = None,
    z_score_range: list[float] | None = None,
) -> list[tuple[BacktestConfig, BacktestResult]]:
    """Grid search over parameters to find optimal configuration.

    Returns list of (config, result) sorted by Sharpe-like metric.
    """
    if confidence_range is None:
        confidence_range = [0.50, 0.55, 0.60, 0.65, 0.70]
    if edge_range is None:
        edge_range = [0.03, 0.05, 0.07, 0.10]
    if z_score_range is None:
        z_score_range = [0.5, 1.0, 1.5, 2.0]

    results: list[tuple[BacktestConfig, BacktestResult]] = []
    total = len(confidence_range) * len(edge_range) * len(z_score_range)
    count = 0

    for conf in confidence_range:
        for edge in edge_range:
            for z_min in z_score_range:
                config = BacktestConfig(
                    min_confidence=conf,
                    min_edge=edge,
                    primary_min_z_score=z_min,
                    early_min_z_score=max(z_min, 2.0),
                )
                result = run_backtest(windows, config)
                results.append((config, result))

                count += 1
                if count % 10 == 0:
                    log.info("Grid search: %d/%d combos", count, total)

    # Sort by risk-adjusted return (Sharpe-like)
    def score(pair):
        _, r = pair
        if r.n_trades < 5:
            return -999
        pnls = [t.pnl for t in r.trades]
        mean = sum(pnls) / len(pnls)
        var = sum((p - mean) ** 2 for p in pnls) / len(pnls)
        std = math.sqrt(var) if var > 0 else 0.001
        return mean / std

    results.sort(key=score, reverse=True)

    if results:
        best_cfg, best_res = results[0]
        log.info(
            "Best config: conf=%.2f edge=%.2f z_min=%.1f → "
            "%d trades, %.0f%% WR, $%.2f P&L",
            best_cfg.min_confidence, best_cfg.min_edge,
            best_cfg.primary_min_z_score,
            best_res.n_trades, best_res.win_rate * 100, best_res.total_pnl,
        )

    return results
