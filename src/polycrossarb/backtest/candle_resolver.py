"""Resolve candle backtest fills against actual BTC outcomes.

After the L2BacktestEngine has run, each `BacktestFill` corresponds to
a market order placed during a candle window. To compute realized P&L
we need to:
  1. Look up which window the fill was in
  2. Get BTC's price at the window's open and close
  3. Determine the actual outcome (up if close >= open, else down)
  4. Compare to the predicted direction
  5. Compute payoff: $1 - fill_price (win) or -fill_price (loss)
  6. Subtract fees

This module produces a `ResolvedTrade` for each fill so you can compute
metrics like win rate, P&L, edge accuracy, slippage realization.
"""
from __future__ import annotations

import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field

from polycrossarb.backtest.btc_history import BTCHistory
from polycrossarb.backtest.candle_registry import CandleContract, CandleRegistry
from polycrossarb.backtest.candle_strategy import CandleStrategyAdapter
from polycrossarb.backtest.l2_replay import BacktestFill, L2BacktestEngine
from polycrossarb.crypto.decision import CandleDecision

log = logging.getLogger(__name__)


@dataclass
class ResolvedTrade:
    """A backtest fill that has been resolved against the actual BTC outcome."""
    contract: CandleContract
    decision: CandleDecision
    fill: BacktestFill

    open_btc: float
    close_btc: float
    actual_direction: str   # "up" or "down"
    won: bool
    pnl: float              # realized: (1 - fill_price) * size on win, -fill_price * size on loss
    pnl_after_fee: float

    @property
    def predicted_direction(self) -> str:
        return self.decision.direction

    @property
    def edge_realized(self) -> float:
        """Realized edge: (1 - fill_price) on win, -fill_price on loss, per share."""
        if self.won:
            return 1.0 - self.fill.fill_price
        return -self.fill.fill_price


@dataclass
class BacktestResults:
    """Aggregated results across all resolved trades."""
    trades: list[ResolvedTrade] = field(default_factory=list)
    unresolved_fills: list[BacktestFill] = field(default_factory=list)

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
        return sum(t.pnl_after_fee for t in self.trades)

    @property
    def total_fees(self) -> float:
        return sum(t.fill.fee for t in self.trades)

    @property
    def total_slippage(self) -> float:
        return sum(t.fill.slippage * t.fill.filled_size for t in self.trades)

    @property
    def avg_pnl(self) -> float:
        return self.total_pnl / max(self.n_trades, 1)

    @property
    def sharpe(self) -> float:
        if self.n_trades < 2:
            return 0.0
        pnls = [t.pnl_after_fee for t in self.trades]
        mean = sum(pnls) / len(pnls)
        var = sum((p - mean) ** 2 for p in pnls) / len(pnls)
        std = math.sqrt(var)
        return mean / std if std > 0 else 0.0

    @property
    def by_zone(self) -> dict[str, dict]:
        out: dict[str, list[ResolvedTrade]] = defaultdict(list)
        for t in self.trades:
            out[t.decision.zone].append(t)
        return {
            zone: _bucket_stats(ts)
            for zone, ts in out.items()
        }

    @property
    def by_confidence_bucket(self) -> dict[str, dict]:
        buckets: dict[str, list[ResolvedTrade]] = {
            "55-65%": [],
            "65-75%": [],
            "75-85%": [],
            "85-95%": [],
        }
        for t in self.trades:
            c = t.decision.confidence
            if 0.55 <= c < 0.65:
                buckets["55-65%"].append(t)
            elif 0.65 <= c < 0.75:
                buckets["65-75%"].append(t)
            elif 0.75 <= c < 0.85:
                buckets["75-85%"].append(t)
            elif 0.85 <= c < 0.95:
                buckets["85-95%"].append(t)
        return {k: _bucket_stats(v) for k, v in buckets.items() if v}

    @property
    def by_asset(self) -> dict[str, dict]:
        out: dict[str, list[ResolvedTrade]] = defaultdict(list)
        for t in self.trades:
            out[t.contract.asset].append(t)
        return {a: _bucket_stats(ts) for a, ts in out.items()}

    @property
    def by_window_minutes(self) -> dict[float, dict]:
        out: dict[float, list[ResolvedTrade]] = defaultdict(list)
        for t in self.trades:
            out[t.contract.window_minutes].append(t)
        return {w: _bucket_stats(ts) for w, ts in sorted(out.items())}


def _bucket_stats(trades: list[ResolvedTrade]) -> dict:
    if not trades:
        return {"count": 0, "win_rate": 0.0, "pnl": 0.0}
    n_wins = sum(1 for t in trades if t.won)
    pnl = sum(t.pnl_after_fee for t in trades)
    return {
        "count": len(trades),
        "win_rate": round(n_wins / len(trades), 3),
        "pnl": round(pnl, 4),
        "avg_edge": round(sum(t.decision.edge for t in trades) / len(trades), 4),
        "avg_confidence": round(sum(t.decision.confidence for t in trades) / len(trades), 3),
    }


def resolve_backtest(
    engine: L2BacktestEngine,
    adapter: CandleStrategyAdapter,
    btc_history: BTCHistory,
) -> BacktestResults:
    """Resolve all fills from a backtest run against actual BTC outcomes.

    Args:
        engine: the L2BacktestEngine that was run
        adapter: the CandleStrategyAdapter used (for window state and decisions)
        btc_history: BTC tick history for outcome resolution

    Returns:
        BacktestResults with resolved P&L and statistics
    """
    results = BacktestResults()

    # Build token_id -> contract index from registry
    registry = adapter.registry

    for fill in engine.fills:
        if not fill.success:
            continue

        contract = registry.lookup(fill.order.token_id)
        if contract is None:
            results.unresolved_fills.append(fill)
            continue

        decision = adapter.decisions_by_window.get(contract.condition_id)
        if decision is None:
            results.unresolved_fills.append(fill)
            continue

        # Get BTC at the window open and close
        open_ts_ms = int(contract.start_time_s * 1000)
        close_ts_ms = int(contract.end_time_s * 1000)
        open_btc = btc_history.price_at(open_ts_ms)
        close_btc = btc_history.price_at(close_ts_ms)

        if open_btc <= 0 or close_btc <= 0:
            results.unresolved_fills.append(fill)
            continue

        actual_direction = "up" if close_btc >= open_btc else "down"
        won = (actual_direction == decision.direction)

        # Payoff: $1 if win, $0 if loss, minus what we paid
        if won:
            pnl = (1.0 - fill.fill_price) * fill.filled_size
        else:
            pnl = -fill.fill_price * fill.filled_size

        pnl_after_fee = pnl - fill.fee

        results.trades.append(ResolvedTrade(
            contract=contract,
            decision=decision,
            fill=fill,
            open_btc=open_btc,
            close_btc=close_btc,
            actual_direction=actual_direction,
            won=won,
            pnl=pnl,
            pnl_after_fee=pnl_after_fee,
        ))

    return results


def format_report(results: BacktestResults) -> str:
    """Human-readable summary of backtest results."""
    lines = [
        "=" * 70,
        "  L2 BACKTEST RESULTS",
        "=" * 70,
        f"  Trades resolved:   {results.n_trades}",
        f"  Wins:              {results.n_wins}",
        f"  Losses:            {results.n_losses}",
        f"  Win rate:          {results.win_rate:.1%}",
        f"  Total P&L:         ${results.total_pnl:+.4f}",
        f"  Avg P&L per trade: ${results.avg_pnl:+.4f}",
        f"  Total fees:        ${results.total_fees:.4f}",
        f"  Total slippage:    ${results.total_slippage:.4f}",
        f"  Sharpe ratio:      {results.sharpe:.3f}",
        f"  Unresolved fills:  {len(results.unresolved_fills)}",
        "",
        "  BY ZONE",
        "  " + "-" * 66,
    ]
    for zone, stats in results.by_zone.items():
        lines.append(
            f"  {zone:>8s}: {stats['count']:>4d} trades, "
            f"{stats['win_rate']:.0%} WR, ${stats['pnl']:+.4f} P&L, "
            f"avg edge {stats['avg_edge']:+.3f}, conf {stats['avg_confidence']:.2f}"
        )

    lines.extend(["", "  BY CONFIDENCE", "  " + "-" * 66])
    for bucket, stats in results.by_confidence_bucket.items():
        if stats["count"] > 0:
            lines.append(
                f"  {bucket:>7s}: {stats['count']:>4d} trades, "
                f"{stats['win_rate']:.0%} WR, ${stats['pnl']:+.4f} P&L"
            )

    lines.extend(["", "  BY ASSET", "  " + "-" * 66])
    for asset, stats in results.by_asset.items():
        lines.append(
            f"  {asset:>5s}: {stats['count']:>4d} trades, "
            f"{stats['win_rate']:.0%} WR, ${stats['pnl']:+.4f} P&L"
        )

    lines.extend(["", "  BY WINDOW LENGTH", "  " + "-" * 66])
    for w, stats in results.by_window_minutes.items():
        lines.append(
            f"  {w:>3.0f} min: {stats['count']:>4d} trades, "
            f"{stats['win_rate']:.0%} WR, ${stats['pnl']:+.4f} P&L"
        )

    lines.append("=" * 70)
    return "\n".join(lines)
