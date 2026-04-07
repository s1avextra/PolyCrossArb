"""Evaluate backtest results: Sharpe, drawdown, win rate by segment."""
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from polycrossarb.backtest.replay_engine import BacktestResult, BacktestTrade

log = logging.getLogger(__name__)


@dataclass
class EvalMetrics:
    """Summary statistics for a backtest."""
    n_trades: int
    n_wins: int
    win_rate: float
    total_pnl: float
    avg_pnl: float
    max_win: float
    max_loss: float
    sharpe_ratio: float
    max_drawdown: float
    max_drawdown_pct: float
    profit_factor: float
    avg_edge: float
    avg_confidence: float
    avg_minutes_remaining: float
    pnl_per_hour: float
    total_hours: float

    # By confidence bucket
    by_confidence: dict[str, dict]
    # By time-of-day (UTC hour)
    by_hour: dict[int, dict]
    # By window size
    by_window: dict[int, dict]


def evaluate(result: BacktestResult) -> EvalMetrics:
    """Compute comprehensive evaluation metrics."""
    trades = result.trades
    if not trades:
        return _empty_metrics()

    pnls = [t.pnl for t in trades]
    cumulative = _cumulative_pnl(pnls)

    # Time span
    first_ts = min(t.window_start_ms for t in trades)
    last_ts = max(t.window_end_ms for t in trades)
    total_hours = (last_ts - first_ts) / 3_600_000

    # Sharpe
    mean_pnl = sum(pnls) / len(pnls)
    var_pnl = sum((p - mean_pnl) ** 2 for p in pnls) / len(pnls)
    std_pnl = math.sqrt(var_pnl) if var_pnl > 0 else 0.001
    sharpe = mean_pnl / std_pnl

    # Max drawdown
    peak = 0.0
    max_dd = 0.0
    max_dd_pct = 0.0
    for cum in cumulative:
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)
        if peak > 0:
            max_dd_pct = max(max_dd_pct, dd / peak)

    # Profit factor
    gross_profit = sum(p for p in pnls if p > 0)
    gross_loss = abs(sum(p for p in pnls if p < 0))
    profit_factor = gross_profit / max(gross_loss, 0.01)

    # Wins/losses
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    return EvalMetrics(
        n_trades=len(trades),
        n_wins=sum(1 for t in trades if t.won),
        win_rate=result.win_rate,
        total_pnl=sum(pnls),
        avg_pnl=mean_pnl,
        max_win=max(pnls) if pnls else 0,
        max_loss=min(pnls) if pnls else 0,
        sharpe_ratio=sharpe,
        max_drawdown=max_dd,
        max_drawdown_pct=max_dd_pct,
        profit_factor=profit_factor,
        avg_edge=result.avg_edge,
        avg_confidence=result.avg_confidence,
        avg_minutes_remaining=sum(t.minutes_remaining for t in trades) / len(trades),
        pnl_per_hour=sum(pnls) / max(total_hours, 0.01),
        total_hours=total_hours,
        by_confidence=_by_confidence(trades),
        by_hour=_by_hour(trades),
        by_window=_by_window(trades),
    )


def _cumulative_pnl(pnls: list[float]) -> list[float]:
    cum = []
    total = 0.0
    for p in pnls:
        total += p
        cum.append(total)
    return cum


def _bucket_stats(trades: list[BacktestTrade]) -> dict:
    if not trades:
        return {"count": 0, "win_rate": 0, "pnl": 0, "avg_edge": 0}
    wins = sum(1 for t in trades if t.won)
    return {
        "count": len(trades),
        "win_rate": round(wins / len(trades), 3),
        "pnl": round(sum(t.pnl for t in trades), 2),
        "avg_edge": round(sum(t.edge for t in trades) / len(trades), 4),
        "avg_confidence": round(sum(t.confidence for t in trades) / len(trades), 3),
    }


def _by_confidence(trades: list[BacktestTrade]) -> dict[str, dict]:
    buckets = {
        "50-60%": [t for t in trades if 0.50 <= t.confidence < 0.60],
        "60-70%": [t for t in trades if 0.60 <= t.confidence < 0.70],
        "70-80%": [t for t in trades if 0.70 <= t.confidence < 0.80],
        "80-90%": [t for t in trades if 0.80 <= t.confidence < 0.90],
        "90%+": [t for t in trades if t.confidence >= 0.90],
    }
    return {k: _bucket_stats(v) for k, v in buckets.items()}


def _by_hour(trades: list[BacktestTrade]) -> dict[int, dict]:
    by_h: dict[int, list[BacktestTrade]] = {}
    for t in trades:
        dt = datetime.fromtimestamp(t.window_start_ms / 1000, tz=timezone.utc)
        h = dt.hour
        by_h.setdefault(h, []).append(t)
    return {h: _bucket_stats(ts) for h, ts in sorted(by_h.items())}


def _by_window(trades: list[BacktestTrade]) -> dict[int, dict]:
    by_w: dict[int, list[BacktestTrade]] = {}
    for t in trades:
        by_w.setdefault(t.window_minutes, []).append(t)
    return {w: _bucket_stats(ts) for w, ts in sorted(by_w.items())}


def _empty_metrics() -> EvalMetrics:
    return EvalMetrics(
        n_trades=0, n_wins=0, win_rate=0, total_pnl=0, avg_pnl=0,
        max_win=0, max_loss=0, sharpe_ratio=0, max_drawdown=0,
        max_drawdown_pct=0, profit_factor=0, avg_edge=0, avg_confidence=0,
        avg_minutes_remaining=0, pnl_per_hour=0, total_hours=0,
        by_confidence={}, by_hour={}, by_window={},
    )


def print_report(metrics: EvalMetrics) -> str:
    """Format a human-readable report."""
    lines = [
        "=" * 60,
        "  BACKTEST RESULTS",
        "=" * 60,
        f"  Trades:         {metrics.n_trades}",
        f"  Wins:           {metrics.n_wins} ({metrics.win_rate:.1%})",
        f"  Total P&L:      ${metrics.total_pnl:+.2f}",
        f"  Avg P&L/trade:  ${metrics.avg_pnl:+.2f}",
        f"  P&L/hour:       ${metrics.pnl_per_hour:+.2f}",
        f"  Max win:        ${metrics.max_win:+.2f}",
        f"  Max loss:       ${metrics.max_loss:+.2f}",
        f"  Sharpe ratio:   {metrics.sharpe_ratio:.2f}",
        f"  Max drawdown:   ${metrics.max_drawdown:.2f} ({metrics.max_drawdown_pct:.1%})",
        f"  Profit factor:  {metrics.profit_factor:.2f}",
        f"  Avg edge:       {metrics.avg_edge:.2%}",
        f"  Avg confidence: {metrics.avg_confidence:.1%}",
        f"  Avg mins left:  {metrics.avg_minutes_remaining:.1f}",
        f"  Total hours:    {metrics.total_hours:.1f}",
        "",
        "  BY CONFIDENCE LEVEL",
        "  " + "-" * 56,
    ]

    for bucket, stats in metrics.by_confidence.items():
        if stats["count"] > 0:
            lines.append(
                f"  {bucket:>8s}: {stats['count']:>4d} trades, "
                f"{stats['win_rate']:.0%} WR, ${stats['pnl']:+.2f}"
            )

    lines.extend(["", "  BY WINDOW SIZE", "  " + "-" * 56])
    for window, stats in metrics.by_window.items():
        if stats["count"] > 0:
            lines.append(
                f"  {window:>3d}min: {stats['count']:>4d} trades, "
                f"{stats['win_rate']:.0%} WR, ${stats['pnl']:+.2f}"
            )

    lines.extend(["", "  BY HOUR (UTC)", "  " + "-" * 56])
    for hour, stats in metrics.by_hour.items():
        if stats["count"] > 0:
            lines.append(
                f"  {hour:02d}:00: {stats['count']:>4d} trades, "
                f"{stats['win_rate']:.0%} WR, ${stats['pnl']:+.2f}"
            )

    lines.append("=" * 60)
    report = "\n".join(lines)
    return report


def save_results(
    metrics: EvalMetrics,
    trades: list[BacktestTrade],
    output_dir: str = "logs",
):
    """Save backtest results to JSON and CSV."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Summary JSON
    summary = {
        "n_trades": metrics.n_trades,
        "win_rate": round(metrics.win_rate, 4),
        "total_pnl": round(metrics.total_pnl, 2),
        "sharpe_ratio": round(metrics.sharpe_ratio, 4),
        "max_drawdown": round(metrics.max_drawdown, 2),
        "profit_factor": round(metrics.profit_factor, 2),
        "pnl_per_hour": round(metrics.pnl_per_hour, 2),
        "avg_edge": round(metrics.avg_edge, 4),
        "avg_confidence": round(metrics.avg_confidence, 4),
        "by_confidence": metrics.by_confidence,
        "by_window": metrics.by_window,
    }
    with open(out / "backtest_results.json", "w") as f:
        json.dump(summary, f, indent=2)

    # Trades CSV
    import csv
    with open(out / "backtest_trades.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "window_start", "window_end", "window_min", "direction",
            "actual", "won", "confidence", "entry_price", "edge",
            "pnl", "btc_open", "btc_entry", "btc_close", "btc_change",
            "minutes_remaining", "consistency",
        ])
        for t in trades:
            writer.writerow([
                t.window_start_ms, t.window_end_ms, t.window_minutes,
                t.direction, t.actual_outcome, t.won,
                round(t.confidence, 4), round(t.entry_price, 4),
                round(t.edge, 4), round(t.pnl, 4),
                round(t.btc_open, 2), round(t.btc_at_entry, 2),
                round(t.btc_close, 2), round(t.btc_change, 2),
                round(t.minutes_remaining, 2), round(t.consistency, 4),
            ])

    log.info("Saved results to %s", out)
