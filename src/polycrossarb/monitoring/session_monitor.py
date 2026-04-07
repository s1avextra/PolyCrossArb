"""Live session monitoring system.

Captures every interaction with Polymarket and exchange feeds
to identify unforeseen obstacles in production.

Collects:
  1. Order lifecycle: placement, fill polling, partial/full fill, cancel, rejection
  2. Price feed health: staleness, source dropouts, cross-exchange divergence
  3. Market data: contract availability, liquidity, spread changes
  4. Execution quality: slippage, fill rate, fill time, fee vs expected
  5. Strategy signals: momentum detections, edge calculations, skipped trades
  6. Risk state: bankroll, exposure, position count, drawdown
  7. System health: latency, errors, reconnections

All data is written to JSONL files for post-session analysis.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class MonitorEvent:
    """A single monitoring event."""
    timestamp: float
    category: str      # order, price, market, execution, signal, risk, system
    event_type: str    # specific event within category
    data: dict[str, Any]

    def to_dict(self) -> dict:
        return {
            "ts": self.timestamp,
            "ts_iso": datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat(),
            "cat": self.category,
            "type": self.event_type,
            **self.data,
        }


class SessionMonitor:
    """Comprehensive live session monitor.

    Usage:
        monitor = SessionMonitor()
        monitor.record_order_placed(...)
        monitor.record_fill(...)
        # At end:
        monitor.print_summary()
        monitor.close()
    """

    def __init__(self, log_dir: str = "logs/sessions"):
        self._dir = Path(log_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

        session_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self._session_id = session_id
        self._events_path = self._dir / f"session_{session_id}.jsonl"
        self._summary_path = self._dir / f"summary_{session_id}.json"
        self._file = open(self._events_path, "a")

        # Counters for summary
        self._start_time = time.time()
        self._order_count = 0
        self._fill_count = 0
        self._reject_count = 0
        self._partial_fill_count = 0
        self._cancel_count = 0
        self._total_slippage = 0.0
        self._total_fees = 0.0
        self._total_cost = 0.0
        self._signal_count = 0
        self._signal_skip_count = 0
        self._skip_reasons: dict[str, int] = {}
        self._price_gaps: list[float] = []
        self._fill_times: list[float] = []
        self._errors: list[dict] = []
        self._source_dropouts: dict[str, int] = {}
        self._api_latencies: list[float] = []

        log.info("Session monitor started: %s", self._events_path)

    def _write(self, category: str, event_type: str, data: dict):
        event = MonitorEvent(
            timestamp=time.time(),
            category=category,
            event_type=event_type,
            data=data,
        )
        self._file.write(json.dumps(event.to_dict()) + "\n")
        self._file.flush()

    # ── Order Lifecycle ─────────────────────────────────────────

    def record_order_placed(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        order_id: str,
        live_price: float,
        book_best_ask: float,
        book_ask_depth: float,
        book_bid_depth: float,
        balance_usd: float,
    ):
        self._order_count += 1
        self._write("order", "placed", {
            "token_id": token_id[:20],
            "side": side,
            "price": round(price, 4),
            "live_price": round(live_price, 4),
            "size": round(size, 2),
            "order_value": round(price * size, 2),
            "order_id": order_id[:16],
            "book_best_ask": round(book_best_ask, 4),
            "book_ask_depth": round(book_ask_depth, 2),
            "book_bid_depth": round(book_bid_depth, 2),
            "balance_usd": round(balance_usd, 2),
        })

    def record_fill(
        self,
        order_id: str,
        filled_size: float,
        requested_size: float,
        fill_price: float,
        limit_price: float,
        fill_time_s: float,
        fee: float,
        n_trades: int,
    ):
        self._fill_count += 1
        slippage = fill_price - limit_price
        self._total_slippage += abs(slippage)
        self._total_fees += fee
        self._total_cost += fill_price * filled_size
        self._fill_times.append(fill_time_s)

        fill_pct = filled_size / requested_size if requested_size > 0 else 0
        if fill_pct < 0.95:
            self._partial_fill_count += 1

        self._write("order", "filled", {
            "order_id": order_id[:16],
            "filled": round(filled_size, 2),
            "requested": round(requested_size, 2),
            "fill_pct": round(fill_pct, 3),
            "fill_price": round(fill_price, 4),
            "limit_price": round(limit_price, 4),
            "slippage": round(slippage, 4),
            "slippage_bps": round(slippage / limit_price * 10000, 1) if limit_price > 0 else 0,
            "fill_time_s": round(fill_time_s, 2),
            "fee": round(fee, 4),
            "n_trades": n_trades,
        })

    def record_order_rejected(self, token_id: str, reason: str, price: float, size: float):
        self._reject_count += 1
        self._write("order", "rejected", {
            "token_id": token_id[:20],
            "reason": reason[:100],
            "price": round(price, 4),
            "size": round(size, 2),
        })

    def record_order_cancelled(self, order_id: str, filled_size: float, reason: str):
        self._cancel_count += 1
        self._write("order", "cancelled", {
            "order_id": order_id[:16],
            "filled_before_cancel": round(filled_size, 2),
            "reason": reason[:100],
        })

    def record_order_timeout(self, order_id: str, filled: float, requested: float):
        self._write("order", "timeout", {
            "order_id": order_id[:16],
            "filled": round(filled, 2),
            "requested": round(requested, 2),
            "fill_pct": round(filled / requested, 3) if requested > 0 else 0,
        })

    # ── Price Feed Health ───────────────────────────────────────

    def record_price_snapshot(
        self,
        btc_price: float,
        n_sources: int,
        spread: float,
        staleness_ms: float,
        sources: dict[str, float],
    ):
        self._write("price", "snapshot", {
            "btc": round(btc_price, 2),
            "sources": n_sources,
            "spread": round(spread, 2),
            "staleness_ms": round(staleness_ms, 1),
            "source_prices": {k: round(v, 2) for k, v in sources.items()},
        })

        # Track cross-exchange divergence
        if len(sources) >= 2:
            prices = list(sources.values())
            gap = max(prices) - min(prices)
            self._price_gaps.append(gap)

    def record_source_dropout(self, source: str, last_price: float, stale_seconds: float):
        self._source_dropouts[source] = self._source_dropouts.get(source, 0) + 1
        self._write("price", "source_dropout", {
            "source": source,
            "last_price": round(last_price, 2),
            "stale_s": round(stale_seconds, 1),
            "total_dropouts": self._source_dropouts[source],
        })

    # ── Market Data ─────────────────────────────────────────────

    def record_contract_scan(
        self,
        total_markets: int,
        candle_contracts: int,
        tradeable_contracts: int,
    ):
        self._write("market", "scan", {
            "total_markets": total_markets,
            "candle_contracts": candle_contracts,
            "tradeable": tradeable_contracts,
        })

    def record_contract_detail(
        self,
        condition_id: str,
        question: str,
        up_price: float,
        down_price: float,
        spread: float,
        volume: float,
        liquidity: float,
        hours_left: float,
    ):
        self._write("market", "contract", {
            "cid": condition_id[:16],
            "q": question[:60],
            "up": round(up_price, 4),
            "down": round(down_price, 4),
            "spread": round(spread, 4),
            "vol": round(volume, 2),
            "liq": round(liquidity, 2),
            "hours_left": round(hours_left, 3),
        })

    # ── Strategy Signals ────────────────────────────────────────

    def record_signal(
        self,
        contract_id: str,
        direction: str,
        confidence: float,
        edge: float,
        market_price: float,
        fair_value: float,
        btc_price: float,
        btc_change: float,
        minutes_remaining: float,
        traded: bool,
    ):
        self._signal_count += 1
        self._write("signal", "momentum", {
            "cid": contract_id[:16],
            "dir": direction,
            "conf": round(confidence, 3),
            "edge": round(edge, 4),
            "mkt_price": round(market_price, 4),
            "fair": round(fair_value, 4),
            "btc": round(btc_price, 2),
            "btc_chg": round(btc_change, 2),
            "mins_left": round(minutes_remaining, 2),
            "traded": traded,
        })

    def record_signal_skip(self, contract_id: str, reason: str):
        self._signal_skip_count += 1
        self._skip_reasons[reason] = self._skip_reasons.get(reason, 0) + 1
        self._write("signal", "skip", {
            "cid": contract_id[:16],
            "reason": reason,
        })

    # ── Resolution & P&L ────────────────────────────────────────

    def record_resolution(
        self,
        contract_id: str,
        predicted: str,
        actual: str,
        won: bool,
        pnl: float,
        entry_price: float,
        open_btc: float,
        close_btc: float,
    ):
        self._write("resolution", "resolved", {
            "cid": contract_id[:16],
            "predicted": predicted,
            "actual": actual,
            "won": won,
            "pnl": round(pnl, 4),
            "entry_price": round(entry_price, 4),
            "open_btc": round(open_btc, 2),
            "close_btc": round(close_btc, 2),
            "btc_move": round(close_btc - open_btc, 2),
        })

    # ── Risk State ──────────────────────────────────────────────

    def record_risk_state(
        self,
        bankroll: float,
        exposure: float,
        available: float,
        positions: int,
        realized_pnl: float,
        wins: int,
        losses: int,
    ):
        self._write("risk", "state", {
            "bankroll": round(bankroll, 2),
            "exposure": round(exposure, 2),
            "available": round(available, 2),
            "positions": positions,
            "realized_pnl": round(realized_pnl, 2),
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / max(wins + losses, 1), 3),
        })

    # ── System Health ───────────────────────────────────────────

    def record_api_call(self, endpoint: str, latency_ms: float, status: int, error: str = ""):
        self._api_latencies.append(latency_ms)
        if error:
            self._errors.append({"endpoint": endpoint, "error": error[:100], "ts": time.time()})
        self._write("system", "api_call", {
            "endpoint": endpoint[:40],
            "latency_ms": round(latency_ms, 1),
            "status": status,
            "error": error[:100] if error else "",
        })

    def record_error(self, component: str, error: str, recoverable: bool = True):
        self._errors.append({"component": component, "error": error[:200], "ts": time.time()})
        self._write("system", "error", {
            "component": component,
            "error": error[:200],
            "recoverable": recoverable,
        })

    def record_reconnect(self, source: str, attempt: int):
        self._write("system", "reconnect", {
            "source": source,
            "attempt": attempt,
        })

    # ── Summary ─────────────────────────────────────────────────

    def get_summary(self) -> dict:
        elapsed = time.time() - self._start_time
        avg_fill_time = sum(self._fill_times) / max(len(self._fill_times), 1)
        avg_slippage = self._total_slippage / max(self._fill_count, 1)
        avg_latency = sum(self._api_latencies) / max(len(self._api_latencies), 1)

        avg_gap = sum(self._price_gaps) / max(len(self._price_gaps), 1) if self._price_gaps else 0
        max_gap = max(self._price_gaps) if self._price_gaps else 0

        return {
            "session_id": self._session_id,
            "duration_minutes": round(elapsed / 60, 1),
            "orders": {
                "placed": self._order_count,
                "filled": self._fill_count,
                "rejected": self._reject_count,
                "partial_fills": self._partial_fill_count,
                "cancelled": self._cancel_count,
                "fill_rate": round(self._fill_count / max(self._order_count, 1), 3),
                "avg_fill_time_s": round(avg_fill_time, 2),
                "avg_slippage": round(avg_slippage, 4),
                "total_fees": round(self._total_fees, 4),
                "total_cost": round(self._total_cost, 2),
            },
            "signals": {
                "total": self._signal_count,
                "skipped": self._signal_skip_count,
                "skip_reasons": dict(sorted(self._skip_reasons.items(), key=lambda x: -x[1])),
            },
            "price_feed": {
                "avg_cross_exchange_gap": round(avg_gap, 2),
                "max_cross_exchange_gap": round(max_gap, 2),
                "source_dropouts": self._source_dropouts,
            },
            "system": {
                "avg_api_latency_ms": round(avg_latency, 1),
                "total_errors": len(self._errors),
                "recent_errors": self._errors[-5:],
            },
        }

    def print_summary(self) -> str:
        s = self.get_summary()
        lines = [
            "=" * 60,
            "  SESSION MONITORING REPORT",
            f"  Session: {s['session_id']}",
            f"  Duration: {s['duration_minutes']} minutes",
            "=" * 60,
            "",
            "  ORDERS",
            f"    Placed:       {s['orders']['placed']}",
            f"    Filled:       {s['orders']['filled']} ({s['orders']['fill_rate']:.0%})",
            f"    Rejected:     {s['orders']['rejected']}",
            f"    Partial:      {s['orders']['partial_fills']}",
            f"    Cancelled:    {s['orders']['cancelled']}",
            f"    Avg fill:     {s['orders']['avg_fill_time_s']:.1f}s",
            f"    Avg slippage: ${s['orders']['avg_slippage']:.4f}",
            f"    Total fees:   ${s['orders']['total_fees']:.4f}",
            f"    Total cost:   ${s['orders']['total_cost']:.2f}",
            "",
            "  SIGNALS",
            f"    Detected:     {s['signals']['total']}",
            f"    Skipped:      {s['signals']['skipped']}",
        ]
        for reason, count in list(s['signals']['skip_reasons'].items())[:5]:
            lines.append(f"      {reason}: {count}")

        lines.extend([
            "",
            "  PRICE FEED",
            f"    Avg gap:      ${s['price_feed']['avg_cross_exchange_gap']:.2f}",
            f"    Max gap:      ${s['price_feed']['max_cross_exchange_gap']:.2f}",
        ])
        for src, cnt in s['price_feed']['source_dropouts'].items():
            lines.append(f"    {src} dropouts: {cnt}")

        lines.extend([
            "",
            "  SYSTEM",
            f"    Avg API lat:  {s['system']['avg_api_latency_ms']:.0f}ms",
            f"    Errors:       {s['system']['total_errors']}",
        ])
        for err in s['system']['recent_errors'][-3:]:
            lines.append(f"      {err['component']}: {err['error'][:60]}")

        lines.append("=" * 60)
        report = "\n".join(lines)
        return report

    def save_summary(self):
        summary = self.get_summary()
        with open(self._summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        log.info("Session summary saved: %s", self._summary_path)

    def close(self):
        self.save_summary()
        self._file.close()
