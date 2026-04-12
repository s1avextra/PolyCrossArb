"""Webhook alerter for critical production events.

Sends alerts to Slack or Telegram when the bot needs human attention.
Configure via environment variables:
  ALERT_WEBHOOK_URL  — Slack incoming webhook or Telegram bot URL
  ALERT_CHANNEL      — "slack" or "telegram" (default: slack)
  ALERT_REQUIRED     — "1" to make a missing webhook a hard error (live mode)
  ALERT_DRY_RUN      — "1" to capture alerts in memory instead of POSTing (tests)

Alerts fire on:
  - Circuit breaker tripped
  - Balance below threshold
  - API error rate spike
  - Liveness failure (no trades in N hours)
  - Trade execution (optional, for live mode)
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import httpx

log = logging.getLogger(__name__)

# Rate limit: max 1 alert per category per 5 minutes
_COOLDOWN_SECONDS = 300
_last_alert: dict[str, float] = {}

# Dry-run capture for tests: list of (category, message) tuples
_dry_run_buffer: list[tuple[str, str]] = []


class AlerterNotConfigured(RuntimeError):
    """Raised when ALERT_REQUIRED=1 but ALERT_WEBHOOK_URL is unset."""


def require_alerter_configured() -> None:
    """Verify alerter is wired. Call once at startup in live mode.

    Raises AlerterNotConfigured if ALERT_WEBHOOK_URL is unset and
    ALERT_DRY_RUN is not enabled. Independent of ALERT_REQUIRED so
    callers can fail-fast regardless of env var.
    """
    if os.environ.get("ALERT_DRY_RUN") == "1":
        return
    if not os.environ.get("ALERT_WEBHOOK_URL", "").strip():
        raise AlerterNotConfigured(
            "ALERT_WEBHOOK_URL must be set in live mode. "
            "Set ALERT_DRY_RUN=1 only in tests."
        )


def reset_dry_run_buffer() -> None:
    """Clear the in-memory alert buffer (test helper)."""
    _dry_run_buffer.clear()


def get_dry_run_alerts() -> list[tuple[str, str]]:
    """Return captured alerts from dry-run mode (test helper)."""
    return list(_dry_run_buffer)


def _should_alert(category: str) -> bool:
    now = time.time()
    last = _last_alert.get(category, 0)
    if now - last < _COOLDOWN_SECONDS:
        return False
    _last_alert[category] = now
    return True


def _get_config() -> tuple[str, str]:
    url = os.environ.get("ALERT_WEBHOOK_URL", "")
    channel = os.environ.get("ALERT_CHANNEL", "slack")
    return url, channel


def _send(message: str, category: str) -> None:
    """Send an alert via webhook. Non-blocking, fire-and-forget.

    In dry-run mode (ALERT_DRY_RUN=1), captures alerts in memory.
    If ALERT_REQUIRED=1 and no webhook is configured, raises.
    """
    if os.environ.get("ALERT_DRY_RUN") == "1":
        if _should_alert(category):
            _dry_run_buffer.append((category, message))
        return

    url, channel = _get_config()
    if not url:
        if os.environ.get("ALERT_REQUIRED") == "1":
            raise AlerterNotConfigured(
                f"Alert [{category}] requested but ALERT_WEBHOOK_URL is unset"
            )
        log.warning("No ALERT_WEBHOOK_URL configured — alert dropped: [%s] %s",
                    category, message[:80])
        return

    if not _should_alert(category):
        return

    try:
        # Both Slack and Telegram bot URLs accept POST {"text": ...}
        httpx.post(url, json={"text": message}, timeout=5)
        log.info("Alert sent [%s]: %s", category, message[:60])
    except Exception:
        log.warning("Alert delivery failed for [%s]", category)


# ── Public API ─────────────────────────────────────────────


def alert_circuit_breaker(strategy: str, reason: str, stats: dict[str, Any]) -> None:
    """Circuit breaker tripped — stop trading."""
    msg = (
        f":rotating_light: *CIRCUIT BREAKER* — {strategy}\n"
        f"Reason: {reason}\n"
        f"Win rate: {stats.get('win_rate', 'N/A')}, "
        f"Drawdown: {stats.get('drawdown', 'N/A')}, "
        f"Trades: {stats.get('trades', 'N/A')}"
    )
    _send(msg, "circuit_breaker")


def alert_low_balance(balance: float, threshold: float) -> None:
    """Wallet balance below operating threshold."""
    msg = (
        f":warning: *LOW BALANCE* — ${balance:.2f} USDC.e\n"
        f"Threshold: ${threshold:.2f}\n"
        f"Bot may stop placing trades."
    )
    _send(msg, "low_balance")


def alert_error_spike(error_count: int, window_minutes: int, recent_errors: list[str]) -> None:
    """API error rate exceeds threshold."""
    errors_str = "\n".join(f"  - {e[:80]}" for e in recent_errors[:3])
    msg = (
        f":x: *ERROR SPIKE* — {error_count} errors in {window_minutes}min\n"
        f"Recent:\n{errors_str}"
    )
    _send(msg, "error_spike")


def alert_liveness(last_trade_age_hours: float, last_check_age_hours: float) -> None:
    """No activity for an extended period."""
    msg = (
        f":zzz: *LIVENESS CHECK FAILED*\n"
        f"Last trade: {last_trade_age_hours:.1f}h ago\n"
        f"Last arb check: {last_check_age_hours:.1f}h ago\n"
        f"Bot may be stuck or disconnected."
    )
    _send(msg, "liveness")


def alert_trade(strategy: str, profit: float, bankroll: float, details: str = "") -> None:
    """Trade executed (optional — enable for live mode visibility)."""
    emoji = ":white_check_mark:" if profit > 0 else ":small_red_triangle_down:"
    msg = (
        f"{emoji} *TRADE* — {strategy}\n"
        f"P&L: ${profit:+.4f} | Bankroll: ${bankroll:.2f}\n"
        f"{details}"
    )
    _send(msg, f"trade_{strategy}")


def alert_shutdown(reason: str, pnl: float, trades: int) -> None:
    """Bot shutting down."""
    msg = (
        f":octagonal_sign: *SHUTDOWN* — {reason}\n"
        f"Session P&L: ${pnl:+.4f} | Trades: {trades}"
    )
    _send(msg, "shutdown")


def alert_latency_degradation(
    p99_us: int,
    threshold_ms: float,
    window_count: int,
    strategy: str = "candle",
) -> None:
    """Latency degradation detected — p99 exceeds threshold for consecutive windows."""
    p99_ms = p99_us / 1000
    msg = (
        f":warning: *LATENCY DEGRADATION* — {strategy}\n"
        f"p99: {p99_ms:.1f}ms (threshold: {threshold_ms:.0f}ms)\n"
        f"Exceeded for {window_count} consecutive 30s windows"
    )
    _send(msg, "latency")
