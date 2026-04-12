"""Scanner for BTC Up/Down candle markets on Polymarket.

Finds markets like:
  "Bitcoin Up or Down - April 4, 3:45AM-4:00AM ET"
  "Bitcoin Up or Down - April 4, 3AM ET"

Parses the time window and groups by resolution time.
"""
from __future__ import annotations

import re
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from polycrossarb.data.models import Market

log = logging.getLogger(__name__)

# Patterns for candle markets
_CANDLE_RE = re.compile(
    r"(?:Bitcoin|BTC|Ethereum|ETH|Solana|SOL)\s+Up or Down\s*[-–—]\s*(.+?)(?:\?|$)",
    re.IGNORECASE,
)


@dataclass
class CandleContract:
    """A crypto Up/Down candle contract (BTC, ETH, or SOL)."""
    market: Market
    up_token_id: str
    down_token_id: str
    up_price: float
    down_price: float
    end_date: str
    hours_left: float
    volume: float
    liquidity: float
    window_description: str  # e.g. "3:45AM-4:00AM ET"
    asset: str = "BTC"      # "BTC", "ETH", or "SOL"

    @property
    def spread(self) -> float:
        """Price spread: up + down should = 1.0, difference is the vig."""
        return abs(self.up_price + self.down_price - 1.0)

    @property
    def minutes_left(self) -> float:
        return self.hours_left * 60


def scan_candle_markets(
    markets: list[Market],
    max_hours: float = 2.0,
    min_liquidity: float = 100.0,
) -> list[CandleContract]:
    """Find all BTC Up/Down candle markets resolving within max_hours."""
    now = datetime.now(timezone.utc)
    contracts: list[CandleContract] = []

    for m in markets:
        if not m.active or m.closed:
            continue

        match = _CANDLE_RE.match(m.question)
        if not match:
            continue

        window_desc = match.group(1).strip()

        # Identify the underlying asset
        q_lower = m.question.lower()
        if q_lower.startswith(("ethereum", "eth")):
            asset = "ETH"
        elif q_lower.startswith(("solana", "sol")):
            asset = "SOL"
        else:
            asset = "BTC"

        # Must have exactly 2 outcomes (Up/Down)
        if len(m.outcomes) != 2:
            continue

        # Identify Up and Down tokens
        up_idx = -1
        down_idx = -1
        for i, o in enumerate(m.outcomes):
            name = o.name.lower()
            if "up" in name:
                up_idx = i
            elif "down" in name:
                down_idx = i

        if up_idx == -1 or down_idx == -1:
            continue

        # Check resolution time
        if not m.end_date:
            continue
        try:
            end = datetime.fromisoformat(m.end_date.replace("Z", "+00:00"))
            hours_left = (end - now).total_seconds() / 3600
        except (ValueError, TypeError):
            continue

        if hours_left <= 0 or hours_left > max_hours:
            continue

        if m.liquidity < min_liquidity:
            continue

        contracts.append(CandleContract(
            market=m,
            up_token_id=m.outcomes[up_idx].token_id,
            down_token_id=m.outcomes[down_idx].token_id,
            up_price=m.outcomes[up_idx].price,
            down_price=m.outcomes[down_idx].price,
            end_date=m.end_date,
            hours_left=hours_left,
            volume=m.volume,
            liquidity=m.liquidity,
            window_description=window_desc,
            asset=asset,
        ))

    contracts.sort(key=lambda c: c.hours_left)

    log.info("Candle scanner: %d contracts found (resolving < %.0fh)",
             len(contracts), max_hours)
    return contracts
