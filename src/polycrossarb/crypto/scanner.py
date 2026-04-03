"""Scan Polymarket for BTC price milestone contracts.

Finds markets like:
  "Will Bitcoin exceed $155,000 by April 12?"
  "Will BTC hit $160k?"

Parses the strike price and expiry from the question text.
"""
from __future__ import annotations

import re
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from polycrossarb.data.models import Market

log = logging.getLogger(__name__)


@dataclass
class CryptoContract:
    """A parsed crypto price contract."""
    market: Market
    asset: str           # "BTC", "ETH", etc.
    strike: float        # e.g. 155000
    direction: str       # "above" or "below"
    expiry_date: str     # ISO date or description
    yes_price: float     # current YES price on Polymarket
    token_id: str        # YES token ID
    volume: float
    event_id: str


# Patterns for crypto price contracts
_BTC_PATTERNS = [
    # "Will Bitcoin exceed $155,000 by April 12?"
    re.compile(r"(?:Will )?(?:Bitcoin|BTC)\s+(?:exceed|hit|reach|be above|surpass|go above|be over)\s+\$?([\d,]+k?)\s+(?:by|before|on)\s+(.+?)[\?.]?$", re.IGNORECASE),
    # "Bitcoin above $155k by April 12?"
    re.compile(r"(?:Bitcoin|BTC)\s+(?:above|over)\s+\$?([\d,]+k?)\s+(?:by|before|on)\s+(.+?)[\?.]?$", re.IGNORECASE),
    # "Will BTC be below $150,000?"
    re.compile(r"(?:Will )?(?:Bitcoin|BTC)\s+(?:be below|drop below|fall below)\s+\$?([\d,]+k?)\s+(?:by|before|on)\s+(.+?)[\?.]?$", re.IGNORECASE),
    # "Bitcoin price on April 12" with specific range
    re.compile(r"(?:Will )?(?:Bitcoin|BTC)\s+(?:price|be)\s+(?:above|over|exceed)\s+\$?([\d,]+k?)(?:\s+(?:by|on|before)\s+(.+?))?[\?.]?$", re.IGNORECASE),
]

# Also match ETH
_ETH_PATTERNS = [
    re.compile(r"(?:Will )?(?:Ethereum|ETH)\s+(?:exceed|hit|reach|be above|surpass)\s+\$?([\d,]+k?)\s+(?:by|before|on)\s+(.+?)[\?.]?$", re.IGNORECASE),
]


def _parse_price(price_str: str) -> float:
    """Parse price string like '155,000' or '155k' to float."""
    s = price_str.replace(",", "").strip()
    if s.lower().endswith("k"):
        return float(s[:-1]) * 1000
    return float(s)


def scan_crypto_contracts(markets: list[Market]) -> list[CryptoContract]:
    """Scan all markets for crypto price contracts."""
    contracts: list[CryptoContract] = []

    for market in markets:
        if not market.active or market.closed:
            continue
        if not market.outcomes:
            continue

        q = market.question

        # Try BTC patterns
        for pattern in _BTC_PATTERNS:
            m = pattern.match(q)
            if m:
                try:
                    strike = _parse_price(m.group(1))
                    expiry = m.group(2).strip() if m.lastindex >= 2 else market.end_date
                    direction = "below" if "below" in q.lower() else "above"

                    contracts.append(CryptoContract(
                        market=market,
                        asset="BTC",
                        strike=strike,
                        direction=direction,
                        expiry_date=expiry or market.end_date,
                        yes_price=market.outcomes[0].price,
                        token_id=market.outcomes[0].token_id,
                        volume=market.volume,
                        event_id=market.event_id,
                    ))
                except (ValueError, IndexError):
                    pass
                break

        # Try ETH patterns
        for pattern in _ETH_PATTERNS:
            m = pattern.match(q)
            if m:
                try:
                    strike = _parse_price(m.group(1))
                    expiry = m.group(2).strip() if m.lastindex >= 2 else market.end_date

                    contracts.append(CryptoContract(
                        market=market,
                        asset="ETH",
                        strike=strike,
                        direction="above",
                        expiry_date=expiry or market.end_date,
                        yes_price=market.outcomes[0].price,
                        token_id=market.outcomes[0].token_id,
                        volume=market.volume,
                        event_id=market.event_id,
                    ))
                except (ValueError, IndexError):
                    pass
                break

    log.info("Crypto scanner: %d contracts found (%d BTC, %d ETH)",
             len(contracts),
             sum(1 for c in contracts if c.asset == "BTC"),
             sum(1 for c in contracts if c.asset == "ETH"))
    return contracts
