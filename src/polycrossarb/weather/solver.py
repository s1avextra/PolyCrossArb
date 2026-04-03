"""Known-outcome solver: position sizing for weather trades.

When we KNOW (with high confidence) which temperature bracket will win:
  - Buy YES on the predicted winner
  - Profit = ($1.00 - purchase_price) × shares
  - Size by Kelly criterion weighted by confidence

This is NOT an LP problem — it's a direct Kelly calculation.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

from polycrossarb.weather.predictor import BracketPrediction

log = logging.getLogger(__name__)


@dataclass
class WeatherTradeOrder:
    """A trade order for a weather market."""
    market_condition_id: str
    outcome_idx: int        # 0 = YES
    side: str               # "buy"
    size: float             # shares
    price: float            # limit price
    expected_profit: float  # if correct
    confidence: float
    city: str
    date: str
    bracket: str
    event_id: str = ""
    neg_risk: bool = True
    token_id: str = ""


def size_weather_trade(
    prediction: BracketPrediction,
    bankroll: float,
    max_position_pct: float = 0.20,
    kelly_fraction: float = 0.25,
    min_edge: float = 0.05,
) -> WeatherTradeOrder | None:
    """Calculate optimal position size for a weather trade.

    Uses Kelly criterion weighted by confidence:
      edge = confidence × (1/price - 1) - (1 - confidence)
      kelly = edge / (1/price - 1)
      position = kelly × kelly_fraction × bankroll

    Args:
        prediction: The bracket prediction with confidence score.
        bankroll: Total available capital.
        max_position_pct: Max fraction of bankroll per trade.
        kelly_fraction: Fractional Kelly (0.25 = quarter Kelly).
        min_edge: Minimum edge to trade (skip if below).
    """
    bracket = prediction.winning_bracket
    price = bracket.yes_price

    if price <= 0.01 or price >= 0.99:
        return None  # too extreme — either already priced in or something is wrong

    confidence = prediction.confidence

    # Calculate edge
    # If we buy at `price` and we're right with probability `confidence`:
    #   Expected return = confidence × (1.0 - price) - (1 - confidence) × price
    #   Simplified: expected_return = confidence - price
    expected_return_per_dollar = confidence - price

    if expected_return_per_dollar < min_edge:
        return None  # not enough edge

    # Kelly criterion for binary bet
    # b = net odds = (1 - price) / price = payout per dollar wagered
    # p = probability of winning = confidence
    # q = probability of losing = 1 - confidence
    # kelly = (b*p - q) / b
    b = (1.0 - price) / price
    kelly_raw = (b * confidence - (1 - confidence)) / b
    if kelly_raw <= 0:
        return None

    # Apply fractional Kelly
    kelly_adj = kelly_raw * kelly_fraction

    # Calculate position in dollars
    max_position = bankroll * max_position_pct
    position_usd = min(kelly_adj * bankroll, max_position)

    if position_usd < 1.0:
        return None  # below Polymarket minimum

    # Convert to shares
    shares = position_usd / price
    expected_profit = shares * (1.0 - price) * confidence - shares * price * (1 - confidence)

    order = WeatherTradeOrder(
        market_condition_id=bracket.market.condition_id,
        outcome_idx=0,  # YES token
        side="buy",
        size=round(shares, 1),
        price=price,
        expected_profit=round(expected_profit, 4),
        confidence=confidence,
        city=prediction.event.city,
        date=prediction.event.date,
        bracket=bracket.info.bracket_label,
        event_id=prediction.event.event_id,
        neg_risk=bracket.market.neg_risk,
        token_id=bracket.token_id,
    )

    log.info(
        "Weather trade: %s %s %s — buy YES @ $%.3f, %d shares, conf=%.0f%%, edge=%.1f%%, kelly=%.1f%%, profit=$%.2f",
        prediction.event.city, prediction.event.date, bracket.info.bracket_label,
        price, shares, confidence * 100, expected_return_per_dollar * 100,
        kelly_raw * 100, expected_profit,
    )

    return order
