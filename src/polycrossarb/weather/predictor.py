"""Resolution predictor: given temperature data + time, predict winning bracket.

Confidence model based on the physics of daily temperature cycles:
  - "Highest temperature" = daily maximum
  - Daily max typically occurs 2-5pm local time
  - Once past peak hours, the max is locked in (~90% of days)
  - The observed max can only increase, never decrease

Confidence increases with:
  1. Hours past peak (more time = less chance of new max)
  2. Temperature clearly inside a bracket (not on boundary)
  3. Multiple sources agreeing
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from polycrossarb.weather.cities import CityConfig
from polycrossarb.weather.data_sources import TemperatureReading
from polycrossarb.weather.scanner import WeatherBracket, WeatherEvent

log = logging.getLogger(__name__)


@dataclass
class BracketPrediction:
    """Prediction for which bracket will win."""
    event: WeatherEvent
    winning_bracket: WeatherBracket
    confidence: float           # 0.0 to 1.0
    current_max_temp: float
    hours_past_peak: float
    hours_until_resolution: float
    reason: str


def predict_outcome(
    event: WeatherEvent,
    reading: TemperatureReading,
    now: datetime | None = None,
) -> BracketPrediction | None:
    """Predict which bracket will win based on current weather data.

    Returns None if confidence is too low to make a prediction.
    """
    if not event.city_config or not event.brackets:
        return None

    if now is None:
        now = datetime.now(timezone.utc)

    cfg = event.city_config
    max_temp = reading.max_today

    # Calculate time context
    local_tz = ZoneInfo(cfg.timezone)
    local_now = now.astimezone(local_tz)
    local_hour = local_now.hour + local_now.minute / 60

    hours_past_peak = max(0, local_hour - cfg.peak_hour)
    hours_until_resolution = 0.0
    if event.resolution_utc:
        hours_until_resolution = max(0, (event.resolution_utc - now).total_seconds() / 3600)

    # Find which bracket the current max falls into
    winning = event.find_bracket_for_temp(max_temp)
    if winning is None:
        return None

    # ── Confidence model ──────────────────────────────────────────

    # Base confidence from hours past peak
    if hours_past_peak > 4:
        # Well past peak — daily max is almost certainly set
        base_confidence = 0.97
    elif hours_past_peak > 2:
        # Past peak — very likely set
        base_confidence = 0.90
    elif hours_past_peak > 0:
        # Near peak — could still rise
        base_confidence = 0.70
    elif local_hour > 10:
        # Late morning — temperature still climbing
        base_confidence = 0.50
    else:
        # Early morning — too early
        base_confidence = 0.30

    # Adjustment: how far is the temp from bracket boundaries?
    bracket_margin = _bracket_margin(winning.info, max_temp)
    if bracket_margin >= 2:
        # Clearly inside bracket — high confidence
        boundary_adj = 0.05
    elif bracket_margin >= 1:
        boundary_adj = 0.0
    elif bracket_margin >= 0.5:
        # Close to boundary — could go either way
        boundary_adj = -0.10
    else:
        # Very close to boundary
        boundary_adj = -0.20

    # Adjustment: time until resolution
    # If resolution is imminent (<1h), we're very confident
    if hours_until_resolution < 1:
        time_adj = 0.05
    elif hours_until_resolution < 3:
        time_adj = 0.02
    else:
        time_adj = 0.0

    confidence = min(0.99, max(0.10, base_confidence + boundary_adj + time_adj))

    reason_parts = []
    reason_parts.append(f"max_temp={max_temp:.1f}°{event.unit}")
    reason_parts.append(f"bracket={winning.info.bracket_label}")
    reason_parts.append(f"hours_past_peak={hours_past_peak:.1f}")
    reason_parts.append(f"margin={bracket_margin:.1f}°")
    reason = ", ".join(reason_parts)

    return BracketPrediction(
        event=event,
        winning_bracket=winning,
        confidence=confidence,
        current_max_temp=max_temp,
        hours_past_peak=hours_past_peak,
        hours_until_resolution=hours_until_resolution,
        reason=reason,
    )


def _bracket_margin(info, temp: float) -> float:
    """How far the temperature is from the nearest bracket boundary.

    Higher margin = more confidence the temp won't cross into another bracket.
    """
    t = round(temp)

    if info.is_lower_bound and info.bracket_high is not None:
        return info.bracket_high - t  # distance below upper limit
    if info.is_upper_bound and info.bracket_low is not None:
        return t - info.bracket_low  # distance above lower limit
    if info.bracket_low is not None and info.bracket_high is not None:
        # Distance from nearest boundary
        return min(t - info.bracket_low, info.bracket_high - t)
    return 0
