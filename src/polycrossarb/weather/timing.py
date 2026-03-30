"""Timezone scheduler: prioritize cities by resolution time and peak status.

The bot trades 24/7 cycling through timezones:
  - Asian cities resolve first (HK midnight = 4pm UTC previous day)
  - European cities next
  - American cities last

Within each timezone window, prioritize cities that are past peak
(highest confidence for temperature prediction).
"""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from polycrossarb.weather.scanner import WeatherEvent


def get_trading_priority(events: list[WeatherEvent], now: datetime | None = None) -> list[WeatherEvent]:
    """Sort weather events by trading urgency.

    Priority (highest first):
      1. Past peak + before resolution (we know the answer, market hasn't resolved)
      2. Near peak + before resolution (temp likely near max)
      3. Too early (pre-peak, low confidence)
      4. Already resolved (skip)
    """
    if now is None:
        now = datetime.now(timezone.utc)

    scored: list[tuple[float, WeatherEvent]] = []

    for event in events:
        if not event.city_config or not event.resolution_utc:
            continue

        hours_until_resolution = (event.resolution_utc - now).total_seconds() / 3600

        # Skip resolved events
        if hours_until_resolution <= 0:
            continue

        # Calculate local time
        local_tz = ZoneInfo(event.city_config.timezone)
        local_now = now.astimezone(local_tz)
        local_hour = local_now.hour + local_now.minute / 60

        hours_past_peak = max(0, local_hour - event.city_config.peak_hour)

        # Scoring: higher = more urgent to trade
        score = 0.0

        # Best: past peak AND resolving within 8 hours
        if hours_past_peak > 2 and hours_until_resolution < 8:
            score = 100 + hours_past_peak  # best window
        elif hours_past_peak > 0 and hours_until_resolution < 12:
            score = 50 + hours_past_peak
        elif local_hour > 10 and hours_until_resolution < 16:
            score = 20
        else:
            score = 1  # too early

        # Bonus for imminent resolution
        if hours_until_resolution < 2:
            score += 50

        scored.append((score, event))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [event for _, event in scored]
