"""Classify Polymarket markets as weather temperature markets.

Scans the full market list, parses weather questions, and groups
them into WeatherEvents (one per city/date combination).
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

from polycrossarb.data.models import Market
from polycrossarb.weather.cities import CityConfig, get_city_config
from polycrossarb.weather.parser import WeatherMarketInfo, parse_weather_question

log = logging.getLogger(__name__)


@dataclass
class WeatherBracket:
    """A single temperature bracket within a weather event."""
    market: Market
    info: WeatherMarketInfo
    yes_price: float
    token_id: str  # YES token ID for this bracket


@dataclass
class WeatherEvent:
    """A complete weather event: all temperature brackets for one city/date."""
    event_id: str
    city: str
    date: str               # "March 30"
    unit: str               # "F" or "C"
    city_config: CityConfig | None
    brackets: list[WeatherBracket] = field(default_factory=list)
    resolution_utc: datetime | None = None

    @property
    def yes_sum(self) -> float:
        return sum(b.yes_price for b in self.brackets)

    @property
    def mispricing(self) -> float:
        return abs(self.yes_sum - 1.0)

    @property
    def total_volume(self) -> float:
        return sum(b.market.volume for b in self.brackets)

    @property
    def n_brackets(self) -> int:
        return len(self.brackets)

    def find_bracket_for_temp(self, temp: float) -> WeatherBracket | None:
        """Find which bracket a temperature falls into."""
        for b in self.brackets:
            if b.info.matches_temp(temp):
                return b
        return None


_DATE_PARSE_PATTERNS = [
    re.compile(r"(\w+ \d+)"),  # "March 30"
    re.compile(r"(\d{4}-\d{2}-\d{2})"),  # "2026-03-30"
]


def _parse_date_to_iso(date_str: str, year: int | None = None) -> str | None:
    """Convert 'March 30' to '2026-03-30'. Auto-detects current year."""
    if year is None:
        year = datetime.now().year
    try:
        for fmt in ["%B %d", "%b %d", "%Y-%m-%d"]:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if dt.year == 1900:
                    dt = dt.replace(year=year)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    except Exception:
        pass
    return None


def scan_weather_markets(markets: list[Market]) -> list[WeatherEvent]:
    """Scan all markets and identify weather temperature events.

    Returns a list of WeatherEvents, each containing all brackets
    for one city/date combination, sorted by resolution time.
    """
    # Group by event_id → parse → build WeatherEvents
    by_event: dict[str, list[tuple[Market, WeatherMarketInfo]]] = defaultdict(list)

    parsed = 0
    for market in markets:
        if not market.active or market.closed:
            continue

        info = parse_weather_question(market.question)
        if info is None:
            continue

        parsed += 1
        by_event[market.event_id].append((market, info))

    # Build WeatherEvents
    events: list[WeatherEvent] = []
    for event_id, market_infos in by_event.items():
        if not market_infos:
            continue

        first_info = market_infos[0][1]
        city = first_info.city
        date = first_info.date
        unit = first_info.unit
        city_config = get_city_config(city)

        brackets: list[WeatherBracket] = []
        for market, info in market_infos:
            yes_price = market.outcomes[0].price if market.outcomes else 0
            token_id = market.outcomes[0].token_id if market.outcomes else ""
            brackets.append(WeatherBracket(
                market=market,
                info=info,
                yes_price=yes_price,
                token_id=token_id,
            ))

        # Sort brackets by temperature (lowest first)
        def _sort_key(b: WeatherBracket) -> int:
            if b.info.is_lower_bound:
                return b.info.bracket_high or -999
            return b.info.bracket_low or 0

        brackets.sort(key=_sort_key)

        # Calculate resolution time (local midnight → UTC)
        resolution_utc = None
        if city_config:
            try:
                from zoneinfo import ZoneInfo
                date_iso = _parse_date_to_iso(date)
                if date_iso:
                    local_midnight = datetime.strptime(
                        f"{date_iso} 23:59:59", "%Y-%m-%d %H:%M:%S"
                    ).replace(tzinfo=ZoneInfo(city_config.timezone))
                    resolution_utc = local_midnight.astimezone(timezone.utc)
            except Exception:
                pass

        events.append(WeatherEvent(
            event_id=event_id,
            city=city,
            date=date,
            unit=unit,
            city_config=city_config,
            brackets=brackets,
            resolution_utc=resolution_utc,
        ))

    # Sort by resolution time (soonest first)
    events.sort(key=lambda e: e.resolution_utc or datetime.max.replace(tzinfo=timezone.utc))

    log.info(
        "Weather scanner: %d markets parsed, %d events, %d cities",
        parsed, len(events), len({e.city for e in events}),
    )
    return events
