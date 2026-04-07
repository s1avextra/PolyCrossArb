"""Weather data fetcher: real-time temperature from free public APIs.

Sources (all free, no paid APIs):
  1. Open-Meteo: global, no API key, no rate limit
  2. Weather.gov/NOAA: US cities only, no API key, official data
  3. OpenWeatherMap: global, free tier (1000 calls/day)

The aggregator tracks max_observed_temp per city/date — since we're
trading "highest temperature" markets, the observed max only increases.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import httpx

from polycrossarb.weather.cities import CityConfig

log = logging.getLogger(__name__)


@dataclass
class TemperatureReading:
    """A temperature reading from an external source."""
    city: str
    current_temp: float      # current temperature
    max_today: float         # max observed today (from API or our tracking)
    unit: str                # "C" or "F"
    source: str              # "open_meteo", "noaa", "owm"
    timestamp: float = 0.0
    confidence: float = 1.0  # 1.0 if reliable


class WeatherAggregator:
    """Aggregates temperature data from multiple sources.

    Tracks the running maximum observed temperature per city.
    Auto-resets max_today at each city's local midnight.
    """

    def __init__(self, owm_api_key: str = ""):
        self._owm_key = owm_api_key
        self._client = httpx.AsyncClient(timeout=10.0)
        self._max_temps: dict[str, float] = {}  # city -> max observed temp
        self._last_readings: dict[str, TemperatureReading] = {}
        self._last_fetch: dict[str, float] = {}
        self._last_reset_date: dict[str, str] = {}  # city -> "YYYY-MM-DD" of last reset

    async def close(self):
        await self._client.aclose()

    def _check_daily_reset(self, city_config: CityConfig):
        """Reset max_today at the city's local midnight."""
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo
        local_now = datetime.now(timezone.utc).astimezone(ZoneInfo(city_config.timezone))
        local_date = local_now.strftime("%Y-%m-%d")

        last_date = self._last_reset_date.get(city_config.name)
        if last_date != local_date:
            # New local day — reset max for this city
            self._max_temps.pop(city_config.name, None)
            self._last_reset_date[city_config.name] = local_date

    async def fetch(self, city_config: CityConfig) -> TemperatureReading | None:
        """Fetch current temperature for a city from the best available source."""
        city = city_config.name

        # Auto-reset max at local midnight
        self._check_daily_reset(city_config)

        # Rate limit: don't fetch same city more than once per 20s
        now = time.time()
        if city in self._last_fetch and now - self._last_fetch[city] < 20:
            return self._last_readings.get(city)

        reading = None

        # Try Open-Meteo first (global, free, no key)
        reading = await self._fetch_open_meteo(city_config)

        # For US cities, also try NOAA
        if city_config.noaa_station and reading is None:
            reading = await self._fetch_noaa(city_config)

        # Fallback: OpenWeatherMap (if key configured)
        if reading is None and self._owm_key:
            reading = await self._fetch_owm(city_config)

        if reading:
            reading.timestamp = now
            self._last_fetch[city] = now
            self._last_readings[city] = reading

            # Update max observed
            prev_max = self._max_temps.get(city, -999)
            if reading.current_temp > prev_max:
                self._max_temps[city] = reading.current_temp
            reading.max_today = max(reading.current_temp, prev_max)

        return reading

    def get_max_observed(self, city: str) -> float | None:
        """Get the highest temperature we've seen for this city today."""
        return self._max_temps.get(city)

    def reset_daily(self):
        """Reset max temps at start of new day."""
        self._max_temps.clear()

    async def _fetch_open_meteo(self, cfg: CityConfig) -> TemperatureReading | None:
        """Fetch from Open-Meteo (free, global, no key)."""
        try:
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": cfg.lat,
                "longitude": cfg.lon,
                "current_weather": "true",
                "temperature_unit": "fahrenheit" if cfg.temp_unit == "F" else "celsius",
            }
            resp = await self._client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            current = data.get("current_weather", {})
            temp = current.get("temperature")
            if temp is not None:
                return TemperatureReading(
                    city=cfg.name,
                    current_temp=float(temp),
                    max_today=float(temp),
                    unit=cfg.temp_unit,
                    source="open_meteo",
                )
        except Exception:
            log.debug("Open-Meteo failed for %s", cfg.name)
        return None

    async def _fetch_noaa(self, cfg: CityConfig) -> TemperatureReading | None:
        """Fetch from NOAA/Weather.gov (US only, free, no key)."""
        if not cfg.noaa_station:
            return None
        try:
            url = f"https://api.weather.gov/stations/{cfg.noaa_station}/observations/latest"
            headers = {"User-Agent": "PolyCrossArb/1.0"}
            resp = await self._client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            props = data.get("properties", {})
            temp_c = props.get("temperature", {}).get("value")
            if temp_c is not None:
                temp = temp_c * 9 / 5 + 32 if cfg.temp_unit == "F" else temp_c
                return TemperatureReading(
                    city=cfg.name,
                    current_temp=round(temp, 1),
                    max_today=round(temp, 1),
                    unit=cfg.temp_unit,
                    source="noaa",
                )
        except Exception:
            log.debug("NOAA failed for %s", cfg.name)
        return None

    async def _fetch_owm(self, cfg: CityConfig) -> TemperatureReading | None:
        """Fetch from OpenWeatherMap (free tier, 1000 calls/day)."""
        if not self._owm_key:
            return None
        try:
            url = "https://api.openweathermap.org/data/2.5/weather"
            units = "imperial" if cfg.temp_unit == "F" else "metric"
            params = {
                "lat": cfg.lat, "lon": cfg.lon,
                "appid": self._owm_key, "units": units,
            }
            resp = await self._client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            temp = data.get("main", {}).get("temp")
            if temp is not None:
                return TemperatureReading(
                    city=cfg.name,
                    current_temp=float(temp),
                    max_today=float(temp),
                    unit=cfg.temp_unit,
                    source="owm",
                )
        except Exception:
            log.debug("OWM failed for %s", cfg.name)
        return None
