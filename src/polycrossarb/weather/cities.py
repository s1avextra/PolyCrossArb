"""City configuration database for weather markets.

Maps Polymarket city names to timezone, coordinates, weather station IDs,
and temperature units. Used for resolution timing and data fetching.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CityConfig:
    name: str
    timezone: str       # IANA timezone
    lat: float
    lon: float
    noaa_station: str   # NOAA/NWS station ID (US cities only, "" for international)
    temp_unit: str      # "F" (US) or "C" (international)
    peak_hour: int      # typical hour of daily max temp (local time, 24h)


# All 37 cities from Polymarket weather markets (as of March 2026 snapshot)
CITIES: dict[str, CityConfig] = {
    # Asia-Pacific
    "Hong Kong": CityConfig("Hong Kong", "Asia/Hong_Kong", 22.32, 114.17, "", "C", 15),
    "Tokyo": CityConfig("Tokyo", "Asia/Tokyo", 35.68, 139.69, "", "C", 14),
    "Seoul": CityConfig("Seoul", "Asia/Seoul", 37.57, 126.98, "", "C", 15),
    "Beijing": CityConfig("Beijing", "Asia/Shanghai", 39.90, 116.41, "", "C", 15),
    "Shanghai": CityConfig("Shanghai", "Asia/Shanghai", 31.23, 121.47, "", "C", 15),
    "Taipei": CityConfig("Taipei", "Asia/Taipei", 25.03, 121.57, "", "C", 14),
    "Singapore": CityConfig("Singapore", "Asia/Singapore", 1.35, 103.82, "", "C", 14),
    "Sydney": CityConfig("Sydney", "Australia/Sydney", -33.87, 151.21, "", "C", 14),
    "Wellington": CityConfig("Wellington", "Pacific/Auckland", -41.29, 174.78, "", "C", 14),

    # Europe
    "London": CityConfig("London", "Europe/London", 51.51, -0.13, "", "C", 15),
    "Paris": CityConfig("Paris", "Europe/Paris", 48.86, 2.35, "", "C", 15),
    "Munich": CityConfig("Munich", "Europe/Berlin", 48.14, 11.58, "", "C", 15),
    "Ankara": CityConfig("Ankara", "Europe/Istanbul", 39.93, 32.86, "", "C", 15),
    "Istanbul": CityConfig("Istanbul", "Europe/Istanbul", 41.01, 28.98, "", "C", 15),

    # Middle East
    "Tel Aviv": CityConfig("Tel Aviv", "Asia/Jerusalem", 32.09, 34.78, "", "C", 14),
    "Dubai": CityConfig("Dubai", "Asia/Dubai", 25.20, 55.27, "", "C", 14),

    # Africa
    "Johannesburg": CityConfig("Johannesburg", "Africa/Johannesburg", -26.20, 28.04, "", "C", 14),

    # South America
    "Sao Paulo": CityConfig("Sao Paulo", "America/Sao_Paulo", -23.55, -46.63, "", "C", 15),
    "Buenos Aires": CityConfig("Buenos Aires", "America/Argentina/Buenos_Aires", -34.60, -58.38, "", "C", 15),
    "Mexico City": CityConfig("Mexico City", "America/Mexico_City", 19.43, -99.13, "", "C", 15),

    # North America — US (Fahrenheit, NOAA stations)
    "New York": CityConfig("New York", "America/New_York", 40.71, -74.01, "KNYC", "F", 15),
    "NYC": CityConfig("NYC", "America/New_York", 40.71, -74.01, "KNYC", "F", 15),
    "Los Angeles": CityConfig("Los Angeles", "America/Los_Angeles", 34.05, -118.24, "KLAX", "F", 15),
    "Chicago": CityConfig("Chicago", "America/Chicago", 41.88, -87.63, "KORD", "F", 15),
    "Houston": CityConfig("Houston", "America/Chicago", 29.76, -95.37, "KIAH", "F", 15),
    "Atlanta": CityConfig("Atlanta", "America/New_York", 33.75, -84.39, "KATL", "F", 15),
    "Dallas": CityConfig("Dallas", "America/Chicago", 32.78, -96.80, "KDFW", "F", 15),
    "Austin": CityConfig("Austin", "America/Chicago", 30.27, -97.74, "KAUS", "F", 15),
    "Miami": CityConfig("Miami", "America/New_York", 25.76, -80.19, "KMIA", "F", 15),
    "Seattle": CityConfig("Seattle", "America/Los_Angeles", 47.61, -122.33, "KSEA", "F", 15),
    "Denver": CityConfig("Denver", "America/Denver", 39.74, -104.99, "KDEN", "F", 15),
    "San Francisco": CityConfig("San Francisco", "America/Los_Angeles", 37.77, -122.42, "KSFO", "F", 15),
    "Toronto": CityConfig("Toronto", "America/Toronto", 43.65, -79.38, "", "C", 15),
    "Lucknow": CityConfig("Lucknow", "Asia/Kolkata", 26.85, 80.95, "", "C", 14),
}


def get_city_config(city_name: str) -> CityConfig | None:
    """Look up city config by name (case-insensitive, fuzzy)."""
    # Exact match
    if city_name in CITIES:
        return CITIES[city_name]

    # Case-insensitive
    lower = city_name.lower()
    for name, cfg in CITIES.items():
        if name.lower() == lower:
            return cfg

    # Partial match (e.g. "New York City" → "New York")
    for name, cfg in CITIES.items():
        if name.lower() in lower or lower in name.lower():
            return cfg

    return None
