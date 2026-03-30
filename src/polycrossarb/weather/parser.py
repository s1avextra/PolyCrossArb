"""Parse weather temperature market questions into structured data.

Polymarket weather market questions follow a uniform format:
  "Will the highest temperature in {City} be {bracket} on {Date}?"

Bracket formats:
  - "73°F or below"         → lower bound
  - "between 74-75°F"       → range (US, 2°F increments)
  - "26°C"                  → exact degree (international)
  - "28°C or higher"        → upper bound
  - "73°F or lower"         → lower bound (alt wording)
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class WeatherMarketInfo:
    """Parsed weather market question."""
    city: str
    date: str               # "March 30" or "April 1" etc.
    bracket_low: int | None  # lower temperature bound (inclusive)
    bracket_high: int | None # upper temperature bound (inclusive)
    unit: str               # "F" or "C"
    is_lower_bound: bool    # "X or below" — covers everything <= X
    is_upper_bound: bool    # "X or higher" — covers everything >= X

    @property
    def bracket_label(self) -> str:
        if self.is_lower_bound and self.bracket_high is not None:
            return f"{self.bracket_high}°{self.unit} or below"
        if self.is_upper_bound and self.bracket_low is not None:
            return f"{self.bracket_low}°{self.unit} or higher"
        if self.bracket_low is not None and self.bracket_high is not None:
            if self.bracket_low == self.bracket_high:
                return f"{self.bracket_low}°{self.unit}"
            return f"{self.bracket_low}-{self.bracket_high}°{self.unit}"
        return "?"

    def matches_temp(self, temp: float) -> bool:
        """Check if a temperature falls in this bracket."""
        t = round(temp)
        if self.is_lower_bound:
            return t <= (self.bracket_high or 0)
        if self.is_upper_bound:
            return t >= (self.bracket_low or 0)
        if self.bracket_low is not None and self.bracket_high is not None:
            return self.bracket_low <= t <= self.bracket_high
        return False


# Main question pattern
_QUESTION_RE = re.compile(
    r"(?:Will )?the highest temperature in (.+?) be (.+?) on (.+?)[\?.]?$",
    re.IGNORECASE,
)

# Bracket patterns (applied to the bracket substring)
_BOUND_LOW_RE = re.compile(r"(\d+)\s*°\s*([CF])\s+or (?:below|lower)", re.IGNORECASE)
_BOUND_HIGH_RE = re.compile(r"(\d+)\s*°\s*([CF])\s+or (?:higher|above)", re.IGNORECASE)
_RANGE_RE = re.compile(r"(?:between\s+)?(\d+)\s*(?:-|–)\s*(\d+)\s*°\s*([CF])", re.IGNORECASE)
_EXACT_RE = re.compile(r"(\d+)\s*°\s*([CF])$", re.IGNORECASE)


def parse_weather_question(question: str) -> WeatherMarketInfo | None:
    """Parse a Polymarket weather market question.

    Returns None if the question doesn't match the weather format.
    """
    # Clean up unicode degree symbol variants
    question = question.replace("\u00b0", "°").replace("℃", "°C").replace("℉", "°F")

    m = _QUESTION_RE.match(question.strip())
    if not m:
        return None

    city = m.group(1).strip()
    bracket_str = m.group(2).strip()
    date = m.group(3).strip()

    # Try each bracket pattern
    # Lower bound: "73°F or below"
    bm = _BOUND_LOW_RE.search(bracket_str)
    if bm:
        return WeatherMarketInfo(
            city=city, date=date,
            bracket_low=None, bracket_high=int(bm.group(1)),
            unit=bm.group(2).upper(),
            is_lower_bound=True, is_upper_bound=False,
        )

    # Upper bound: "28°C or higher"
    bm = _BOUND_HIGH_RE.search(bracket_str)
    if bm:
        return WeatherMarketInfo(
            city=city, date=date,
            bracket_low=int(bm.group(1)), bracket_high=None,
            unit=bm.group(2).upper(),
            is_lower_bound=False, is_upper_bound=True,
        )

    # Range: "between 74-75°F"
    bm = _RANGE_RE.search(bracket_str)
    if bm:
        return WeatherMarketInfo(
            city=city, date=date,
            bracket_low=int(bm.group(1)), bracket_high=int(bm.group(2)),
            unit=bm.group(3).upper(),
            is_lower_bound=False, is_upper_bound=False,
        )

    # Exact: "26°C"
    bm = _EXACT_RE.search(bracket_str)
    if bm:
        val = int(bm.group(1))
        return WeatherMarketInfo(
            city=city, date=date,
            bracket_low=val, bracket_high=val,
            unit=bm.group(2).upper(),
            is_lower_bound=False, is_upper_bound=False,
        )

    return None
