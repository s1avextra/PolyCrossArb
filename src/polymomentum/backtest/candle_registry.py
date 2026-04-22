"""Candle market registry — maps PMXT token_ids to candle contract metadata.

PMXT events are keyed by token_id but a strategy needs to know:
  - which candle window the token belongs to (start/end)
  - what asset (BTC/ETH/SOL)
  - whether it's the "Up" or "Down" side
  - the corresponding token_id for the opposite side (for YES+NO mispricing)

This module fetches that metadata from Polymarket's Gamma API and
caches it on disk so backtests don't re-query.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx

log = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"
DEFAULT_CACHE_DIR = Path("data/candle_registry_cache")


# Pattern: "Bitcoin Up or Down - April 4, 10:30AM-10:45AM ET"
#          "Bitcoin Up or Down - April 7, 10AM ET"
_CANDLE_PATTERN = re.compile(
    r"^(Bitcoin|Ethereum|Solana|BTC|ETH|SOL)\s+Up or Down\s*[-–—]\s*(.+?)$",
    re.IGNORECASE,
)
_TIME_RANGE = re.compile(r"(\d{1,2}):?(\d{0,2})\s*(AM|PM)\s*[-–—]\s*(\d{1,2}):?(\d{0,2})\s*(AM|PM)")
_HOURLY = re.compile(r"(\d{1,2})(AM|PM)")


@dataclass
class CandleContract:
    """A candle market with both Up and Down token IDs."""
    condition_id: str
    question: str
    asset: str              # "BTC", "ETH", "SOL"
    up_token_id: str
    down_token_id: str
    end_time_s: float       # unix timestamp of resolution
    window_minutes: float   # 5, 15, or 60
    window_label: str       # human-readable e.g. "April 4, 10:30AM-10:45AM ET"
    end_date: str           # ISO timestamp string
    closed: bool = False

    @property
    def start_time_s(self) -> float:
        return self.end_time_s - self.window_minutes * 60

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "CandleContract":
        return cls(**d)


def _parse_window_minutes(window_label: str) -> float:
    """Parse window length from a label like '10:30AM-10:45AM ET'."""
    label = window_label.strip()
    m = _TIME_RANGE.search(label)
    if m:
        h1, m1, am1, h2, m2, am2 = m.groups()
        try:
            t1 = _to_minutes(int(h1), int(m1 or 0), am1)
            t2 = _to_minutes(int(h2), int(m2 or 0), am2)
            diff = (t2 - t1) % (24 * 60)
            return float(diff) if diff > 0 else 60.0
        except (ValueError, TypeError):
            pass
    if _HOURLY.search(label):
        return 60.0
    return 15.0  # default to 15-min


def _to_minutes(hours: int, minutes: int, am_pm: str) -> int:
    hours = hours % 12
    if am_pm.upper() == "PM":
        hours += 12
    return hours * 60 + minutes


def _parse_candle_market(item: dict) -> CandleContract | None:
    """Parse a Gamma market dict into a CandleContract, or None if not a candle."""
    question = item.get("question", "")
    if not question:
        return None
    m = _CANDLE_PATTERN.match(question)
    if not m:
        return None
    asset_raw = m.group(1).upper()
    asset_map = {"BITCOIN": "BTC", "ETHEREUM": "ETH", "SOLANA": "SOL"}
    asset = asset_map.get(asset_raw, asset_raw)

    window_label = m.group(2).strip()
    window_minutes = _parse_window_minutes(window_label)

    end_date = item.get("endDate") or item.get("end_date") or ""
    try:
        if end_date:
            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            end_time_s = end_dt.timestamp()
        else:
            return None
    except (ValueError, TypeError):
        return None

    # Tokens — Gamma returns clobTokenIds as a JSON array string
    raw_tokens = item.get("clobTokenIds") or item.get("clob_token_ids") or "[]"
    if isinstance(raw_tokens, str):
        try:
            token_ids = json.loads(raw_tokens)
        except json.JSONDecodeError:
            return None
    else:
        token_ids = raw_tokens

    if not isinstance(token_ids, list) or len(token_ids) < 2:
        return None

    # By Polymarket convention, first token = "Up" (YES), second = "Down" (NO)
    up_token_id = str(token_ids[0])
    down_token_id = str(token_ids[1])

    if not up_token_id or not down_token_id:
        return None

    return CandleContract(
        condition_id=str(item.get("conditionId") or item.get("condition_id") or ""),
        question=question,
        asset=asset,
        up_token_id=up_token_id,
        down_token_id=down_token_id,
        end_time_s=end_time_s,
        window_minutes=window_minutes,
        window_label=window_label,
        end_date=end_date,
        closed=bool(item.get("closed", False)),
    )


class CandleRegistry:
    """Discovers and caches candle market metadata from Polymarket.

    Usage:
        reg = CandleRegistry()
        reg.fetch_range(start, end)  # one-time fetch via Gamma
        contract = reg.lookup(token_id)
        if contract:
            print(contract.asset, contract.window_label)
    """

    def __init__(self, cache_dir: str | Path = DEFAULT_CACHE_DIR):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._by_token: dict[str, CandleContract] = {}
        self._by_condition: dict[str, CandleContract] = {}

    @property
    def n_contracts(self) -> int:
        return len(self._by_condition)

    @property
    def n_tokens(self) -> int:
        return len(self._by_token)

    def lookup(self, token_id: str) -> CandleContract | None:
        return self._by_token.get(token_id)

    def lookup_by_condition(self, condition_id: str) -> CandleContract | None:
        return self._by_condition.get(condition_id)

    def all_token_ids(self) -> set[str]:
        return set(self._by_token.keys())

    def add(self, contract: CandleContract) -> None:
        if not contract.up_token_id or not contract.down_token_id:
            return
        self._by_token[contract.up_token_id] = contract
        self._by_token[contract.down_token_id] = contract
        self._by_condition[contract.condition_id] = contract

    def fetch_range(
        self,
        start: datetime,
        end: datetime,
        page_size: int = 500,
        max_pages: int = 200,
    ) -> int:
        """Fetch candle markets that resolved in the given range from Gamma.

        Returns number of contracts added.
        """
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        # Check disk cache first — include hours so a narrower fetch doesn't
        # poison a wider one (e.g. fetching 07-13 then 07-21 same day).
        cache_file = self._cache_dir / f"range_{start.strftime('%Y%m%dT%H')}_{end.strftime('%Y%m%dT%H')}.json"
        if cache_file.exists():
            try:
                with open(cache_file) as f:
                    data = json.load(f)
                added = 0
                for item in data:
                    self.add(CandleContract.from_dict(item))
                    added += 1
                log.info("Loaded %d candle contracts from cache %s", added, cache_file.name)
                return added
            except Exception as e:
                log.warning("Cache load failed: %s", e)

        # Fetch via Gamma API — closed candle markets, paginated
        added = 0
        scanned_contracts: list[CandleContract] = []

        with httpx.Client(timeout=20.0) as client:
            for page in range(max_pages):
                offset = page * page_size
                try:
                    resp = client.get(
                        f"{GAMMA_BASE}/markets",
                        params={
                            "limit": page_size,
                            "offset": offset,
                            "closed": "true",
                            "end_date_min": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "end_date_max": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "order": "endDate",
                            "ascending": "true",
                        },
                    )
                    resp.raise_for_status()
                    items = resp.json()
                except Exception as e:
                    log.warning("Gamma fetch failed page=%d: %s", page, str(e)[:80])
                    break

                if not items:
                    break

                page_added = 0
                for item in items:
                    contract = _parse_candle_market(item)
                    if contract is None:
                        continue
                    self.add(contract)
                    scanned_contracts.append(contract)
                    page_added += 1
                    added += 1

                log.debug("Page %d: %d items, %d candles", page, len(items), page_added)
                if len(items) < page_size:
                    break

        log.info(
            "Fetched %d candle contracts from Gamma (%s -> %s)",
            added, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"),
        )

        # Cache to disk
        try:
            with open(cache_file, "w") as f:
                json.dump([c.to_dict() for c in scanned_contracts], f)
            log.info("Cached registry to %s", cache_file.name)
        except Exception as e:
            log.warning("Cache save failed: %s", e)

        return added

    def filter_by_asset(self, asset: str) -> list[CandleContract]:
        return [c for c in self._by_condition.values() if c.asset == asset]

    def filter_by_window_minutes(self, minutes: float) -> list[CandleContract]:
        return [c for c in self._by_condition.values() if c.window_minutes == minutes]
