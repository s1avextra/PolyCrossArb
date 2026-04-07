"""Shared rate limiter for Polymarket API calls.

Enforces a global rate limit across all strategies to prevent 429 errors.
Uses a token bucket algorithm with configurable rates.

Usage:
    from polycrossarb.data.rate_limiter import rate_limiter

    async with rate_limiter.acquire("gamma"):
        response = await client.get(...)
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import AsyncIterator

log = logging.getLogger(__name__)


@dataclass
class _BucketConfig:
    rate: float  # tokens per second (sustained)
    burst_rate: float  # tokens per second during burst window
    burst_window: float  # seconds of burst allowance after reset


@dataclass
class _Bucket:
    config: _BucketConfig
    tokens: float = 0.0
    last_refill: float = field(default_factory=time.monotonic)
    burst_start: float = field(default_factory=time.monotonic)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __post_init__(self) -> None:
        # Start with a full burst bucket
        self.tokens = self.config.burst_rate * self.config.burst_window

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        if elapsed <= 0:
            return

        # Use burst rate if still within burst window
        in_burst = (now - self.burst_start) < self.config.burst_window
        rate = self.config.burst_rate if in_burst else self.config.rate

        self.tokens = min(
            self.tokens + elapsed * rate,
            rate * self.config.burst_window,  # cap at burst-window worth of tokens
        )
        self.last_refill = now


# Default bucket configurations
_DEFAULT_BUCKETS: dict[str, _BucketConfig] = {
    "gamma": _BucketConfig(rate=5.0, burst_rate=15.0, burst_window=5.0),
    "clob": _BucketConfig(rate=10.0, burst_rate=30.0, burst_window=5.0),
    "weather_api": _BucketConfig(rate=5.0, burst_rate=15.0, burst_window=5.0),
}


class RateLimiter:
    """Process-level async rate limiter with named token buckets."""

    def __init__(self, configs: dict[str, _BucketConfig] | None = None) -> None:
        self._configs = configs or dict(_DEFAULT_BUCKETS)
        self._buckets: dict[str, _Bucket] = {}
        self._init_lock = asyncio.Lock()

    async def _get_bucket(self, name: str) -> _Bucket:
        if name in self._buckets:
            return self._buckets[name]
        async with self._init_lock:
            # Double-check after acquiring lock
            if name not in self._buckets:
                config = self._configs.get(name)
                if config is None:
                    raise ValueError(
                        f"Unknown rate-limit bucket {name!r}. "
                        f"Known: {sorted(self._configs)}"
                    )
                self._buckets[name] = _Bucket(config=config)
            return self._buckets[name]

    @asynccontextmanager
    async def acquire(self, bucket_name: str) -> AsyncIterator[None]:
        """Acquire a token from the named bucket, waiting if necessary."""
        bucket = await self._get_bucket(bucket_name)
        async with bucket.lock:
            bucket._refill()
            while bucket.tokens < 1.0:
                # Calculate wait time for one token
                in_burst = (
                    time.monotonic() - bucket.burst_start
                ) < bucket.config.burst_window
                rate = bucket.config.burst_rate if in_burst else bucket.config.rate
                wait = (1.0 - bucket.tokens) / rate
                log.debug("Rate limiter [%s]: waiting %.3fs", bucket_name, wait)
                await asyncio.sleep(wait)
                bucket._refill()
            bucket.tokens -= 1.0
        yield

    def reset_burst(self, bucket_name: str) -> None:
        """Reset the burst window for a bucket (e.g. after a long idle period)."""
        if bucket_name in self._buckets:
            self._buckets[bucket_name].burst_start = time.monotonic()


# Module-level singleton
rate_limiter = RateLimiter()
