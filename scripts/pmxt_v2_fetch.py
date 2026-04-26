#!/usr/bin/env python3
"""Fetch a range of hourly v2 PMXT parquet files into the shared cache.

Idempotent — skips already-cached files. Default cache location is
/opt/shared/pmxt_v2_cache on VPS (shared with polyarbitrage), or
data/pmxt_v2_cache on a laptop. Override with --cache-dir or the
PMXT_V2_CACHE_DIR env var.

Usage:
    # Fetch yesterday's bot trading window
    uv run python scripts/pmxt_v2_fetch.py \\
        --start "2026-04-25T11" --end "2026-04-25T16"

    # Fetch a whole day
    uv run python scripts/pmxt_v2_fetch.py --start "2026-04-25T00" --end "2026-04-25T23"

    # Force re-download
    uv run python scripts/pmxt_v2_fetch.py --start "2026-04-25T11" --end "2026-04-25T11" --force

Files: ~100-400 MB each. A full day is ~6 GB. Cache is shared
multi-tenant — both polymomentum and polyarbitrage read from the same
location, so don't bother re-fetching what someone else already has.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "src")

from polymomentum.backtest.pmxt_v2_loader import PMXTv2Loader, DEFAULT_CACHE_DIR


def parse_hour(s: str) -> datetime:
    """Parse YYYY-MM-DDTHH into a UTC datetime."""
    return datetime.strptime(s, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--start", required=True,
                        help="Start hour (UTC, format: YYYY-MM-DDTHH, e.g. 2026-04-25T11)")
    parser.add_argument("--end", required=True,
                        help="End hour INCLUSIVE (same format)")
    parser.add_argument("--cache-dir", default=None,
                        help=f"Override cache directory (default: {DEFAULT_CACHE_DIR})")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if file is already cached")
    args = parser.parse_args()

    try:
        start = parse_hour(args.start)
        end = parse_hour(args.end)
    except ValueError as e:
        print(f"Bad date format: {e}. Use YYYY-MM-DDTHH (e.g. 2026-04-25T11).",
              file=sys.stderr)
        return 2

    if end < start:
        print("--end must be >= --start", file=sys.stderr)
        return 2

    loader = PMXTv2Loader(cache_dir=args.cache_dir)
    print(f"Cache dir: {loader._cache_dir}")
    print(f"Range:     {start.isoformat()} → {end.isoformat()} (inclusive)")

    n_hours = int((end - start).total_seconds() // 3600) + 1
    print(f"Hours:     {n_hours}")
    print()

    cached = downloaded = failed = 0
    cur = start
    while cur <= end:
        path = loader.cache_path(cur)
        already = loader.is_cached(cur) and not args.force
        if already:
            cached += 1
            print(f"  [cache] {path.name}  ({path.stat().st_size // (1024*1024)} MB)")
        else:
            try:
                loader.download_hour(cur, force=args.force)
                downloaded += 1
                size_mb = path.stat().st_size // (1024 * 1024)
                print(f"  [fetch] {path.name}  ({size_mb} MB)")
            except Exception as e:
                failed += 1
                print(f"  [FAIL]  {path.name}  {e}", file=sys.stderr)
        cur += timedelta(hours=1)

    print()
    print(f"Done: {cached} cached, {downloaded} downloaded, {failed} failed")
    print(f"Total cache size: {sum(p.stat().st_size for p in loader._cache_dir.glob('*.parquet')) // (1024*1024)} MB")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
