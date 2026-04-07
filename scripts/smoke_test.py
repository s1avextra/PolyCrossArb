#!/usr/bin/env python3
"""Integration smoke test — validates Polymarket APIs match our models.

Run daily (or in CI) to catch API changes before they break live trading.
Exits 0 on success, 1 on failure.

Usage:
  uv run python scripts/smoke_test.py
"""
from __future__ import annotations

import asyncio
import sys
import time

sys.path.insert(0, "src")


async def main() -> int:
    failures: list[str] = []
    t0 = time.time()

    # ── 1. Gamma API: market metadata ─────────────────────────
    print("1. Testing Gamma API (market metadata)...", end=" ", flush=True)
    try:
        import httpx
        from polycrossarb.config import settings

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{settings.poly_gamma_url}/markets",
                params={"limit": 5, "active": "true"},
                timeout=15,
            )
            resp.raise_for_status()
            markets_raw = resp.json()

            if not isinstance(markets_raw, list) or len(markets_raw) == 0:
                failures.append("Gamma: empty or non-list response")
                print("FAIL")
            else:
                m = markets_raw[0]
                required_fields = ["condition_id", "question", "tokens", "active"]
                missing = [f for f in required_fields if f not in m]
                if missing:
                    failures.append(f"Gamma: missing fields {missing} in market object")
                    print("FAIL")
                else:
                    print(f"OK ({len(markets_raw)} markets)")
    except Exception as e:
        failures.append(f"Gamma: {e}")
        print(f"FAIL ({e})")

    # ── 2. CLOB API: order book ───────────────────────────────
    print("2. Testing CLOB API (order book)...", end=" ", flush=True)
    try:
        # Get a token_id from the Gamma response
        token_id = None
        for m in markets_raw:
            tokens = m.get("tokens", [])
            if isinstance(tokens, list) and tokens:
                tid = tokens[0].get("token_id", "")
                if tid:
                    token_id = tid
                    break

        if not token_id:
            failures.append("CLOB: no token_id found from Gamma markets")
            print("FAIL (no token)")
        else:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{settings.poly_base_url}/book",
                    params={"token_id": token_id},
                    timeout=15,
                )
                resp.raise_for_status()
                book = resp.json()

                required_fields = ["bids", "asks"]
                missing = [f for f in required_fields if f not in book]
                if missing:
                    failures.append(f"CLOB book: missing fields {missing}")
                    print("FAIL")
                else:
                    n_bids = len(book.get("bids", []))
                    n_asks = len(book.get("asks", []))
                    print(f"OK ({n_bids} bids, {n_asks} asks)")
    except Exception as e:
        failures.append(f"CLOB book: {e}")
        print(f"FAIL ({e})")

    # ── 3. CLOB API: prices ───────────────────────────────────
    print("3. Testing CLOB API (prices)...", end=" ", flush=True)
    try:
        if token_id:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{settings.poly_base_url}/price",
                    params={"token_id": token_id, "side": "buy"},
                    timeout=15,
                )
                resp.raise_for_status()
                price_data = resp.json()
                if "price" not in price_data:
                    failures.append(f"CLOB price: missing 'price' field, got keys {list(price_data.keys())}")
                    print("FAIL")
                else:
                    print(f"OK (price={price_data['price']})")
        else:
            print("SKIP (no token)")
    except Exception as e:
        failures.append(f"CLOB price: {e}")
        print(f"FAIL ({e})")

    # ── 4. Model parsing ─────────────────────────────────────
    print("4. Testing model parsing...", end=" ", flush=True)
    try:
        from polycrossarb.data.client import PolymarketClient

        client = PolymarketClient()
        parsed = await client.fetch_all_active_markets(min_liquidity=100, limit=10)
        await client.close()

        if len(parsed) == 0:
            failures.append("Model parsing: no markets parsed")
            print("FAIL")
        else:
            # Validate key fields
            m = parsed[0]
            assert m.condition_id, "condition_id empty"
            assert m.question, "question empty"
            assert len(m.outcomes) > 0, "no outcomes"
            assert m.outcomes[0].name, "outcome name empty"
            print(f"OK ({len(parsed)} markets parsed, first: {m.question[:40]}...)")
    except Exception as e:
        failures.append(f"Model parsing: {e}")
        print(f"FAIL ({e})")

    # ── Summary ───────────────────────────────────────────────
    elapsed = time.time() - t0
    print(f"\nCompleted in {elapsed:.1f}s")

    if failures:
        print(f"\n{len(failures)} FAILURE(S):")
        for f in failures:
            print(f"  - {f}")
        return 1
    else:
        print("All checks passed.")
        return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
