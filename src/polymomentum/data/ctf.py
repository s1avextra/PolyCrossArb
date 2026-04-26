"""Polymarket Conditional Token Framework (CTF) reader.

Reads on-chain market resolution state directly from Polygon, bypassing
Gamma API indexing lag. Authoritative — the CTF contract IS where market
resolution lives once UMA's optimistic oracle settles.

For a binary "Up or Down" candle market with `condition_id`:
  payoutDenominator(cid) == 0           -> not resolved yet
  payoutDenominator(cid) >  0           -> resolved
    payoutNumerators(cid, 0) > num[1]  -> outcome 0 (Up) won
    payoutNumerators(cid, 1) > num[0]  -> outcome 1 (Down) won
    50/50 split                         -> tie (rare, UMA "p1=0.5")

Pattern: raw eth_call via httpx, matching execution/wallet.py — no web3
dependency required.
"""
from __future__ import annotations

import logging

import httpx

from polymomentum.config import settings

log = logging.getLogger(__name__)

# Polymarket uses the standard Gnosis ConditionalTokens deployment on Polygon.
# Verified 2026-04-26: returns sensible payouts for our recently-resolved
# condition_ids (paper run 2026-04-25).
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# 4-byte selectors (keccak256 of the signature)
_PAYOUT_DENOMINATOR = "0xdd34de67"   # payoutDenominator(bytes32)
_PAYOUT_NUMERATORS = "0x0504c814"    # payoutNumerators(bytes32,uint256)


def _eth_call(data: str, rpc_url: str | None = None, timeout: float = 10.0) -> int:
    """Make a raw eth_call to the CTF contract and return the uint256 result."""
    rpc = rpc_url or settings.polygon_rpc_url
    resp = httpx.post(rpc, json={
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": CTF_ADDRESS, "data": data}, "latest"],
        "id": 1,
    }, timeout=timeout)
    resp.raise_for_status()
    body = resp.json()
    if "error" in body:
        raise RuntimeError(f"CTF eth_call error: {body['error']}")
    return int(body.get("result", "0x0"), 16)


def _strip_0x(s: str) -> str:
    return s[2:] if s.startswith("0x") else s


def get_resolution(
    condition_id: str,
    rpc_url: str | None = None,
) -> tuple[str | None, list[int]]:
    """Read the on-chain resolution for a binary candle market.

    Returns:
        (winner, [num0, num1])
        winner is "up", "down", "tie", or None if not yet resolved.
        num0/num1 are the raw payout numerators (typically 0 or 1 for a
        binary resolution; can be fractional for UMA p1=0.5 ties).

    Assumes outcome 0 maps to "Up" (YES) and outcome 1 to "Down" (NO),
    which is how the candle scanner reads markets via Gamma. If a market
    has outcomes in the opposite order, the caller is responsible for
    reversing the result.

    Raises on RPC errors so callers can decide whether to retry.
    """
    cid_hex = _strip_0x(condition_id)
    if len(cid_hex) != 64:
        raise ValueError(f"condition_id must be 32 bytes, got {len(cid_hex)//2}")

    denom = _eth_call(_PAYOUT_DENOMINATOR + cid_hex, rpc_url)
    if denom == 0:
        return None, [0, 0]

    num0 = _eth_call(_PAYOUT_NUMERATORS + cid_hex + "0".zfill(64), rpc_url)
    num1 = _eth_call(_PAYOUT_NUMERATORS + cid_hex + "1".zfill(64), rpc_url)

    if num0 == num1:
        return "tie", [num0, num1]
    return ("up" if num0 > num1 else "down"), [num0, num1]
