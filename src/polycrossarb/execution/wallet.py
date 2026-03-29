"""Wallet balance reader for Polygon USDC.e and native USDC.

Queries on-chain balances to auto-detect bankroll at startup.
Uses raw RPC calls via httpx — no web3 dependency required.
"""
from __future__ import annotations

import logging

import httpx
from eth_account import Account

from polycrossarb.config import settings

log = logging.getLogger(__name__)

# Polygon token contracts
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # bridged (Polymarket uses this)
USDC_NATIVE = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # native

# balanceOf(address) selector
_BALANCE_OF = "0x70a08231"


def _rpc_call(to: str, data: str, rpc_url: str) -> int:
    """Make a raw eth_call and return the result as int."""
    resp = httpx.post(rpc_url, json={
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": to, "data": data}, "latest"],
        "id": 1,
    }, timeout=10.0)
    resp.raise_for_status()
    result = resp.json().get("result", "0x0")
    return int(result, 16)


def get_wallet_address() -> str | None:
    """Derive wallet address from private key in config."""
    if not settings.private_key:
        return None
    try:
        return Account.from_key(settings.private_key).address
    except Exception:
        return None


def get_wallet_balances(rpc_url: str | None = None) -> dict[str, float]:
    """Query on-chain USDC balances for the configured wallet.

    Returns:
        {
            "address": "0x...",
            "usdc_e": 98.92,        # USDC.e (what Polymarket uses)
            "usdc_native": 0.0,     # native USDC
            "total_usdc": 98.92,
            "pol": 10.51,
        }
    """
    address = get_wallet_address()
    if not address:
        return {"address": "", "usdc_e": 0, "usdc_native": 0, "total_usdc": 0, "pol": 0}

    rpc = rpc_url or settings.polygon_rpc_url
    call_data = _BALANCE_OF + address[2:].lower().zfill(64)

    result: dict[str, float] = {"address": address}

    try:
        # USDC.e (6 decimals)
        usdc_e = _rpc_call(USDC_E, call_data, rpc) / 1e6
        result["usdc_e"] = round(usdc_e, 2)
    except Exception:
        result["usdc_e"] = 0.0

    try:
        # Native USDC (6 decimals)
        usdc_native = _rpc_call(USDC_NATIVE, call_data, rpc) / 1e6
        result["usdc_native"] = round(usdc_native, 2)
    except Exception:
        result["usdc_native"] = 0.0

    result["total_usdc"] = round(result["usdc_e"] + result["usdc_native"], 2)

    try:
        # POL balance (18 decimals)
        resp = httpx.post(rpc, json={
            "jsonrpc": "2.0",
            "method": "eth_getBalance",
            "params": [address, "latest"],
            "id": 2,
        }, timeout=10.0)
        resp.raise_for_status()
        pol_wei = int(resp.json().get("result", "0x0"), 16)
        result["pol"] = round(pol_wei / 1e18, 4)
    except Exception:
        result["pol"] = 0.0

    return result


def detect_bankroll() -> float:
    """Auto-detect bankroll from wallet USDC.e balance.

    Returns the USDC.e balance, or falls back to config if wallet
    can't be read. Logs what it found.
    """
    balances = get_wallet_balances()

    if balances["usdc_e"] > 0:
        log.info(
            "Wallet %s: USDC.e=$%.2f, native=$%.2f, POL=%.4f",
            balances["address"][:10], balances["usdc_e"],
            balances["usdc_native"], balances["pol"],
        )
        return balances["usdc_e"]

    if balances["usdc_native"] > 0:
        log.warning(
            "Wallet has $%.2f native USDC but $0 USDC.e. "
            "Polymarket needs USDC.e — swap or deposit via polymarket.com",
            balances["usdc_native"],
        )

    if balances["total_usdc"] == 0 and balances["address"]:
        log.warning("Wallet %s has no USDC — using config bankroll", balances["address"][:10])

    return settings.bankroll_usd
