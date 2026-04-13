"""Hyperliquid API client for cross-platform monitoring and (future) trading.

HIP-4 outcome contracts are currently TESTNET ONLY.
This module monitors for mainnet launch and tracks available markets.

When HIP-4 goes mainnet, cross-arb with Polymarket becomes viable:
  - Hyperliquid: 70ms blocks (AWS Tokyo), FIFO matching, 200K orders/sec
  - Polymarket: ~2s blocks (Polygon), off-chain CLOB
  - Latency mismatch = Hyperliquid leads price discovery on crypto events

Access: No KYC, wallet-based, 180+ countries (US/Ontario/Russia blocked).
Fees: taker 0.045%, maker 0.015% (far cheaper than Polymarket's 1.8% crypto taker).

API: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
SDKs: hyperliquid-python-sdk, hyperliquid-rust-sdk

Usage:
    client = HyperliquidClient()
    markets = await client.fetch_outcome_markets()
    if markets:
        print("HIP-4 mainnet is LIVE!")
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)

# Hyperliquid API endpoints
HL_MAINNET = "https://api.hyperliquid.xyz"
HL_TESTNET = "https://api.hyperliquid-testnet.xyz"
HL_WS_MAINNET = "wss://api.hyperliquid.xyz/ws"


@dataclass
class HLOutcomeMarket:
    """A Hyperliquid outcome (prediction) market."""
    name: str           # e.g. "BTC > 85000 by Apr 10"
    asset_id: int
    yes_price: float
    no_price: float
    volume: float
    status: str         # "active", "settled"
    settle_time: str


@dataclass
class HLPerpPrice:
    """Hyperliquid perp price for hedging."""
    asset: str          # "BTC", "ETH", etc
    mark_price: float
    funding_rate: float
    open_interest: float


class HyperliquidClient:
    """Monitors Hyperliquid for HIP-4 outcome markets.

    Checks both mainnet and testnet. When HIP-4 launches on mainnet,
    this client enables cross-arb detection with Polymarket.
    """

    def __init__(self, use_testnet: bool = False):
        base = HL_TESTNET if use_testnet else HL_MAINNET
        self._base = base
        self._client = httpx.AsyncClient(
            base_url=base,
            timeout=10,
            headers={"Content-Type": "application/json"},
        )
        self._hip4_live = False
        self._last_check: float = 0

    async def check_hip4_mainnet(self) -> bool:
        """Check if HIP-4 outcome trading is live on mainnet.

        Returns True if outcome markets are available.
        Cache result for 5 minutes to avoid spam.
        """
        if time.time() - self._last_check < 300:
            return self._hip4_live

        self._last_check = time.time()

        try:
            # The info endpoint returns all market metadata
            resp = await self._client.post(
                "/info",
                json={"type": "metaAndAssetCtxs"},
            )
            resp.raise_for_status()
            data = resp.json()

            # Check if any assets have outcome-type metadata
            # HIP-4 assets will have a different structure than perps
            if isinstance(data, list) and len(data) >= 2:
                asset_contexts = data[1] if len(data) > 1 else []
                for ctx in asset_contexts:
                    if isinstance(ctx, dict) and ctx.get("outcomeType"):
                        self._hip4_live = True
                        log.info("HIP-4 outcome trading detected on mainnet!")
                        return True

            self._hip4_live = False
            return False
        except Exception:
            return False

    async def fetch_outcome_markets(self) -> list[HLOutcomeMarket]:
        """Fetch available outcome markets (testnet or mainnet)."""
        try:
            resp = await self._client.post(
                "/info",
                json={"type": "metaAndAssetCtxs"},
            )
            resp.raise_for_status()
            data = resp.json()

            markets = []
            if isinstance(data, list) and len(data) >= 2:
                meta = data[0] if isinstance(data[0], dict) else {}
                universe = meta.get("universe", [])
                contexts = data[1] if len(data) > 1 else []

                for i, asset in enumerate(universe):
                    name = asset.get("name", "")
                    # Filter for outcome-type assets
                    if i < len(contexts):
                        ctx = contexts[i]
                        if isinstance(ctx, dict) and ctx.get("outcomeType"):
                            markets.append(HLOutcomeMarket(
                                name=name,
                                asset_id=i,
                                yes_price=float(ctx.get("markPx", 0)),
                                no_price=1.0 - float(ctx.get("markPx", 0)),
                                volume=float(ctx.get("dayNtlVlm", 0)),
                                status="active",
                                settle_time="",
                            ))

            return markets
        except Exception as e:
            log.debug("HIP-4 fetch failed: %s", str(e)[:60])
            return []

    async def fetch_perp_prices(self, assets: list[str] | None = None) -> list[HLPerpPrice]:
        """Fetch perp prices for hedging.

        Even before HIP-4 launches, perps can be used to hedge
        Polymarket crypto positions (BTC, ETH, SOL).
        """
        try:
            resp = await self._client.post(
                "/info",
                json={"type": "metaAndAssetCtxs"},
            )
            resp.raise_for_status()
            data = resp.json()

            prices = []
            if isinstance(data, list) and len(data) >= 2:
                meta = data[0] if isinstance(data[0], dict) else {}
                universe = meta.get("universe", [])
                contexts = data[1] if len(data) > 1 else []

                for i, asset in enumerate(universe):
                    name = asset.get("name", "")
                    if assets and name not in assets:
                        continue
                    if i < len(contexts):
                        ctx = contexts[i]
                        if isinstance(ctx, dict):
                            prices.append(HLPerpPrice(
                                asset=name,
                                mark_price=float(ctx.get("markPx", 0)),
                                funding_rate=float(ctx.get("funding", 0)),
                                open_interest=float(ctx.get("openInterest", 0)),
                            ))

            return prices
        except Exception:
            return []

    async def close(self):
        await self._client.aclose()
