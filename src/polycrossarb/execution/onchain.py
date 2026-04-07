"""On-chain split/merge execution for Conditional Tokens on Polygon.

Enables hybrid arb execution:
  - OVERPRICED: splitPosition (mint all tokens for 1.00 USDC.e) → sell on CLOB
  - UNDERPRICED: buy on CLOB → mergePositions (redeem all tokens for 1.00 USDC.e)

Contract addresses from py-clob-client config (Polygon mainnet, chain 137):
  - CTFExchange: 0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E
  - NegRisk CTFExchange: 0xC5d563A36AE78145C45a50134d48A1215220f80a
  - ConditionalTokens: 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
  - USDC.e: 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
"""
from __future__ import annotations

import asyncio
import logging
import time
from functools import lru_cache

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from eth_account import Account

from polycrossarb.config import settings

log = logging.getLogger(__name__)

# ── Contract addresses (Polygon mainnet) ──────────────────────────────
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# ── Minimal ABIs (only the functions we need) ─────────────────────────

# Gnosis ConditionalTokens — splitPosition and mergePositions
CONDITIONAL_TOKENS_ABI = [
    {
        "name": "splitPosition",
        "type": "function",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "partition", "type": "uint256[]"},
            {"name": "amount", "type": "uint256"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    },
    {
        "name": "mergePositions",
        "type": "function",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "partition", "type": "uint256[]"},
            {"name": "amount", "type": "uint256"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    },
    {
        "name": "getOutcomeSlotCount",
        "type": "function",
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
    {
        "name": "balanceOf",
        "type": "function",
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
]

# ERC20 — approve and allowance
ERC20_ABI = [
    {
        "name": "approve",
        "type": "function",
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
    },
    {
        "name": "allowance",
        "type": "function",
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
    {
        "name": "balanceOf",
        "type": "function",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
]

# Null parent collection (no parent conditions)
NULL_BYTES32 = b"\x00" * 32


class OnChainExecutor:
    """Executes split/merge operations on Polymarket's Conditional Tokens contracts.

    split_position: Deposit USDC.e → receive all outcome tokens (for overpriced arbs)
    merge_positions: Return all outcome tokens → receive USDC.e (for underpriced arbs)
    """

    def __init__(self, rpc_url: str | None = None, private_key: str | None = None):
        self._rpc_url = rpc_url or settings.polygon_rpc_url
        self._key = private_key or settings.private_key

        if not self._key:
            raise ValueError("Private key required for on-chain execution")

        self._w3 = Web3(Web3.HTTPProvider(self._rpc_url, request_kwargs={"timeout": 15}))
        self._w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        self._account = Account.from_key(self._key)
        self._address = self._account.address

        # Contract instances
        self._ct = self._w3.eth.contract(
            address=Web3.to_checksum_address(CONDITIONAL_TOKENS),
            abi=CONDITIONAL_TOKENS_ABI,
        )
        self._usdc = self._w3.eth.contract(
            address=Web3.to_checksum_address(USDC_E),
            abi=ERC20_ABI,
        )

        # Nonce tracking
        self._nonce: int | None = None

        log.info("OnChainExecutor initialized: wallet=%s rpc=%s",
                 self._address[:10], self._rpc_url[:30])

    @property
    def address(self) -> str:
        return self._address

    def _get_nonce(self) -> int:
        """Get next nonce from chain — always fresh to avoid conflicts.

        Always queries the chain to prevent desync when the wallet is
        used from multiple sources (e.g. manual tx + bot concurrently).
        """
        self._nonce = self._w3.eth.get_transaction_count(self._address, "pending")
        return self._nonce

    def _estimate_gas_price(self) -> dict:
        """Get current gas price for Polygon PoS."""
        try:
            gas_price = self._w3.eth.gas_price
            # Use 1.2x current gas price for faster inclusion
            return {"gasPrice": int(gas_price * 1.2)}
        except Exception:
            # Fallback: 50 gwei
            return {"gasPrice": Web3.to_wei(50, "gwei")}

    def _send_tx(self, tx_func, gas_limit: int = 200_000) -> str:
        """Build, sign, send transaction and wait for receipt.

        Returns transaction hash.
        Raises on revert or timeout.
        """
        gas_params = self._estimate_gas_price()
        nonce = self._get_nonce()

        tx = tx_func.build_transaction({
            "from": self._address,
            "nonce": nonce,
            "gas": gas_limit,
            "chainId": 137,
            **gas_params,
        })

        signed = self._w3.eth.account.sign_transaction(tx, self._key)
        tx_hash = self._w3.eth.send_raw_transaction(signed.raw_transaction)

        log.info("Tx sent: %s (nonce=%d, gas=%d)", tx_hash.hex()[:16], nonce, gas_limit)

        # Wait for receipt (up to 60 seconds)
        receipt = self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

        if receipt["status"] != 1:
            raise RuntimeError(f"Transaction reverted: {tx_hash.hex()}")

        gas_used = receipt["gasUsed"]
        gas_price = receipt.get("effectiveGasPrice", gas_params.get("gasPrice", 0))
        cost_wei = gas_used * gas_price
        cost_usd = (cost_wei / 1e18) * 0.50  # rough POL→USD

        log.info("Tx confirmed: %s (gas=%d, cost≈$%.4f)", tx_hash.hex()[:16], gas_used, cost_usd)

        return tx_hash.hex()

    # ── Public API ─────────────────────────────────────────────────

    def check_usdc_allowance(self, spender: str) -> float:
        """Check USDC.e allowance for a spender. Returns amount in USD."""
        allowance = self._usdc.functions.allowance(
            Web3.to_checksum_address(self._address),
            Web3.to_checksum_address(spender),
        ).call()
        return allowance / 1e6

    def approve_usdc(self, spender: str, amount_usd: float | None = None) -> str:
        """Approve USDC.e spending for a contract.

        Args:
            spender: Contract address to approve.
            amount_usd: Amount in USD. None = unlimited.

        Returns:
            Transaction hash.
        """
        if amount_usd is None:
            amount = 2**256 - 1  # max uint256 (unlimited)
        else:
            amount = int(amount_usd * 1e6)

        log.info("Approving USDC.e: spender=%s amount=%s",
                 spender[:10], "unlimited" if amount_usd is None else f"${amount_usd:.2f}")

        tx_func = self._usdc.functions.approve(
            Web3.to_checksum_address(spender),
            amount,
        )
        return self._send_tx(tx_func, gas_limit=60_000)

    def ensure_usdc_approval(self, spender: str, min_amount_usd: float = 1000.0) -> bool:
        """Ensure sufficient USDC.e allowance, approving if needed.

        Returns True if approval was already sufficient, False if new approval was sent.
        """
        current = self.check_usdc_allowance(spender)
        if current >= min_amount_usd:
            return True

        self.approve_usdc(spender, amount_usd=None)  # unlimited approval
        return False

    def get_outcome_count(self, condition_id: str) -> int:
        """Get the number of outcomes for a condition."""
        cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        return self._ct.functions.getOutcomeSlotCount(cid_bytes).call()

    def get_token_balance(self, token_id: str | int) -> float:
        """Get ERC1155 balance for a conditional token. Returns in shares."""
        if isinstance(token_id, str):
            token_id = int(token_id, 16) if token_id.startswith("0x") else int(token_id)
        balance = self._ct.functions.balanceOf(self._address, token_id).call()
        return balance / 1e6  # USDC.e is 6 decimals

    def split_position(
        self,
        condition_id: str,
        amount_usd: float,
        num_outcomes: int,
    ) -> str:
        """Split collateral into outcome tokens.

        Deposits `amount_usd` USDC.e and receives `amount` of each outcome token.

        Args:
            condition_id: The market condition ID (hex string).
            amount_usd: USDC.e amount to split.
            num_outcomes: Number of outcomes (e.g. 2 for YES/NO).

        Returns:
            Transaction hash.
        """
        amount = int(amount_usd * 1e6)
        cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))

        # Partition: [1, 2, 4, ...] — each bit represents an outcome
        partition = [1 << i for i in range(num_outcomes)]

        # Ensure USDC.e approval for ConditionalTokens contract
        self.ensure_usdc_approval(CONDITIONAL_TOKENS, amount_usd)

        log.info("splitPosition: condition=%s amount=$%.2f outcomes=%d",
                 condition_id[:16], amount_usd, num_outcomes)

        tx_func = self._ct.functions.splitPosition(
            Web3.to_checksum_address(USDC_E),
            NULL_BYTES32,
            cid_bytes,
            partition,
            amount,
        )
        return self._send_tx(tx_func, gas_limit=300_000)

    def merge_positions(
        self,
        condition_id: str,
        amount_usd: float,
        num_outcomes: int,
    ) -> str:
        """Merge outcome tokens back into collateral.

        Returns `amount` of each outcome token and receives `amount_usd` USDC.e.
        Caller must hold `amount` of EACH outcome token.

        Args:
            condition_id: The market condition ID (hex string).
            amount_usd: Amount of each token to merge (in USDC.e units).
            num_outcomes: Number of outcomes.

        Returns:
            Transaction hash.
        """
        amount = int(amount_usd * 1e6)
        cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        partition = [1 << i for i in range(num_outcomes)]

        log.info("mergePositions: condition=%s amount=$%.2f outcomes=%d",
                 condition_id[:16], amount_usd, num_outcomes)

        tx_func = self._ct.functions.mergePositions(
            Web3.to_checksum_address(USDC_E),
            NULL_BYTES32,
            cid_bytes,
            partition,
            amount,
        )
        return self._send_tx(tx_func, gas_limit=300_000)

    def get_usdc_balance(self) -> float:
        """Get wallet USDC.e balance."""
        bal = self._usdc.functions.balanceOf(self._address).call()
        return bal / 1e6

    def estimate_split_cost(self, amount_usd: float) -> dict:
        """Estimate total cost of a split operation."""
        gas_params = self._estimate_gas_price()
        gas_price = gas_params.get("gasPrice", Web3.to_wei(50, "gwei"))
        gas_cost_pol = (300_000 * gas_price) / 1e18
        gas_cost_usd = gas_cost_pol * 0.50  # rough POL→USD

        return {
            "collateral": amount_usd,
            "gas_usd": round(gas_cost_usd, 4),
            "total": round(amount_usd + gas_cost_usd, 4),
        }

    def estimate_merge_revenue(self, amount_usd: float) -> dict:
        """Estimate total revenue from a merge operation."""
        gas_params = self._estimate_gas_price()
        gas_price = gas_params.get("gasPrice", Web3.to_wei(50, "gwei"))
        gas_cost_pol = (300_000 * gas_price) / 1e18
        gas_cost_usd = gas_cost_pol * 0.50

        return {
            "collateral_returned": amount_usd,
            "gas_usd": round(gas_cost_usd, 4),
            "net": round(amount_usd - gas_cost_usd, 4),
        }
