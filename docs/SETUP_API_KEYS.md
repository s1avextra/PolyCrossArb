# Getting Your Polymarket API Keys

This guide walks you through getting the credentials needed for live trading.
**Paper trading and scanning require NO keys** — skip this until you're ready to trade real money.

---

## What you need

| Credential | What it is | Used for |
|-----------|------------|----------|
| `PRIVATE_KEY` | Your Polygon wallet's private key | Signing orders + deriving API keys |
| `POLY_API_KEY` | UUID identifying your API access | Authenticating CLOB API requests |
| `POLY_API_SECRET` | Base64-encoded HMAC signing key | Signing authenticated requests |
| `POLY_API_PASSPHRASE` | Random string | Additional auth factor |

The API key, secret, and passphrase are **derived from your private key** using Polymarket's SDK. You don't create them on a website — you run a script.

---

## Step 1: Create a dedicated trading wallet

**Do NOT use your main wallet.** Create a fresh one just for this bot.

### Option A: MetaMask (easiest)

1. Open MetaMask → click your account icon → **Create Account**
2. Name it something like "PolyMomentum Trading"
3. Switch to the **Polygon network**:
   - Network name: `Polygon Mainnet`
   - RPC URL: `https://polygon-rpc.com`
   - Chain ID: `137`
   - Currency: `POL`
   - Explorer: `https://polygonscan.com`
4. Export the private key:
   - Click the three dots next to the account → **Account Details** → **Show Private Key**
   - Enter your MetaMask password
   - Copy the key (starts with `0x...`)

### Option B: Generate a new wallet with Python

```bash
.venv/bin/python -c "
from eth_account import Account
acct = Account.create()
print(f'Address:     {acct.address}')
print(f'Private key: {acct.key.hex()}')
print()
print('Save both of these. The private key cannot be recovered.')
"
```

> Requires `eth-account` — install with: `uv pip install eth-account`

---

## Step 2: Fund the wallet with USDC on Polygon

Your wallet needs **USDC.e on the Polygon network** (this is what Polymarket uses for trading).

### How to fund

1. **Buy USDC on an exchange** (Coinbase, Binance, Kraken, etc.)
2. **Withdraw to your wallet address** on the **Polygon network**
   - Make sure you select **Polygon** as the withdrawal network, NOT Ethereum
   - Send to the wallet address from Step 1
3. **You also need a tiny amount of POL** for gas fees (~0.1 POL is enough)
   - Most exchanges let you withdraw POL directly to Polygon
   - Or use a faucet / bridge

### Verify your balance

```bash
.venv/bin/python -c "
from web3 import Web3
w3 = Web3(Web3.HTTPProvider('https://polygon-rpc.com'))
address = 'YOUR_WALLET_ADDRESS_HERE'

# POL balance
pol = w3.eth.get_balance(address)
print(f'POL: {w3.from_wei(pol, \"ether\"):.4f}')

# USDC.e balance
usdc_contract = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'
abi = [{\"constant\":True,\"inputs\":[{\"name\":\"account\",\"type\":\"address\"}],\"name\":\"balanceOf\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"type\":\"function\"}]
usdc = w3.eth.contract(address=Web3.to_checksum_address(usdc_contract), abi=abi)
bal = usdc.functions.balanceOf(Web3.to_checksum_address(address)).call()
print(f'USDC.e: {bal / 1e6:.2f}')
"
```

---

## Step 3: Derive your API credentials

Polymarket API keys are **derived from your private key** using their SDK. Run this script once to generate them:

```bash
# Install the Polymarket client
uv pip install py-clob-client

# Generate credentials
.venv/bin/python -c "
from py_clob_client.client import ClobClient

HOST = 'https://clob.polymarket.com'
CHAIN_ID = 137
PRIVATE_KEY = 'YOUR_PRIVATE_KEY_HERE'  # from Step 1

# Create a temporary client and derive API credentials
client = ClobClient(HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID)
creds = client.create_or_derive_api_creds()

print('Add these to your .env file:')
print()
print(f'POLY_API_KEY={creds.api_key}')
print(f'POLY_API_SECRET={creds.api_secret}')
print(f'POLY_API_PASSPHRASE={creds.api_passphrase}')
"
```

This calls `create_or_derive_api_creds()` which either creates new credentials or retrieves existing ones tied to your wallet. The credentials are deterministic — running it again with the same private key returns the same credentials.

---

## Step 4: Set token allowances

Before your first live trade, you must approve Polymarket's exchange contracts to spend your USDC and outcome tokens. This is a one-time setup per wallet.

```bash
.venv/bin/python -c "
from py_clob_client.client import ClobClient

HOST = 'https://clob.polymarket.com'
CHAIN_ID = 137
PRIVATE_KEY = 'YOUR_PRIVATE_KEY_HERE'
FUNDER = 'YOUR_WALLET_ADDRESS_HERE'

client = ClobClient(
    HOST,
    key=PRIVATE_KEY,
    chain_id=CHAIN_ID,
    signature_type=0,  # 0 = EOA (MetaMask/hardware wallet)
    funder=FUNDER,
)
client.set_api_creds(client.create_or_derive_api_creds())

# Approve token allowances (one-time)
client.approve_all()
print('Token allowances set successfully.')
"
```

### Signature type reference

| Value | Wallet type | When to use |
|-------|------------|-------------|
| `0` | EOA (MetaMask, hardware wallet, raw private key) | You exported a private key from MetaMask or generated one yourself |
| `1` | Email / Magic wallet | You log into Polymarket with email |
| `2` | Browser wallet proxy | You use Polymarket's browser-based wallet |

If you followed Step 1, use **signature_type=0**.

---

## Step 5: Fill in your .env file

```env
# Wallet
PRIVATE_KEY=0xabc123...your_private_key_here
POLYGON_RPC_URL=https://polygon-rpc.com

# API credentials (from Step 3)
POLY_API_KEY=550e8400-e29b-41d4-a716-446655440000
POLY_API_SECRET=base64EncodedSecretString==
POLY_API_PASSPHRASE=randomPassphraseString

# Capital settings for $100 bankroll
BANKROLL_USD=100.0
MAX_TOTAL_EXPOSURE_USD=80.0
MAX_POSITION_PER_MARKET_USD=20.0
MIN_ARB_MARGIN=0.02
MIN_PROFIT_USD=0.10
SCAN_INTERVAL_SECONDS=30
COOLDOWN_SECONDS=120
```

---

## Security checklist

- [ ] Using a **dedicated wallet** (not your main wallet)
- [ ] `.env` is in `.gitignore` (it is by default)
- [ ] Private key is **never** committed to git
- [ ] Started with a **small amount** ($100 or less) for validation
- [ ] Ran **paper trading** first to verify the bot works

---

## Troubleshooting

**"insufficient funds for gas"**
→ You need POL on Polygon for gas. Send ~0.1 POL to your wallet.

**"insufficient USDC balance"**
→ Make sure you sent USDC.e **on Polygon**, not on Ethereum mainnet. Check your balance on [polygonscan.com](https://polygonscan.com).

**"API key invalid" or "unauthorized"**
→ Re-run Step 3 to regenerate credentials. Make sure the private key in `.env` matches the one you used to derive the API keys.

**"allowance too low"**
→ Re-run Step 4 to approve token allowances.

**"nonce too low"**
→ Another transaction is pending. Wait for it to confirm, or increase the gas price.
