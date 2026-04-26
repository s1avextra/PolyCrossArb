# Getting Your Polymarket API Keys

This is a **one-time** setup needed only for **live mode**. Paper mode and the `scan`/`wallet`/`ctf`/`validate-replay` subcommands work without any of these.

## What you need

| Credential | What it is | Used for |
|-----------|------------|----------|
| `PRIVATE_KEY` | Your Polygon wallet's private key | Signing orders + (optionally) auto-detecting bankroll |
| `POLY_API_KEY` | UUID identifying your API access | Authenticating CLOB POSTs |
| `POLY_API_SECRET` | Base64 HMAC signing key | Signing CLOB request headers |
| `POLY_API_PASSPHRASE` | Random string | Additional auth factor |

The API key, secret, and passphrase are **derived from your private key**. Polymarket's CLOB exposes a `POST /auth/api-key` endpoint that returns deterministic creds tied to a wallet — running the derivation twice yields the same values.

## Step 1 — Create a dedicated trading wallet

**Do NOT use your main wallet.** Spin up a fresh one (MetaMask → "Create Account", or any wallet generator). Export the private key — it should start with `0x` and be 32 bytes / 64 hex chars.

## Step 2 — Fund with USDC.e + a little POL

Polymarket settles in **USDC.e** (the bridged variant) on Polygon. Withdraw USDC to your new wallet on the Polygon network, plus ~0.1 POL for gas.

Verify with the bot:

```bash
PRIVATE_KEY=0x...your_key... \
POLYGON_RPC_URL=https://polygon-rpc.com \
./target/release/polymomentum-engine wallet
```

## Step 3 — Derive API credentials

Polymarket's CLOB derivation is documented in their official SDK (Python `py-clob-client` or TypeScript `@polymarket/clob-client`). Run their `create_or_derive_api_creds` once with your private key — it's a single one-time HTTP call. You'll get back the three values to paste into `.env`:

```
POLY_API_KEY=<uuid>
POLY_API_SECRET=<base64>
POLY_API_PASSPHRASE=<string>
```

Easiest path: clone the official Polymarket TypeScript example, plug in your `PRIVATE_KEY`, run, copy the printed creds. The bot itself doesn't need to derive — it just uses what you paste in.

## Step 4 — Set token allowances

Before your first live trade you must approve Polymarket's exchange contracts to spend your USDC.e and outcome tokens. Polymarket's UI does this automatically the first time you place a manual order at `polymarket.com`. Alternatively, you can call `approve` on:

- USDC.e: `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174`
- CTF Exchange: `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E`
- Neg Risk Exchange: `0xC5d563A36AE78145C45a50134d48A1215220f80a`
- CTF Adapter: `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296`

with `cast send` (Foundry) or any Polygon explorer "Write Contract" UI.

## Step 5 — Drop everything into `.env`

Paste the values into `/etc/polymomentum/env` (or a local `.env` for dev runs):

```
PRIVATE_KEY=0x...
POLY_API_KEY=...
POLY_API_SECRET=...
POLY_API_PASSPHRASE=...
POLYGON_RPC_URL=https://polygon-rpc.com
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
ALERT_REQUIRED=1
```

Verify the bot can read your wallet and the on-chain view of an old market:

```bash
./target/release/polymomentum-engine wallet
./target/release/polymomentum-engine ctf 0x<some_resolved_condition_id>
```

## Step 6 — First live trade

When you're ready (paper validated, $1-sized stake), flip the systemd unit:

```bash
ssh vps 'sudo sed -i "s/--mode paper/--mode live --i-understand-live/" \
            /etc/systemd/system/polymomentum-engine.service && \
         sudo systemctl daemon-reload && sudo systemctl restart polymomentum-engine'
```

Watch the next trade tape:

```bash
ssh vps 'journalctl -u polymomentum-engine -f -n 0 | grep candle.trade.live'
```

Roll back to paper by reversing the `sed` and restarting.
