#!/usr/bin/env python3
"""Derive Polymarket API credentials from your wallet private key.

Usage:
  python scripts/derive_api_keys.py

Reads PRIVATE_KEY from .env or prompts you to enter it.
Outputs the API credentials to add to your .env file.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, "src")


def main():
    # Try to load from .env
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    private_key = os.getenv("PRIVATE_KEY", "").strip()

    if not private_key:
        print("PRIVATE_KEY not found in .env")
        print("Enter your Polygon wallet private key (starts with 0x):")
        private_key = input("> ").strip()

    if not private_key:
        print("No private key provided. Exiting.")
        sys.exit(1)

    try:
        from py_clob_client.client import ClobClient
    except ImportError:
        print("py-clob-client not installed. Run:")
        print("  uv pip install py-clob-client")
        sys.exit(1)

    print("\nDeriving API credentials from your wallet...")
    print("(This signs an EIP-712 message with your key — no funds are moved)\n")

    try:
        client = ClobClient(
            host="https://clob.polymarket.com",
            key=private_key,
            chain_id=137,
        )
        creds = client.create_or_derive_api_creds()
    except Exception as e:
        print(f"Failed to derive credentials: {e}")
        print("\nMake sure your private key is correct and you have internet access.")
        sys.exit(1)

    api_key = creds.api_key
    api_secret = creds.api_secret
    api_passphrase = creds.api_passphrase

    print("Success! Add these lines to your .env file:\n")
    print(f"POLY_API_KEY={api_key}")
    print(f"POLY_API_SECRET={api_secret}")
    print(f"POLY_API_PASSPHRASE={api_passphrase}")
    print()

    # Offer to write to .env
    if os.path.exists(".env"):
        print("Would you like to append these to your .env file? [y/N]")
        answer = input("> ").strip().lower()
        if answer == "y":
            with open(".env", "a") as f:
                f.write(f"\n# Polymarket API credentials (auto-generated)\n")
                f.write(f"POLY_API_KEY={api_key}\n")
                f.write(f"POLY_API_SECRET={api_secret}\n")
                f.write(f"POLY_API_PASSPHRASE={api_passphrase}\n")
            print("Written to .env")
    else:
        print("No .env file found. Create one with: cp .env.example .env")


if __name__ == "__main__":
    main()
