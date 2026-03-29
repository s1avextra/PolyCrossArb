from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    # Polymarket API
    poly_api_key: str = ""
    poly_api_secret: str = ""
    poly_api_passphrase: str = ""
    poly_base_url: str = "https://clob.polymarket.com"
    poly_gamma_url: str = "https://gamma-api.polymarket.com"

    # Execution
    private_key: str = ""
    polygon_rpc_url: str = "https://polygon-rpc.com"

    # Arb detection
    min_arb_margin: float = 0.02
    min_profit_usd: float = 0.10
    max_position_usd: float = 20.0
    scan_interval_seconds: float = 30.0

    # Risk — conservative defaults for $100 bankroll
    bankroll_usd: float = 100.0
    max_total_exposure_usd: float = 80.0
    max_position_per_market_usd: float = 20.0
    cooldown_seconds: float = 120.0

    # Kelly fraction: 0.25 = quarter Kelly (academic optimal for prediction markets)
    # Full Kelly maximises growth but has 50% chance of 50% drawdown.
    # Quarter Kelly: ~94% of growth rate, max drawdown ~12%.
    kelly_fraction: float = 0.25


settings = Settings()
