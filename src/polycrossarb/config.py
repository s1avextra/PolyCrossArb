from __future__ import annotations

from pydantic import field_validator
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

    # Risk — bankroll_usd=0 means auto-detect from wallet USDC.e balance
    bankroll_usd: float = 0.0
    max_total_exposure_usd: float = 80.0
    max_position_per_market_usd: float = 20.0
    cooldown_seconds: float = 120.0

    # Kelly fraction: 0.25 = quarter Kelly (academic optimal for prediction markets)
    # Full Kelly maximises growth but has 50% chance of 50% drawdown.
    # Quarter Kelly: ~94% of growth rate, max drawdown ~12%.
    kelly_fraction: float = 0.25

    @field_validator("max_total_exposure_usd", "max_position_per_market_usd")
    @classmethod
    def must_be_positive(cls, v: float, info) -> float:
        if v <= 0:
            raise ValueError(f"{info.field_name} must be > 0, got {v}")
        return v

    @field_validator("bankroll_usd")
    @classmethod
    def bankroll_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"bankroll_usd must be >= 0 (0 = auto-detect from wallet), got {v}")
        return v

    @field_validator("kelly_fraction")
    @classmethod
    def kelly_range(cls, v: float) -> float:
        if not 0 < v <= 1.0:
            raise ValueError(f"kelly_fraction must be in (0, 1], got {v}")
        return v

    @field_validator("min_arb_margin", "min_profit_usd", "cooldown_seconds", "scan_interval_seconds")
    @classmethod
    def must_be_non_negative(cls, v: float, info) -> float:
        if v < 0:
            raise ValueError(f"{info.field_name} must be >= 0, got {v}")
        return v


settings = Settings()
