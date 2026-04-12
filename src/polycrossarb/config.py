from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

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

    # Max days until resolution — skip events that lock capital too long
    max_resolution_days: float = 3.0

    # Strategy safety limits
    max_legs_per_trade: int = 4        # max outcomes per arb (2-4 realistic)
    min_leg_probability: float = 0.05  # skip outcomes below 5% (illiquid lottery tickets)
    min_leg_bid_depth_usd: float = 50   # each leg must have $50+ bid depth (exitable)
    min_leg_value_usd: float = 5.0     # each leg order must be >= $5 (not dust)
    min_leg_volume_usd: float = 500    # each leg must have $500+ volume (real trading activity)

    # When on-chain merge is disabled, only trade OVERPRICED events.
    # With merge enabled, underpriced arbs are safely exitable via
    # mergePositions (buy all tokens on CLOB → redeem for $1.00 on-chain).
    only_overpriced: bool = True

    # On-chain split/merge execution (requires web3)
    enable_onchain_execution: bool = False  # safety flag — enable after testing

    # Kelly fraction: 0.25 = quarter Kelly (academic optimal for prediction markets)
    # Full Kelly maximises growth but has 50% chance of 50% drawdown.
    # Quarter Kelly: ~94% of growth rate, max drawdown ~12%.
    kelly_fraction: float = 0.25

    # Weather strategy
    min_weather_confidence: float = 0.80
    max_weather_position_pct: float = 0.15

    # Crypto strategy
    min_crypto_edge: float = 0.03
    max_crypto_position_pct: float = 0.10

    # ── Candle strategy zone gates ────────────────────────────────
    # Three-zone entry timing. Tunable from .env without redeploy.
    candle_zone_early_min_confidence: float = 0.55
    candle_zone_early_min_z: float = 2.0
    candle_zone_early_min_edge: float = 0.03
    candle_zone_primary_min_z: float = 1.0
    candle_zone_late_min_confidence: float = 0.65
    candle_zone_late_min_z: float = 0.5
    candle_zone_late_min_edge: float = 0.08
    candle_dead_zone_lo: float = 0.80
    candle_dead_zone_hi: float = 0.90
    candle_min_price: float = 0.10
    candle_max_price: float = 0.90
    candle_edge_cap: float = 0.25
    candle_skip_dead_zone: bool = True

    # ── Candle circuit breaker ────────────────────────────────────
    candle_breaker_min_trades: int = 20
    candle_breaker_min_win_rate: float = 0.65
    candle_breaker_max_drawdown_pct: float = 0.30

    # ── Operational kill switch ───────────────────────────────────
    # Touch this file from any shell to halt trading immediately.
    kill_switch_path: str = "/tmp/polycrossarb/KILL"

    # Hard-fail in live mode if alerter webhook is missing
    alert_required: bool = False

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
