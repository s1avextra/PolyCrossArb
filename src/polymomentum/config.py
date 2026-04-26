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

    # Risk — bankroll_usd=0 means auto-detect from wallet USDC.e balance
    bankroll_usd: float = 0.0
    max_total_exposure_usd: float = 80.0
    max_position_per_market_usd: float = 20.0
    cooldown_seconds: float = 120.0
    min_profit_usd: float = 0.10

    # Kelly fraction: 0.25 = quarter Kelly (academic optimal).
    kelly_fraction: float = 0.25

    # Crypto strategy edge threshold
    min_crypto_edge: float = 0.03
    max_crypto_position_pct: float = 0.10

    # ── Candle strategy zone gates ────────────────────────────────
    # Four-zone entry timing. Tunable from .env without redeploy.
    # Zones: early [0%,40%) | primary [40%,80%) | late [80%,95%) | terminal [95%,100%]
    candle_zone_early_min_confidence: float = 0.55
    candle_zone_early_min_z: float = 2.0
    candle_zone_early_min_edge: float = 0.03
    candle_zone_primary_min_z: float = 1.0
    candle_zone_late_min_confidence: float = 0.65
    candle_zone_late_min_z: float = 0.5
    candle_zone_late_min_edge: float = 0.08
    candle_zone_terminal_min_confidence: float = 0.55
    candle_zone_terminal_min_z: float = 0.3
    candle_zone_terminal_min_edge: float = 0.03
    candle_dead_zone_lo: float = 0.80
    candle_dead_zone_hi: float = 0.90
    candle_min_price: float = 0.10
    candle_max_price: float = 0.90
    candle_edge_cap: float = 0.25
    candle_skip_dead_zone: bool = True
    # Entry-price EV filter — skip if confidence ≤ market_price + buffer.
    # See decision.py:ZoneConfig.min_ev_buffer for the math derivation.
    # Set to a negative value to disable.
    candle_min_ev_buffer: float = 0.05

    # ── Momentum noise filter ────────────────────────────────────
    # z-scores below this threshold get 60% confidence penalty.
    # At IV=47%, BTC=$70k, 5-min window: z=0.3 requires ~$31 move.
    # Lower to 0.15 to allow trades on ~$15 moves in flat markets.
    candle_noise_z_threshold: float = 0.3

    # ── Position sizing ──────────────────────────────────────────
    # Per-trade position as a fraction of effective bankroll. Default
    # 10% sits between quarter and half Kelly for our typical edge
    # profile (60% conf / 50% edge over fair → Kelly ≈ 25%). At 10%,
    # a 5-loss streak ≈ 50% drawdown — survivable. At 20%, the same
    # streak wipes the bankroll. Top Polymarket candle traders (e.g.
    # @stingo43, $388k cumulative profit over 3,852 trades) cluster
    # most positions under $50 even with substantial bankroll —
    # high-volume small-position compounding beats large concentrated
    # bets when edges are small and books are thin.
    candle_position_pct: float = 0.10

    # ── Volatility regime sizing ─────────────────────────────────
    # Vol multipliers stack on top of candle_position_pct. Capped by
    # max_position_per_market_usd ($20 default), so EXTREME vol on a
    # $1k bankroll still tops out at $20/trade, not $40.
    candle_vol_high_multiplier: float = 1.5
    candle_vol_extreme_multiplier: float = 2.0

    # ── Cross-asset lead-lag ──────────────────────────���──────────
    # Use BTC momentum as leading indicator for ETH/SOL contracts.
    candle_cross_asset_enabled: bool = False
    candle_cross_asset_min_correlation: float = 0.70
    candle_cross_asset_confidence_boost: float = 0.10

    # ── Maker fee optimization ────────────────────────────────────
    candle_prefer_maker: bool = False
    candle_maker_timeout_s: float = 3.0

    # ── Candle circuit breaker ────────────────────────────────────
    candle_breaker_min_trades: int = 20
    candle_breaker_min_win_rate: float = 0.65
    candle_breaker_max_drawdown_pct: float = 0.30

    # ── Operational kill switch ───────────────────────────────────
    # Touch this file from any shell to halt trading immediately.
    kill_switch_path: str = "/tmp/polymomentum/KILL"

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

    @field_validator("min_profit_usd", "cooldown_seconds")
    @classmethod
    def must_be_non_negative(cls, v: float, info) -> float:
        if v < 0:
            raise ValueError(f"{info.field_name} must be >= 0, got {v}")
        return v


settings = Settings()
