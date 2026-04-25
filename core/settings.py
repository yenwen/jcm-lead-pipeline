from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

from pydantic import field_validator, model_validator
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ---------------------------------------------------------------
    # Core financial / underwriting assumptions
    # All percentage-like values are stored as decimals: 0.15 = 15%
    # ---------------------------------------------------------------
    TARGET_PROFIT_PCT: float = 0.15
    PURCHASE_FACTOR_PROXY: float = 0.75
    DOWN_PAYMENT_PCT: float = 0.20
    REHAB_CASH_EXPOSURE_PCT: float = 0.25
    HOLDING_MONTHS: int = 6
    HARD_MONEY_RATE_MONTHLY: float = 0.01
    ORIGINATION_POINTS_PCT: float = 0.02
    CLOSING_COST_PCT: float = 0.025
    SELLING_COST_PCT: float = 0.06
    CONTINGENCY_PCT: float = 0.10

    # ---------------------------------------------------------------
    # Workflow behavior
    # ---------------------------------------------------------------
    ALLOW_PROXY_APPROVAL: bool = False
    MAX_LEADS_PER_RUN: int = 50
    MAX_PUBLISH_PER_RUN: int = 10
    CROSS_RUN_LOOKBACK_DAYS: int = 30
    RANKING_MAX_ROI_CEILING: float = 0.35

    # ---------------------------------------------------------------
    # Sourcing / filtering defaults
    # ---------------------------------------------------------------
    MIN_PRICE: int = 300_000
    MAX_PRICE: int = 2_500_000
    MIN_SQFT: int = 700
    MAX_SQFT: int = 4_000
    MIN_YEAR_BUILT: int = 1940
    MIN_EQUITY_PCT: float = 0.30
    MIN_OWNERSHIP_YEARS: int = 3
    RECENT_SALE_EXCLUDE_DAYS: int = 365
    LUXURY_PRICE_THRESHOLD: int = 3_000_000

    # ---------------------------------------------------------------
    # Geography
    # ---------------------------------------------------------------
    TARGET_COUNTIES: list[str] = ["Santa Clara", "Alameda"]

    # ---------------------------------------------------------------
    # Hidden scope reserve assumptions (Spec 1.8.2 Section 7.2)
    # ---------------------------------------------------------------
    HIDDEN_SCOPE_RESERVE_PCT_DEFAULT: float = 0.12
    HIDDEN_SCOPE_RESERVE_PCT_PRE1965: float = 0.18
    HIDDEN_SCOPE_RESERVE_PCT_PRE1940: float = 0.22

    # ---------------------------------------------------------------
    # Cache TTLs (days) — Spec 1.8.2 Section 6.1
    # ---------------------------------------------------------------
    CACHE_TTL_IMAGES_DAYS: int = 90
    CACHE_TTL_COMPS_DAYS: int = 7
    CACHE_TTL_PROPERTY_FACTS_DAYS: int = 30
    CACHE_TTL_GEOCODE_DAYS: int = 180

    # ---------------------------------------------------------------
    # Scoring thresholds — Spec 1.8.2 Section 8
    # ---------------------------------------------------------------

    # Node 3: minimum distress score to proceed past enrichment gate
    DISTRESS_SCORE_MIN_THRESHOLD: int = 20

    # Node 5: comp count bands for arv_confidence assignment
    ARV_COMP_COUNT_HIGH: int = 5     # >= this → "high"
    ARV_COMP_COUNT_MEDIUM: int = 3   # >= this → "medium", else "low"

    # Node 7: image count bands for capex_confidence assignment
    VISION_IMAGE_COUNT_HIGH: int = 4    # >= this (+ avg_conf) → "high"
    VISION_IMAGE_COUNT_MEDIUM: int = 2  # >= this → "medium", else "low"

    # Node 7: minimum average observation confidence for capex "high" band
    VISION_AVG_CONFIDENCE_MIN_HIGH: float = 0.75

    # Node 8: Rule-of-70 divergence alert threshold
    MAX_OFFER_DIVERGENCE_ALERT_PCT: float = 0.10

    # Node 8: base execution score and contingency multiplier
    UNDERWRITING_BASE_EXECUTION_SCORE: int = 70
    CONTINGENCY_MULTIPLIER_LOW_CONF: float = 2.0  # applied when capex_confidence == "low"

    # Node 8: ranking weights — must sum to 1.0 (validated below)
    RANK_WEIGHT_ROI: float = 0.35
    RANK_WEIGHT_DISTRESS: float = 0.20
    RANK_WEIGHT_EXECUTION: float = 0.20
    RANK_WEIGHT_DATA_CONFIDENCE: float = 0.25

    # ---------------------------------------------------------------
    # Mock API control — set False in .env to force NotImplementedError
    # for any mock that hasn't been replaced with a real API call
    # ---------------------------------------------------------------
    USE_MOCK_APIS: bool = True

    # ---------------------------------------------------------------
    # Paths / runtime
    # ---------------------------------------------------------------
    DATABASE_PATH: Path = Path(__file__).parent.parent / "jcm_pipeline.db"
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    # ---------------------------------------------------------------
    # External service credentials (obfuscated in logs via SecretStr)
    # None defaults allow fail-fast validation in node logic
    # ---------------------------------------------------------------
    PROPSTREAM_API_KEY: Optional[SecretStr] = None
    ATTOM_API_KEY: Optional[SecretStr] = None
    OPENAI_API_KEY: Optional[SecretStr] = None

    # Airtable uses PATs, not legacy API keys
    AIRTABLE_ACCESS_TOKEN: Optional[SecretStr] = None

    # ---------------------------------------------------------------
    # Optional endpoints / identifiers
    # Required for Node 9; validated at publish time, not at startup
    # ---------------------------------------------------------------
    AIRTABLE_BASE_ID: Optional[str] = None
    AIRTABLE_TABLE_NAME: Optional[str] = None

    OPENAI_MODEL_VISION: str = "gpt-4o"
    OPENAI_MODEL_VISION_FALLBACK: Optional[str] = None  # e.g. "gpt-4o-mini"

    TIMEZONE: str = "America/Los_Angeles"

    # ---------------------------------------------------------------
    # Field validators
    # ---------------------------------------------------------------

    @field_validator(
        "TARGET_PROFIT_PCT",
        "DOWN_PAYMENT_PCT",
        "REHAB_CASH_EXPOSURE_PCT",
        "CLOSING_COST_PCT",
        "SELLING_COST_PCT",
        "CONTINGENCY_PCT",
        "HARD_MONEY_RATE_MONTHLY",
        "ORIGINATION_POINTS_PCT",
        mode="before",
    )
    @classmethod
    def must_be_valid_pct(cls, v: float) -> float:
        if not (0.0 < v < 1.0):
            raise ValueError(f"Percentage field must be between 0 and 1, got {v}")
        return v

    @field_validator("MIN_PRICE", "MIN_SQFT", "HOLDING_MONTHS", mode="before")
    @classmethod
    def must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"Must be positive, got {v}")
        return v

    # ---------------------------------------------------------------
    # Cross-field validators
    # ---------------------------------------------------------------

    @model_validator(mode="after")
    def validate_ranges(self) -> "Settings":
        if self.MIN_PRICE >= self.MAX_PRICE:
            raise ValueError("MIN_PRICE must be less than MAX_PRICE")
        if self.MIN_SQFT >= self.MAX_SQFT:
            raise ValueError("MIN_SQFT must be less than MAX_SQFT")
        if self.LUXURY_PRICE_THRESHOLD <= self.MAX_PRICE:
            raise ValueError("LUXURY_PRICE_THRESHOLD must exceed MAX_PRICE")
        return self

    @model_validator(mode="after")
    def ranking_weights_sum_to_one(self) -> "Settings":
        total = (
            self.RANK_WEIGHT_ROI
            + self.RANK_WEIGHT_DISTRESS
            + self.RANK_WEIGHT_EXECUTION
            + self.RANK_WEIGHT_DATA_CONFIDENCE
        )
        if not abs(total - 1.0) < 1e-6:
            raise ValueError(
                f"Ranking weights must sum to 1.0, got {total:.4f}"
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def override_settings_for_testing(**overrides) -> Settings:
    """
    For test use only. Clears the lru_cache so overrides take effect.
    Always call get_settings.cache_clear() in test teardown.

    Usage:
        settings = override_settings_for_testing(ALLOW_PROXY_APPROVAL=True)
        yield
        get_settings.cache_clear()
    """
    get_settings.cache_clear()
    return Settings(**overrides)