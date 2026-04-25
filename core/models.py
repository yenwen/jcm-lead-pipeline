from __future__ import annotations

from typing import Any, List, Optional, Literal, TYPE_CHECKING
from pydantic import BaseModel, Field, field_validator


# -------------------------------------------------------------------
# Type aliases / enums
# -------------------------------------------------------------------

County = Literal["Santa Clara", "Alameda"]

PropertyType = Literal[
    "SFR",
    "Duplex",
    "Triplex",
    "Fourplex",
    "Townhome",
    "Condo",
]

PipelineStage = Literal[
    "new",
    "sourced",
    "normalized",
    "enriched",
    "underwritten",
    "published",
    "rejected",
    "error",
]

ConfidenceBand = Literal["low", "medium", "high"]

DistressTier = Literal["none", "weak", "moderate", "strong"]


# -------------------------------------------------------------------
# Sub-models
# -------------------------------------------------------------------

class DistressSignal(BaseModel):
    signal_type: Literal[
        "tax_delinquency",
        "preforeclosure",
        "code_violation",
        "probate",
        "days_on_market",
        "vacancy",
        "unknown",
    ]
    source: str
    severity: float = Field(..., ge=0, le=1)
    signal_date: Optional[str] = None
    corroborated: bool = False


class ComparableSale(BaseModel):
    address: str
    distance_miles: float = Field(..., ge=0)
    sold_date: str
    sold_price: float = Field(..., ge=0)
    sqft: Optional[int] = Field(default=None, ge=0)
    beds: Optional[float] = Field(default=None, ge=0)
    baths: Optional[float] = Field(default=None, ge=0)
    property_type: Optional[str] = None
    source: str
    comp_score: Optional[float] = Field(default=None, ge=0, le=100)
    selection_rationale: Optional[str] = None


class VisualObservation(BaseModel):
    image_source: str
    image_capture_date: Optional[str] = None
    category: Literal[
        "roof",
        "paint",
        "windows",
        "landscaping",
        "driveway",
        "fence",
        "garage_door",
        "debris",
        "exterior_unknown",
    ]
    condition: Literal["good", "fair", "poor", "unknown"]
    estimated_quantity: float = Field(..., ge=0)
    unit: Literal["sqft", "unit", "lf", "flat"]
    evidence: str
    confidence: float = Field(..., ge=0, le=1)


class CostLineItem(BaseModel):
    category: str
    oracle_unit_price: float = Field(..., ge=0)
    estimated_quantity: float = Field(..., ge=0)
    unit: Literal["sqft", "unit", "lf", "flat"]
    total_cost: float = Field(..., ge=0)


class AcquisitionDecision(BaseModel):
    recommendation: Literal["approved", "manual_review", "rejected"]
    reason_codes: List[str] = Field(default_factory=list)
    summary: str
    next_action: str


class AuditEntry(BaseModel):
    timestamp: str
    node: str
    field_mutated: Optional[str] = None
    value_before: Optional[Any] = None
    value_after: Optional[Any] = None
    rationale: str


class PipelineError(BaseModel):
    timestamp: str
    node: str
    error_code: str
    message: str
    is_fatal: bool


# -------------------------------------------------------------------
# Node 1 ingestion payload shield
# -------------------------------------------------------------------

class PropStreamPayload(BaseModel):
    apn: str = Field(..., alias="parcel_number")
    raw_address: str = Field(..., alias="property_address")
    county: str = Field(..., alias="county_name")

    asking_price: Optional[float] = Field(default=None, alias="list_price")
    building_sqft: Optional[int] = Field(default=None, alias="sqft")
    year_built: Optional[int] = Field(default=None, alias="year_built")
    beds: Optional[float] = None
    baths: Optional[float] = None

    @field_validator("asking_price", "building_sqft", "year_built", mode="before")
    @classmethod
    def coerce_empty_strings(cls, v: Any) -> Any:
        if v in ("", " ", "0", 0):
            return None
        return v


# -------------------------------------------------------------------
# Canonical property state
# -------------------------------------------------------------------

class JCMPropertyState(BaseModel):
    spec_version: Literal["1.8.1"] = "1.8.1"

    run_id: str
    property_id: str
    pipeline_stage: PipelineStage

    # Identity / geo
    county: Optional[County] = None
    city: Optional[str] = None
    zip_code: Optional[str] = None
    property_type: Optional[PropertyType] = None

    raw_address: Optional[str] = None
    normalized_address: Optional[str] = None
    apn: Optional[str] = None

    latitude: Optional[float] = None
    longitude: Optional[float] = None
    geocode_confidence: Optional[ConfidenceBand] = None

    # Pricing / listing
    asking_price: Optional[float] = Field(default=None, ge=0)
    estimated_purchase_price: Optional[float] = Field(default=None, ge=0)
    purchase_price_method: Optional[Literal["asking_price", "proxy_arv_factor"]] = None

    listing_status: Optional[Literal["active", "pending", "sold", "off_market", "unknown"]] = None
    days_on_market: Optional[int] = Field(default=None, ge=0)

    # Ownership / equity
    owner_occupancy_status: Optional[Literal["owner_occupied", "absentee", "unknown"]] = None
    estimated_equity_pct: Optional[float] = Field(default=None, ge=-1.0, le=1)
    last_sale_date: Optional[str] = None
    last_sale_price: Optional[float] = Field(default=None, ge=0)

    # Distress / ranking
    distress_signals: List[DistressSignal] = Field(default_factory=list)
    distress_score: Optional[float] = Field(default=None, ge=0, le=100)
    distress_tier: Optional[DistressTier] = None

    execution_score: Optional[float] = Field(default=None, ge=0, le=100)
    data_confidence_score: Optional[float] = Field(default=None, ge=0, le=100)
    final_rank_score: Optional[float] = Field(default=None, ge=0, le=100)

    # Images / rehab
    image_urls: List[str] = Field(default_factory=list)
    image_capture_dates: List[str] = Field(default_factory=list)
    visual_observations: List[VisualObservation] = Field(default_factory=list)

    visible_scope_capex_base: Optional[float] = Field(default=None, ge=0)
    hidden_scope_reserve: Optional[float] = Field(default=None, ge=0)
    total_rehab_estimate: Optional[float] = Field(default=None, ge=0)
    capex_confidence: Optional[ConfidenceBand] = None
    rehab_line_items: List[CostLineItem] = Field(default_factory=list)

    # Property facts
    building_sqft: Optional[int] = Field(default=None, ge=0)
    lot_sqft: Optional[int] = Field(default=None, ge=0)
    year_built: Optional[int] = Field(default=None, ge=1800, le=2100)
    beds: Optional[float] = Field(default=None, ge=0)
    baths: Optional[float] = Field(default=None, ge=0)
    facts_confidence: Optional[ConfidenceBand] = None

    # ARV / financials
    estimated_arv_base: Optional[float] = Field(default=None, ge=0)
    risk_adjusted_arv: Optional[float] = Field(default=None, ge=0)
    arv_confidence: Optional[ConfidenceBand] = None
    comps_used: List[ComparableSale] = Field(default_factory=list)

    risk_adjusted_contingency: Optional[float] = Field(default=None, ge=0)
    total_project_cost: Optional[float] = Field(default=None, ge=0)
    total_invested_cash: Optional[float] = Field(default=None, ge=0)
    expected_profit: Optional[float] = None
    expected_roi: Optional[float] = None
    max_offer_price: Optional[float] = None
    rule_of_70_max: Optional[float] = None
    max_offer_divergence_pct: Optional[float] = Field(default=None, ge=0)

    # Output / audit
    decision: Optional[AcquisitionDecision] = None
    crm_payload_id: Optional[str] = None
    skip_publish_reason: Optional[str] = None

    data_sources: List[str] = Field(default_factory=list)
    manual_review_reasons: List[str] = Field(default_factory=list)

    # Source confidence — computed at ingestion time
    source_confidence: Optional[Any] = Field(
        default=None,
        description="SourceConfidence report: reliability, completeness, freshness, and composite score",
    )

    audit_log: List[AuditEntry] = Field(default_factory=list)
    errors: List[PipelineError] = Field(default_factory=list)

    @field_validator("zip_code", mode="before")
    @classmethod
    def normalize_zip_code(cls, v: Any) -> Any:
        if v is None:
            return v
        s = str(v).strip()
        if not s:
            return None
        return s[:10]

    @field_validator("image_urls", "image_capture_dates", "data_sources", "manual_review_reasons", mode="before")
    @classmethod
    def none_to_empty_list(cls, v: Any) -> Any:
        return [] if v is None else v
