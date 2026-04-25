# core/source_confidence.py
"""
Source Confidence Engine — per-lead trust scoring at ingestion time.

Computes a SourceConfidence report for every lead immediately after mapping,
BEFORE it enters the pipeline. This gives users the ability to quickly assess
whether a lead is worth acting on, even before underwriting runs.

Dimensions:
  1. Source Reliability   — how trustworthy is the adapter that produced this lead?
  2. Data Completeness    — what fraction of critical fields are populated?
  3. Missing Fields       — explicit list of what's absent
  4. Freshness            — is the data stale or current?
  5. Confidence Score     — 0-100 composite of all dimensions

The SourceConfidence sub-model is attached to JCMPropertyState at ingestion.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Source reliability tiers — each adapter gets a base trust score
# ---------------------------------------------------------------------------
# Rationale:
#   County tax collector = government record, highest reliability.
#   Bid4Assets = public auction platform, high reliability.
#   Zillow = MLS-adjacent data, good but not authoritative.
#   Apify FB = user-generated marketplace posts, moderate.
#   Craigslist = unstructured classified ads, lowest.

SOURCE_RELIABILITY: dict[str, float] = {
    "adapter_firecrawl":   0.90,   # County tax collector — official gov data
    "adapter_bid4assets":  0.85,   # Public auction platform
    "adapter_zillow":      0.75,   # MLS-adjacent, structured but scraped
    "adapter_apify_fb":    0.55,   # Facebook Marketplace — user-generated
    "adapter_craigslist":  0.45,   # Craigslist — unstructured classifieds
}

# Default for unknown sources
DEFAULT_SOURCE_RELIABILITY = 0.50

# ---------------------------------------------------------------------------
# Critical fields — the must-haves for a lead to be actionable
# ---------------------------------------------------------------------------
# Weighted by importance. Weight determines how much a missing field
# penalizes the completeness score.

CRITICAL_FIELDS: dict[str, float] = {
    "raw_address":      3.0,   # Can't do anything without an address
    "county":           2.5,   # Required for geo targeting
    "asking_price":     2.0,   # Need price to underwrite
    "property_type":    1.5,   # Affects comp selection and rehab scope
    "beds":             1.0,   # Key comp filter
    "building_sqft":    1.0,   # Key comp filter
    "year_built":       0.5,   # Affects hidden scope reserve
    "baths":            0.5,   # Secondary comp filter
    "apn":              1.5,   # Uniqueness + enrichment key
    "city":             1.0,   # Geo precision
    "lot_sqft":         0.5,   # Nice to have
}

# Bonus fields — not critical but improve confidence
BONUS_FIELDS: dict[str, float] = {
    "distress_signals":      1.0,   # Has distress evidence
    "image_urls":            0.5,   # Has visual data
    "normalized_address":    0.5,   # Passed geocoding
    "latitude":              0.3,   # Has coordinates
}


# ---------------------------------------------------------------------------
# Freshness thresholds
# ---------------------------------------------------------------------------
FRESH_HOURS = 24          # < 24h old = "fresh"
RECENT_HOURS = 72         # 24-72h = "recent"
# > 72h = "stale"


# ---------------------------------------------------------------------------
# SourceConfidence model
# ---------------------------------------------------------------------------

class SourceConfidence(BaseModel):
    """
    Attached to each JCMPropertyState at ingestion time.
    Gives users an immediate quality signal without waiting for underwriting.
    """
    # Source identity
    source_adapter: str = ""
    source_reliability: float = Field(0.0, ge=0, le=1)
    source_reliability_label: str = ""  # "high", "medium", "low"

    # Completeness
    completeness_score: float = Field(0.0, ge=0, le=1)
    fields_present: int = 0
    fields_total: int = 0
    missing_fields: list[str] = Field(default_factory=list)

    # Freshness
    freshness: str = "unknown"       # "fresh", "recent", "stale", "unknown"
    data_age_hours: Optional[float] = None

    # Composite
    confidence_score: int = Field(0, ge=0, le=100)
    confidence_label: str = ""       # "high", "medium", "low", "very_low"
    confidence_breakdown: str = ""   # human-readable explanation


# ---------------------------------------------------------------------------
# Computation
# ---------------------------------------------------------------------------

def _identify_source(data_sources: list[str]) -> str:
    """Extract the adapter name from data_sources list."""
    for src in data_sources:
        if src.startswith("adapter_"):
            return src
    return "unknown"


def _compute_completeness(state_dict: dict) -> tuple[float, int, int, list[str]]:
    """
    Compute weighted completeness score and list of missing critical fields.

    Returns: (score 0-1, fields_present, fields_total, missing_field_names)
    """
    total_weight = sum(CRITICAL_FIELDS.values())
    earned_weight = 0.0
    present = 0
    missing: list[str] = []

    for field_name, weight in CRITICAL_FIELDS.items():
        value = state_dict.get(field_name)
        if _is_populated(field_name, value):
            earned_weight += weight
            present += 1
        else:
            missing.append(field_name)

    # Bonus fields add up to 10% extra (don't penalize if missing)
    bonus_total = sum(BONUS_FIELDS.values())
    bonus_earned = 0.0
    for field_name, weight in BONUS_FIELDS.items():
        value = state_dict.get(field_name)
        if _is_populated(field_name, value):
            bonus_earned += weight

    base_score = earned_weight / total_weight if total_weight else 0.0
    bonus_pct = (bonus_earned / bonus_total * 0.10) if bonus_total else 0.0

    score = min(1.0, base_score + bonus_pct)
    return score, present, len(CRITICAL_FIELDS), missing


def _is_populated(field_name: str, value) -> bool:
    """Check if a field has a meaningful value."""
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    if isinstance(value, list) and len(value) == 0:
        return False
    return True


def _compute_freshness(audit_log: list) -> tuple[str, Optional[float]]:
    """
    Determine data freshness from the earliest audit entry timestamp.

    Returns: (freshness_label, age_in_hours)
    """
    if not audit_log:
        return "unknown", None

    try:
        # Find the ingestion timestamp (first audit entry)
        timestamps = []
        for entry in audit_log:
            ts = entry.get("timestamp") if isinstance(entry, dict) else getattr(entry, "timestamp", None)
            if ts:
                timestamps.append(ts)

        if not timestamps:
            return "unknown", None

        # Parse the earliest timestamp
        earliest = min(timestamps)
        if isinstance(earliest, str):
            # Handle both +00:00 and Z suffixes
            earliest = earliest.replace("Z", "+00:00")
            if "+" not in earliest and "-" not in earliest[10:]:
                earliest += "+00:00"
            dt = datetime.fromisoformat(earliest)
        else:
            return "unknown", None

        now = datetime.now(UTC)
        age = now - dt
        age_hours = age.total_seconds() / 3600

        if age_hours < FRESH_HOURS:
            return "fresh", round(age_hours, 1)
        elif age_hours < RECENT_HOURS:
            return "recent", round(age_hours, 1)
        else:
            return "stale", round(age_hours, 1)

    except Exception:
        return "unknown", None


def _reliability_label(score: float) -> str:
    if score >= 0.80:
        return "high"
    elif score >= 0.60:
        return "medium"
    else:
        return "low"


def _confidence_label(score: int) -> str:
    if score >= 80:
        return "high"
    elif score >= 60:
        return "medium"
    elif score >= 40:
        return "low"
    else:
        return "very_low"


def compute_source_confidence(state) -> SourceConfidence:
    """
    Compute a SourceConfidence report for a JCMPropertyState.

    Accepts either a JCMPropertyState instance or a dict.
    Called at ingestion time by every adapter.

    Composite score formula (0-100):
        40% source reliability
        45% data completeness
        15% freshness
    """
    # Accept both Pydantic model and dict
    if hasattr(state, "model_dump"):
        state_dict = state.model_dump()
    elif isinstance(state, dict):
        state_dict = state
    else:
        return SourceConfidence(confidence_label="unknown")

    # 1. Source reliability
    data_sources = state_dict.get("data_sources", [])
    adapter_name = _identify_source(data_sources)
    reliability = SOURCE_RELIABILITY.get(adapter_name, DEFAULT_SOURCE_RELIABILITY)
    reliability_label = _reliability_label(reliability)

    # 2. Completeness
    completeness, present, total, missing = _compute_completeness(state_dict)

    # 3. Freshness
    audit_log = state_dict.get("audit_log", [])
    freshness, age_hours = _compute_freshness(audit_log)

    freshness_score = {
        "fresh":   1.0,
        "recent":  0.7,
        "stale":   0.3,
        "unknown": 0.5,
    }.get(freshness, 0.5)

    # 4. Composite score
    raw_score = (
        reliability * 40
        + completeness * 45
        + freshness_score * 15
    )
    confidence = max(0, min(100, round(raw_score)))

    # Build breakdown string
    parts = [
        f"source={adapter_name}({reliability:.0%})",
        f"complete={completeness:.0%}({present}/{total})",
        f"freshness={freshness}",
    ]
    if missing:
        parts.append(f"missing=[{', '.join(missing[:5])}{'...' if len(missing) > 5 else ''}]")

    breakdown = " | ".join(parts)
    label = _confidence_label(confidence)

    return SourceConfidence(
        source_adapter=adapter_name,
        source_reliability=round(reliability, 2),
        source_reliability_label=reliability_label,
        completeness_score=round(completeness, 3),
        fields_present=present,
        fields_total=total,
        missing_fields=missing,
        freshness=freshness,
        data_age_hours=age_hours,
        confidence_score=confidence,
        confidence_label=label,
        confidence_breakdown=breakdown,
    )
