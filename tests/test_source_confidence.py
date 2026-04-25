# tests/test_source_confidence.py
"""
Tests for the source confidence engine.

Covers:
  - Source reliability scoring per adapter
  - Data completeness calculation
  - Missing field detection
  - Freshness computation (fresh / recent / stale / unknown)
  - Composite confidence score
  - Integration with adapters (via persist_lead)
"""

import pytest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from core.source_confidence import (
    SourceConfidence,
    compute_source_confidence,
    _compute_completeness,
    _compute_freshness,
    _identify_source,
    SOURCE_RELIABILITY,
    CRITICAL_FIELDS,
)
from core.models import AuditEntry, JCMPropertyState


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_state(**overrides) -> JCMPropertyState:
    """Create a minimal test state with overrideable fields."""
    now_iso = datetime.now(UTC).isoformat()
    defaults = dict(
        run_id="test_run_001",
        property_id="test_prop_001",
        pipeline_stage="sourced",
        county="Santa Clara",
        raw_address="123 Test St, San Jose, CA 95112",
        data_sources=["adapter_craigslist"],
        audit_log=[
            AuditEntry(
                timestamp=now_iso,
                node="test",
                field_mutated="pipeline_stage",
                value_before=None,
                value_after="sourced",
                rationale="test ingestion",
            )
        ],
    )
    defaults.update(overrides)
    return JCMPropertyState(**defaults)


# ---------------------------------------------------------------------------
# 1. Source reliability
# ---------------------------------------------------------------------------

class TestSourceReliability:

    def test_firecrawl_is_highest(self):
        state = _make_state(data_sources=["adapter_firecrawl"])
        sc = compute_source_confidence(state)
        assert sc.source_adapter == "adapter_firecrawl"
        assert sc.source_reliability >= 0.85
        assert sc.source_reliability_label == "high"

    def test_craigslist_is_lowest(self):
        state = _make_state(data_sources=["adapter_craigslist"])
        sc = compute_source_confidence(state)
        assert sc.source_adapter == "adapter_craigslist"
        assert sc.source_reliability <= 0.50
        assert sc.source_reliability_label == "low"

    def test_unknown_source_gets_default(self):
        state = _make_state(data_sources=["some_random_source"])
        sc = compute_source_confidence(state)
        assert sc.source_adapter == "unknown"
        assert sc.source_reliability == 0.50

    def test_bid4assets_medium_to_high(self):
        state = _make_state(data_sources=["adapter_bid4assets"])
        sc = compute_source_confidence(state)
        assert sc.source_reliability >= 0.80


# ---------------------------------------------------------------------------
# 2. Data completeness
# ---------------------------------------------------------------------------

class TestDataCompleteness:

    def test_full_record_high_completeness(self):
        state = _make_state(
            asking_price=500000,
            property_type="SFR",
            beds=3,
            baths=2,
            building_sqft=1500,
            year_built=1990,
            apn="123-45-678",
            city="San Jose",
            lot_sqft=5000,
        )
        sc = compute_source_confidence(state)
        assert sc.completeness_score >= 0.90
        assert len(sc.missing_fields) == 0

    def test_minimal_record_low_completeness(self):
        state = _make_state()
        sc = compute_source_confidence(state)
        # Only raw_address and county are populated
        assert sc.completeness_score < 0.60
        assert len(sc.missing_fields) > 3

    def test_missing_fields_listed(self):
        state = _make_state()
        sc = compute_source_confidence(state)
        assert "asking_price" in sc.missing_fields
        assert "beds" in sc.missing_fields
        assert "building_sqft" in sc.missing_fields
        # raw_address IS populated, so should NOT be missing
        assert "raw_address" not in sc.missing_fields

    def test_empty_string_is_missing(self):
        state = _make_state(raw_address="", city="")
        sc = compute_source_confidence(state)
        assert "raw_address" in sc.missing_fields
        assert "city" in sc.missing_fields


# ---------------------------------------------------------------------------
# 3. Missing fields
# ---------------------------------------------------------------------------

class TestMissingFields:

    def test_counts_match(self):
        state = _make_state(
            asking_price=100000,
            property_type="SFR",
            beds=3,
        )
        sc = compute_source_confidence(state)
        expected_present = sum(
            1 for f in CRITICAL_FIELDS
            if getattr(state, f, None) is not None
            and (not isinstance(getattr(state, f), str) or getattr(state, f).strip())
        )
        assert sc.fields_present == expected_present
        assert sc.fields_present + len(sc.missing_fields) == sc.fields_total


# ---------------------------------------------------------------------------
# 4. Freshness
# ---------------------------------------------------------------------------

class TestFreshness:

    def test_fresh_within_24h(self):
        now = datetime.now(UTC).isoformat()
        state = _make_state(
            audit_log=[AuditEntry(
                timestamp=now, node="test", rationale="test",
                field_mutated="pipeline_stage", value_before=None, value_after="sourced",
            )]
        )
        sc = compute_source_confidence(state)
        assert sc.freshness == "fresh"
        assert sc.data_age_hours is not None
        assert sc.data_age_hours < 1.0

    def test_stale_after_72h(self):
        old = (datetime.now(UTC) - timedelta(hours=100)).isoformat()
        state = _make_state(
            audit_log=[AuditEntry(
                timestamp=old, node="test", rationale="test",
                field_mutated="pipeline_stage", value_before=None, value_after="sourced",
            )]
        )
        sc = compute_source_confidence(state)
        assert sc.freshness == "stale"
        assert sc.data_age_hours >= 72

    def test_recent_between_24_72h(self):
        mid = (datetime.now(UTC) - timedelta(hours=48)).isoformat()
        state = _make_state(
            audit_log=[AuditEntry(
                timestamp=mid, node="test", rationale="test",
                field_mutated="pipeline_stage", value_before=None, value_after="sourced",
            )]
        )
        sc = compute_source_confidence(state)
        assert sc.freshness == "recent"

    def test_no_audit_log_unknown(self):
        state = _make_state(audit_log=[])
        sc = compute_source_confidence(state)
        assert sc.freshness == "unknown"


# ---------------------------------------------------------------------------
# 5. Composite confidence score
# ---------------------------------------------------------------------------

class TestCompositeScore:

    def test_score_range(self):
        state = _make_state()
        sc = compute_source_confidence(state)
        assert 0 <= sc.confidence_score <= 100

    def test_full_firecrawl_record_is_high(self):
        """A complete county record should score high."""
        state = _make_state(
            data_sources=["adapter_firecrawl"],
            asking_price=500000,
            property_type="SFR",
            beds=3,
            baths=2,
            building_sqft=1500,
            year_built=1990,
            apn="123-45-678",
            city="San Jose",
            lot_sqft=5000,
        )
        sc = compute_source_confidence(state)
        assert sc.confidence_score >= 75
        assert sc.confidence_label in ("high", "medium")

    def test_sparse_craigslist_is_low(self):
        """A sparse Craigslist lead should score low."""
        state = _make_state(
            data_sources=["adapter_craigslist"],
            raw_address="cheap house for sale",
        )
        sc = compute_source_confidence(state)
        assert sc.confidence_score <= 55
        assert sc.confidence_label in ("low", "very_low")

    def test_breakdown_string_populated(self):
        state = _make_state()
        sc = compute_source_confidence(state)
        assert "source=" in sc.confidence_breakdown
        assert "complete=" in sc.confidence_breakdown
        assert "freshness=" in sc.confidence_breakdown

    def test_confidence_label_matches_score(self):
        state = _make_state(
            data_sources=["adapter_firecrawl"],
            asking_price=500000,
            property_type="SFR",
            beds=3,
            baths=2,
            building_sqft=1500,
            year_built=1990,
            apn="264-16-027",
            city="San Jose",
            lot_sqft=6000,
        )
        sc = compute_source_confidence(state)
        if sc.confidence_score >= 80:
            assert sc.confidence_label == "high"
        elif sc.confidence_score >= 60:
            assert sc.confidence_label == "medium"
        elif sc.confidence_score >= 40:
            assert sc.confidence_label == "low"
        else:
            assert sc.confidence_label == "very_low"


# ---------------------------------------------------------------------------
# 6. Integration: persist_lead attaches confidence
# ---------------------------------------------------------------------------

class TestIntegration:

    def test_persist_attaches_confidence(self, tmp_path, monkeypatch):
        """persist_lead should attach source_confidence to the state."""
        from adapters.common import persist_lead

        db_file = tmp_path / "test.db"
        monkeypatch.setenv("DATABASE_PATH", str(db_file))
        from core.settings import get_settings
        get_settings.cache_clear()

        state = _make_state(
            data_sources=["adapter_zillow"],
            asking_price=750000,
            property_type="SFR",
        )

        assert state.source_confidence is None
        persist_lead(state, dry_run=True)

        # source_confidence should now be populated
        assert state.source_confidence is not None
        assert isinstance(state.source_confidence, dict)
        assert "confidence_score" in state.source_confidence
        assert "missing_fields" in state.source_confidence
        assert state.source_confidence["source_adapter"] == "adapter_zillow"

        get_settings.cache_clear()
