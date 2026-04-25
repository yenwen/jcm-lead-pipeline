"""
Tests for pure node logic functions (no DB, no API calls).
Covers: ARV confidence bands, capex confidence, execution score,
data confidence score, and timezone utility.
"""

import pytest
from unittest.mock import patch

from core.models import (
    ComparableSale,
    JCMPropertyState,
    VisualObservation,
)
from core.settings import Settings, get_settings, override_settings_for_testing
from core.utils import to_local_dt
from nodes.node_05_arv import ARV_HAIRCUTS
from nodes.node_07_vision import determine_capex_confidence
from nodes.node_08_underwriting import (
    compute_data_confidence_score,
    compute_execution_score,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_state(**kwargs) -> JCMPropertyState:
    defaults = dict(run_id="test-run", property_id="prop-001", pipeline_stage="enriched")
    return JCMPropertyState(**{**defaults, **kwargs})


def _make_obs(confidence: float = 0.8) -> VisualObservation:
    return VisualObservation(
        image_source="https://example.com/img.jpg",
        category="roof",
        condition="poor",
        estimated_quantity=800.0,
        unit="sqft",
        evidence="test evidence",
        confidence=confidence,
    )


# ---------------------------------------------------------------------------
# node_05_arv — ARV confidence bands
# ---------------------------------------------------------------------------

class TestARVConfidenceBands:
    def test_high_confidence_with_five_comps(self):
        settings = get_settings()
        # >= ARV_COMP_COUNT_HIGH → "high"
        count = settings.ARV_COMP_COUNT_HIGH
        assert count >= 5
        haircut = ARV_HAIRCUTS["high"]
        assert haircut == 0.00

    def test_medium_confidence_with_three_comps(self):
        haircut = ARV_HAIRCUTS["medium"]
        assert haircut == 0.05

    def test_low_confidence_haircut(self):
        haircut = ARV_HAIRCUTS["low"]
        assert haircut == 0.10

    def test_arv_haircut_applied_correctly(self):
        median_arv = 800_000.0
        haircut = ARV_HAIRCUTS["medium"]
        risk_adj = round(median_arv * (1.0 - haircut), 2)
        assert risk_adj == 760_000.0

    def test_unknown_confidence_falls_back_to_10pct(self):
        haircut = ARV_HAIRCUTS.get("unknown", 0.10)
        assert haircut == 0.10


# ---------------------------------------------------------------------------
# node_07_vision — capex confidence
# ---------------------------------------------------------------------------

class TestCapexConfidence:
    def test_zero_images_is_low(self):
        assert determine_capex_confidence(0, []) == "low"

    def test_one_image_is_low(self):
        assert determine_capex_confidence(1, [_make_obs()]) == "low"

    def test_two_images_is_medium(self):
        assert determine_capex_confidence(2, [_make_obs(), _make_obs()]) == "medium"

    def test_three_images_is_medium(self):
        obs = [_make_obs(0.9)] * 3
        assert determine_capex_confidence(3, obs) == "medium"

    def test_four_images_high_confidence_is_high(self):
        obs = [_make_obs(0.85)] * 4  # avg=0.85 > VISION_AVG_CONFIDENCE_MIN_HIGH=0.75
        assert determine_capex_confidence(4, obs) == "high"

    def test_four_images_low_confidence_stays_medium(self):
        obs = [_make_obs(0.50)] * 4  # avg=0.50 < 0.75
        assert determine_capex_confidence(4, obs) == "medium"

    def test_four_images_no_observations_stays_medium(self):
        # Empty observations list despite 4 image_urls
        assert determine_capex_confidence(4, []) == "medium"


# ---------------------------------------------------------------------------
# node_08_underwriting — execution score
# ---------------------------------------------------------------------------

class TestExecutionScore:
    def test_sfr_boosts_score(self):
        state = _make_state(property_type="SFR", year_built=1985, building_sqft=1800, beds=3, baths=2)
        score, rationale = compute_execution_score(state)
        settings = get_settings()
        # SFR +10, no deductions → base + 10
        assert score == settings.UNDERWRITING_BASE_EXECUTION_SCORE + 10
        assert "SFR" in rationale

    def test_pre1940_reduces_score(self):
        state = _make_state(property_type="SFR", year_built=1930, building_sqft=1800, beds=3, baths=2)
        score, _ = compute_execution_score(state)
        settings = get_settings()
        # SFR +10, pre-1940 -15 → base - 5
        assert score == settings.UNDERWRITING_BASE_EXECUTION_SCORE - 5

    def test_missing_sqft_reduces_score(self):
        state = _make_state(property_type="SFR", year_built=1990, building_sqft=None, beds=3, baths=2)
        score, _ = compute_execution_score(state)
        settings = get_settings()
        # SFR +10, missing sqft -10 → base
        assert score == settings.UNDERWRITING_BASE_EXECUTION_SCORE

    def test_score_capped_at_100(self):
        state = _make_state(property_type="SFR", year_built=2010, building_sqft=2000, beds=4, baths=3)
        score, _ = compute_execution_score(state)
        assert 0 <= score <= 100

    def test_score_floored_at_0(self):
        state = _make_state(property_type=None, year_built=1920, building_sqft=None, beds=None, baths=None)
        score, _ = compute_execution_score(state)
        assert score >= 0


# ---------------------------------------------------------------------------
# node_08_underwriting — data confidence score
# ---------------------------------------------------------------------------

class TestDataConfidenceScore:
    def test_all_high_confidence_floors_at_80(self):
        state = _make_state(
            arv_confidence="high",
            capex_confidence="high",
            facts_confidence="high",
            image_urls=["url1", "url2", "url3", "url4"],
        )
        score, _ = compute_data_confidence_score(state)
        assert score >= 80

    def test_low_arv_caps_at_50(self):
        state = _make_state(
            arv_confidence="low",
            capex_confidence="medium",
            image_urls=["url1"],
        )
        score, _ = compute_data_confidence_score(state)
        assert score <= 50

    def test_no_images_caps_at_50(self):
        state = _make_state(
            arv_confidence="medium",
            capex_confidence="medium",
            image_urls=[],
        )
        score, _ = compute_data_confidence_score(state)
        assert score <= 50

    def test_score_in_valid_range(self):
        state = _make_state(arv_confidence="medium", capex_confidence="medium", image_urls=["url1"])
        score, _ = compute_data_confidence_score(state)
        assert 0 <= score <= 100


# ---------------------------------------------------------------------------
# core/utils — timezone conversion
# ---------------------------------------------------------------------------

class TestToLocalDt:
    def test_utc_to_pacific_offset(self):
        # 2026-04-23 18:00 UTC → 2026-04-23 11:00 PDT (UTC-7 in summer)
        dt = to_local_dt("2026-04-23T18:00:00+00:00")
        assert dt.hour == 11
        assert dt.tzname() in ("PDT", "PST")

    def test_naive_utc_assumed(self):
        dt = to_local_dt("2026-04-23T18:00:00")
        assert dt.hour == 11

    def test_custom_timezone(self):
        dt = to_local_dt("2026-04-23T18:00:00+00:00", tz_name="America/New_York")
        assert dt.hour == 14  # EDT = UTC-4
