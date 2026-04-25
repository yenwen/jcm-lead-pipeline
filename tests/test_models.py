import pytest
from pydantic import ValidationError
from core.models import (
    JCMPropertyState,
    DistressSignal,
    AuditEntry,
    PropStreamPayload
)

def test_valid_jcm_property_state():
    state = JCMPropertyState(
        run_id="run_001",
        property_id="prop_001",
        pipeline_stage="new"
    )
    assert state.spec_version == "1.8.1"
    assert state.run_id == "run_001"
    assert state.pipeline_stage == "new"
    assert state.distress_signals == []
    assert state.comps_used == []

def test_invalid_score_rejection():
    with pytest.raises(ValidationError):
        JCMPropertyState(
            run_id="run_001",
            property_id="prop_001",
            pipeline_stage="new",
            distress_score=150.0  # > 100
        )

def test_invalid_severity_rejection():
    with pytest.raises(ValidationError):
        DistressSignal(
            signal_type="tax_delinquency",
            source="test",
            severity=1.5,  # > 1.0
            corroborated=False
        )

def test_propstream_payload_ignores_extra():
    payload = PropStreamPayload(
        parcel_number="123-456",
        property_address="123 Main St",
        county_name="Santa Clara",
        some_random_field="value"
    )
    assert payload.apn == "123-456"

def test_audit_entry_accepts_non_string():
    entry = AuditEntry(
        timestamp="2026-04-19T00:00:00Z",
        node="TestNode",
        field_mutated="estimated_arv_base",
        value_before=100000,
        value_after={"complex": "object"},
        rationale="Testing"
    )
    assert entry.value_before == 100000
    assert entry.value_after == {"complex": "object"}

def test_default_factory_list_fields_independent():
    state1 = JCMPropertyState(run_id="1", property_id="1", pipeline_stage="new")
    state2 = JCMPropertyState(run_id="2", property_id="2", pipeline_stage="new")
    
    state1.manual_review_reasons.append("TEST_REASON")
    
    assert "TEST_REASON" in state1.manual_review_reasons
    assert "TEST_REASON" not in state2.manual_review_reasons
