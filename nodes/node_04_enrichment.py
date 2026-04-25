"""Node 4 — Property Fact Enrichment."""

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional

from core.database import (
    get_cache,
    get_leads_by_stage,
    make_expires_at,
    set_cache,
    upsert_lead,
)
from core.models import AuditEntry, JCMPropertyState, PipelineError
from core.settings import get_settings
from core.utils import retry_with_backoff

logger = logging.getLogger(__name__)
NODE_NAME = "node_04_enrichment"

# Strict allowlist of fields this node is allowed to modify (Spec 1.8.2)
ENRICHABLE_FIELDS = frozenset({
    "building_sqft", "lot_sqft", "year_built", "beds", "baths", 
    "listing_status", "days_on_market", "owner_occupancy_status", 
    "estimated_equity_pct", "last_sale_date", "last_sale_price"
})

def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()

@retry_with_backoff(max_retries=3)
def fetch_property_facts(apn: str, county: str) -> dict[str, Any]:
    """Mock function representing an ATTOM Data or PropStream API call."""
    return {
        "building_sqft": 1850,
        "beds": 3,
        "baths": 2,
        "year_built": 1965,
        "owner_occupancy_status": "absentee",
        "estimated_equity_pct": 0.45,
    }

def fetch_facts_with_cache(
    apn: str, 
    county: str,
    db_path: Optional[str | Path] = None
) -> tuple[Optional[dict[str, Any]], bool]:
    """Wraps API call with TTL cache per Spec 1.8.2 Section 6.1."""
    settings = get_settings()
    cache_key = f"property_facts:{apn}"

    cached = get_cache(cache_key, db_path=db_path)
    if cached is not None:
        return cached, True

    # Note: County is required by the API
    result = fetch_property_facts(apn, county) 
    if result is not None:
        set_cache(
            cache_key, 
            result, 
            make_expires_at(settings.CACHE_TTL_PROPERTY_FACTS_DAYS), 
            db_path=db_path
        )

    return result, False

def _fail_lead(
    state: JCMPropertyState,
    audit_entries: list[AuditEntry],
    error_code: str,
    message: str,
    review_reason: str,
    rationale: str,
) -> None:
    """Shared helper to cleanly fail a lead and upsert."""
    prior_stage = state.pipeline_stage
    now = utc_now_iso()

    err = PipelineError(
        timestamp=now,
        node=NODE_NAME,
        error_code=error_code,
        message=message,
        is_fatal=False,
    )
    state.errors.append(err)
    
    if review_reason not in state.manual_review_reasons:
        state.manual_review_reasons.append(review_reason)
        
    state.pipeline_stage = "error"
    state.audit_log.extend(audit_entries)
    state.audit_log.append(
        AuditEntry(
            timestamp=now,
            node=NODE_NAME,
            field_mutated="pipeline_stage",
            value_before=prior_stage,
            value_after="error",
            rationale=rationale,
        )
    )

def run_enrichment(
    run_id: str,
    db_path: Optional[str | Path] = None,
) -> dict[str, int]:
    """
    Node 4 — Property Fact Enrichment.
    Reads:  pipeline_stage = 'normalized'
    Writes: pipeline_stage = 'enriched' (Success) or 'error' (Failure)
    """
    leads = get_leads_by_stage(run_id, "normalized", db_path)

    metrics: dict[str, int] = {
        "processed": 0,
        "enriched": 0,
        "skipped": 0,
        "errors": 0,
        "cache_hits": 0,
    }

    for state in leads:
        # Idempotency Guard
        if NODE_NAME in state.data_sources:
            metrics["skipped"] += 1
            continue

        # Sequential Guard: Must have passed Node 3 scoring
        # (Node 3 keeps passing leads at 'normalized' stage and adds itself to data_sources)
        if "node_03_distress" not in state.data_sources and state.pipeline_stage != "normalized":
            metrics["skipped"] += 1
            continue

        metrics["processed"] += 1
        now_iso = utc_now_iso()
        audit_entries: list[AuditEntry] = []

        try:
            if not state.apn or not state.county:
                _fail_lead(
                    state, audit_entries,
                    error_code="MISSING_REQUIRED_INPUT",
                    message="Lead is missing APN or County required for enrichment API call.",
                    review_reason="MISSING_APN_OR_COUNTY",
                    rationale="Cannot fetch facts without APN and County"
                )
                upsert_lead(state, db_path)
                metrics["errors"] += 1
                continue

            # 1. Fetch Data with Cache (Threading db_path)
            facts, was_cached = fetch_facts_with_cache(state.apn, state.county, db_path)
            
            if was_cached:
                metrics["cache_hits"] += 1

            # 2. Merge Data against ENRICHABLE_FIELDS allowlist
            if facts:
                for key, val in facts.items():
                    if key not in ENRICHABLE_FIELDS:
                        continue
                    
                    current_val = getattr(state, key)
                    if current_val is None and val is not None:
                        setattr(state, key, val)
                        audit_entries.append(AuditEntry(
                            timestamp=now_iso,
                            node=NODE_NAME,
                            field_mutated=key,
                            value_before=None,
                            value_after=val,
                            rationale="Enriched via Property Facts API"
                        ))

            # 3. Calculate Confidence Band
            has_core_facts = all(
                getattr(state, key) is not None 
                for key in ["beds", "baths", "building_sqft"]
            )
            has_year_built = getattr(state, "year_built") is not None
            
            if has_core_facts and has_year_built:
                confidence = "high"
            elif has_core_facts:
                confidence = "medium"
            else:
                confidence = "low"
            
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="facts_confidence",
                value_before=state.facts_confidence,
                value_after=confidence,
                rationale=f"Core facts complete: {has_core_facts}, Year built present: {has_year_built}"
            ))
            state.facts_confidence = confidence

            # 4. Advance Stage
            prior_stage = state.pipeline_stage
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="pipeline_stage",
                value_before=prior_stage,
                value_after="enriched",
                rationale="Property enrichment complete"
            ))
            
            state.pipeline_stage = "enriched"
            state.data_sources.append(NODE_NAME)
            state.audit_log.extend(audit_entries)
            
            upsert_lead(state, db_path)
            metrics["enriched"] += 1

            logger.info("Node 4: Enriched property %s (APN: %s)", state.property_id, state.apn)

        except Exception as exc:
            metrics["errors"] += 1
            
            _fail_lead(
                state, audit_entries,
                error_code="PROPERTY_ENRICHMENT_FAILURE",
                message=f"Exception during enrichment for property_id={state.property_id}: {exc}",
                review_reason="PROPERTY_ENRICHMENT_FAILURE",
                rationale=f"Unexpected error: {exc}"
            )
            upsert_lead(state, db_path)
            logger.exception("Node 4 Error: %s", exc, extra={"property_id": state.property_id})

    logger.info("Node 4 complete. Metrics: %s", metrics)
    return metrics

if __name__ == "__main__":
    from core.database import init_db
    logging.basicConfig(level=logging.INFO)
    init_db()
    print(run_enrichment("manual-enrichment-check"))
