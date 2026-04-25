# nodes/node_05_arv.py
"""
Node 5 — ARV Estimation (Comparable Sales)

Responsibilities per SPEC.md Section 7, Node 5:
  - Fetch all leads at pipeline_stage='enriched' for the current run.
  - Query comps API (mock for Sprint 1) with TTL cache.
  - Calculate base ARV as median of comparable sold prices.
  - Apply risk-adjusted haircut based on arv_confidence.
  - Assign arv_confidence band based on comp count.
  - Flag insufficient comps for manual review.
  - Do NOT change pipeline_stage — leave at 'enriched'.
  - On failure: attach PipelineError, set pipeline_stage='error', persist, continue.
"""

import logging
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional

from core.database import (
    get_cache,
    get_leads_by_stage,
    make_expires_at,
    set_cache,
    upsert_lead,
)
from core.models import AuditEntry, ComparableSale, JCMPropertyState, PipelineError
from core.settings import get_settings
from core.utils import retry_with_backoff

logger = logging.getLogger(__name__)
NODE_NAME = "node_05_arv"

# ARV risk-adjustment haircuts by confidence band (Spec 1.8.2 Section 8.2)
ARV_HAIRCUTS: dict[str, float] = {
    "high":   0.00,  # 0% haircut
    "medium": 0.05,  # 5% haircut
    "low":    0.10,  # 10% haircut
}


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@retry_with_backoff(max_retries=3)
def fetch_comps(apn: str, county: str) -> list[ComparableSale]:
    """
    Mock function representing an ATTOM / PropStream comps API call.
    Returns 4 comparable sales.
    Sprint 1: assumes API returns pre-filtered Tier 1 comps.
    TODO: Replace with real ATTOM /sale/comparables endpoint in v1.1.
    """
    if not get_settings().USE_MOCK_APIS:
        raise NotImplementedError(
            "Real comps API not implemented. Set USE_MOCK_APIS=true or implement fetch_comps() with ATTOM."
        )
    return [
        ComparableSale(
            address="123 Alpha St",
            distance_miles=0.2,
            sold_date="2025-10-01",
            sold_price=810_000.0,
            sqft=1800,
            source="ATTOM",
        ),
        ComparableSale(
            address="456 Beta Ave",
            distance_miles=0.4,
            sold_date="2025-11-15",
            sold_price=790_000.0,
            sqft=1820,
            source="ATTOM",
        ),
        ComparableSale(
            address="789 Gamma Rd",
            distance_miles=0.3,
            sold_date="2025-12-10",
            sold_price=805_000.0,
            sqft=1790,
            source="ATTOM",
        ),
        ComparableSale(
            address="101 Delta Ln",
            distance_miles=0.5,
            sold_date="2026-01-05",
            sold_price=820_000.0,
            sqft=1850,
            source="ATTOM",
        ),
    ]


def fetch_comps_with_cache(
    apn: str,
    county: str,
    db_path: Optional[str | Path] = None,
) -> tuple[list[ComparableSale], bool]:
    """
    Wraps comps API call with TTL cache per Spec 1.8.2 Section 6.1.
    Cache key includes county to prevent cross-county APN collisions.
    """
    settings = get_settings()
    cache_key = f"comps:{apn}:{county}"

    cached = get_cache(cache_key, db_path=db_path)
    if cached is not None:
        comps = [ComparableSale.model_validate(c) for c in cached]
        return comps, True

    comps = fetch_comps(apn, county)
    if comps:
        set_cache(
            cache_key,
            [c.model_dump() for c in comps],
            make_expires_at(settings.CACHE_TTL_COMPS_DAYS),
            db_path=db_path,
        )

    return comps, False


def _fail_lead(
    state: JCMPropertyState,
    audit_entries: list[AuditEntry],
    error_code: str,
    message: str,
    review_reason: str,
    rationale: str,
    now_iso: str,
    db_path: Optional[str | Path] = None,
) -> None:
    """
    Shared helper: append PipelineError, add to manual_review_reasons,
    set pipeline_stage to 'error', flush audit entries, upsert.
    now_iso passed from caller to ensure timestamp consistency.
    """
    prior_stage = state.pipeline_stage

    state.errors.append(PipelineError(
        timestamp=now_iso,
        node=NODE_NAME,
        error_code=error_code,
        message=message,
        is_fatal=False,
    ))

    if review_reason not in state.manual_review_reasons:
        state.manual_review_reasons.append(review_reason)

    state.pipeline_stage = "error"
    state.audit_log.extend(audit_entries)
    state.audit_log.append(AuditEntry(
        timestamp=now_iso,
        node=NODE_NAME,
        field_mutated="pipeline_stage",
        value_before=prior_stage,
        value_after="error",
        rationale=rationale,
    ))
    upsert_lead(state, db_path)


def run_arv_estimation(
    run_id: str,
    db_path: Optional[str | Path] = None,
) -> dict[str, int]:
    """
    Node 5 — ARV Estimation.

    Reads:  pipeline_stage = 'enriched'
    Writes: remains at 'enriched' (no stage change on success)
            pipeline_stage = 'error' on failure
    Idempotent: skips leads with NODE_NAME already in data_sources.
    """
    settings = get_settings()
    leads = get_leads_by_stage(run_id, "enriched", db_path)

    metrics: dict[str, int] = {
        "processed": 0,
        "arv_calculated": 0,
        "insufficient_comps": 0,
        "skipped": 0,
        "errors": 0,
        "cache_hits": 0,
    }

    for state in leads:
        # Idempotency guard
        if NODE_NAME in state.data_sources:
            metrics["skipped"] += 1
            continue

        # Sequential guard: Node 4 must have run first
        if "node_04_enrichment" not in state.data_sources:
            metrics["skipped"] += 1
            continue

        metrics["processed"] += 1
        now_iso = utc_now_iso()
        audit_entries: list[AuditEntry] = []

        try:
            # Guard: APN and county required for comps lookup
            if not state.apn or not state.county:
                _fail_lead(
                    state,
                    audit_entries,
                    error_code="MISSING_REQUIRED_INPUT",
                    message="Missing APN or county required for comps lookup.",
                    review_reason="MISSING_APN_OR_COUNTY",
                    rationale="Cannot fetch comps without APN and county",
                    now_iso=now_iso,
                    db_path=db_path,
                )
                metrics["errors"] += 1
                continue

            # 1. Fetch comps with cache
            comps, was_cached = fetch_comps_with_cache(
                state.apn, state.county, db_path
            )
            if was_cached:
                metrics["cache_hits"] += 1

            # 2. Zero-comps path — flag for manual review, no ARV produced
            if not comps:
                state.arv_confidence = "low"  # type: ignore[assignment]

                if "INSUFFICIENT_COMPS" not in state.manual_review_reasons:
                    state.manual_review_reasons.append("INSUFFICIENT_COMPS")

                audit_entries.append(AuditEntry(
                    timestamp=now_iso,
                    node=NODE_NAME,
                    field_mutated="arv_confidence",
                    value_before=None,
                    value_after="low",
                    rationale="No comparable sales returned — cannot estimate ARV",
                ))
                metrics["insufficient_comps"] += 1

            else:
                # 3. Compute median ARV from sorted comp prices
                sorted_comps = sorted(comps, key=lambda c: c.sold_price)
                prices = [c.sold_price for c in sorted_comps]
                comp_count = len(sorted_comps)
                median_arv = round(statistics.median(prices), 2)

                # 4. Assign confidence band per settings thresholds
                if comp_count >= settings.ARV_COMP_COUNT_HIGH:
                    confidence = "high"
                elif comp_count >= settings.ARV_COMP_COUNT_MEDIUM:
                    confidence = "medium"
                else:
                    confidence = "low"
                    if "INSUFFICIENT_COMPS" not in state.manual_review_reasons:
                        state.manual_review_reasons.append("INSUFFICIENT_COMPS")
                    metrics["insufficient_comps"] += 1

                # 5. Apply risk-adjusted haircut
                haircut = ARV_HAIRCUTS.get(confidence, 0.10)
                risk_adj_arv = round(median_arv * (1.0 - haircut), 2)

                # 6. Write to state
                state.estimated_arv_base = median_arv
                state.risk_adjusted_arv = risk_adj_arv
                state.arv_confidence = confidence  # type: ignore[assignment]
                state.comps_used = sorted_comps

                # 7. Audit all field mutations
                audit_entries.append(AuditEntry(
                    timestamp=now_iso,
                    node=NODE_NAME,
                    field_mutated="estimated_arv_base",
                    value_before=None,
                    value_after=median_arv,
                    rationale=(
                        f"Median of {comp_count} comparable sold prices: {prices}"
                    ),
                ))
                audit_entries.append(AuditEntry(
                    timestamp=now_iso,
                    node=NODE_NAME,
                    field_mutated="arv_confidence",
                    value_before=None,
                    value_after=confidence,
                    rationale=(
                        f"{comp_count} comps: "
                        f">={settings.ARV_COMP_COUNT_HIGH}=high, "
                        f">={settings.ARV_COMP_COUNT_MEDIUM}=medium, else=low"
                    ),
                ))
                audit_entries.append(AuditEntry(
                    timestamp=now_iso,
                    node=NODE_NAME,
                    field_mutated="risk_adjusted_arv",
                    value_before=None,
                    value_after=risk_adj_arv,
                    rationale=(
                        f"Applied {int(haircut * 100)}% haircut to "
                        f"median_arv={median_arv} based on {confidence} confidence"
                    ),
                ))
                audit_entries.append(AuditEntry(
                    timestamp=now_iso,
                    node=NODE_NAME,
                    field_mutated="comps_used",
                    value_before=None,
                    value_after=comp_count,
                    rationale=(
                        f"Stored {comp_count} comps (cache_hit={was_cached})"
                    ),
                ))

                metrics["arv_calculated"] += 1

            # 8. Finalize — no stage change per spec
            state.data_sources.append(NODE_NAME)
            state.audit_log.extend(audit_entries)
            upsert_lead(state, db_path)

            logger.info(
                "node_05_arv: processed property %s (APN=%s) "
                "arv_base=%s risk_adj_arv=%s confidence=%s comps=%d",
                state.property_id,
                state.apn,
                getattr(state, "estimated_arv_base", None),
                getattr(state, "risk_adjusted_arv", None),
                getattr(state, "arv_confidence", None),
                len(comps),
            )

        except Exception as exc:
            metrics["errors"] += 1
            _fail_lead(
                state,
                audit_entries,
                error_code="ARV_ESTIMATION_FAILED",
                message=(
                    f"Exception during ARV estimation for "
                    f"property_id={state.property_id}: {exc}"
                ),
                review_reason="ARV_ESTIMATION_FAILED",
                rationale=f"Unexpected error: {exc}",
                now_iso=now_iso,
                db_path=db_path,
            )
            logger.exception(
                "node_05_arv: estimation failed",
                extra={"property_id": state.property_id},
            )

    logger.info(
        "node_05_arv complete",
        extra={"run_id": run_id, "metrics": metrics},
    )
    return metrics


if __name__ == "__main__":
    from core.database import init_db

    logging.basicConfig(level=logging.INFO)
    init_db()
    print(run_arv_estimation("manual-arv-check"))
