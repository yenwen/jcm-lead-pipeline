# nodes/node_07_vision.py
"""
Node 7 — The Cost Oracle (Vision Analysis)

Reads:  pipeline_stage = 'enriched'
Writes: remains at 'enriched' (no stage change — Sprint 1 override)

Responsibilities:
  - Analyze property images via LLM vision model (mock for Sprint 1).
  - LLM returns ONLY VisualObservation objects — forbidden from dollar amounts.
  - Cost Oracle (Python-only) multiplies observations × regional pricing matrix.
  - Computes visible_scope_capex_base, hidden_scope_reserve, total_rehab_estimate.
  - Populates rehab_line_items from CostLineItem objects.
  - Assigns capex_confidence per Spec 1.8.2 Section 8.3:
      <2 images → low, 2-3 → medium, 4+ AND avg conf >= 0.75 → high.
  - Handles missing images gracefully (no error, flag for manual review).
  - On unexpected exception: _fail_lead with VISION_MODEL_UNAVAILABLE.
"""

import logging
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional

from pydantic import ValidationError

from core.database import get_leads_by_stage, upsert_lead
from core.models import (
    AuditEntry,
    CostLineItem,
    JCMPropertyState,
    PipelineError,
    VisualObservation,
)
from core.settings import get_settings
from core.utils import retry_with_backoff

logger = logging.getLogger(__name__)
NODE_NAME = "node_07_vision"


# ---------------------------------------------------------------------------
# Regional Pricing Matrix (Spec 1.8.2 Section 8.3)
# Key: "{category}_{unit}" → oracle unit price in USD
# TODO: Migrate to regional_pricing_matrix.json in v1.1
# ---------------------------------------------------------------------------

REGIONAL_PRICING_MATRIX: dict[str, float] = {
    "roof_sqft": 12.50,
    "paint_sqft": 4.75,
    "windows_unit": 650.00,
    "landscaping_sqft": 3.25,
    "driveway_sqft": 8.50,
    "fence_lf": 35.00,
    "garage_door_unit": 1_200.00,
    "debris_flat": 2_500.00,
    "exterior_unknown_flat": 1_500.00,  # Fallback for unrecognized categories
}


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# LLM Vision Analysis (mock for Sprint 1)
# ---------------------------------------------------------------------------

@retry_with_backoff(max_retries=3)
def analyze_images_with_llm(image_urls: list[str]) -> list[VisualObservation]:
    """
    Mock function representing an OpenAI GPT-4o vision analysis call.
    Returns ONLY VisualObservation objects — the LLM is forbidden
    from estimating dollar amounts (Spec 1.8.2 constraint).

    Sprint 1: returns two hardcoded observations (roof damage + paint issues).
    TODO: Replace with real OpenAI vision API call in v1.1.
    """
    if not get_settings().USE_MOCK_APIS:
        raise NotImplementedError(
            "Real vision API not implemented. Set USE_MOCK_APIS=true or implement analyze_images_with_llm() with OpenAI."
        )
    return [
        VisualObservation(
            image_source=image_urls[0] if image_urls else "unknown",
            image_capture_date=None,
            category="roof",
            condition="poor",
            estimated_quantity=800.0,
            unit="sqft",
            evidence="Visible shingle deterioration and moss growth on south-facing slope",
            confidence=0.82,
        ),
        VisualObservation(
            image_source=image_urls[1] if len(image_urls) > 1 else image_urls[0],
            image_capture_date=None,
            category="paint",
            condition="fair",
            estimated_quantity=1200.0,
            unit="sqft",
            evidence="Fading and peeling paint on front and side exterior walls",
            confidence=0.75,
        ),
    ]


def _validate_llm_observations(
    raw: list[Any],
    audit_entries: list[AuditEntry],
    now_iso: str,
) -> list[VisualObservation]:
    """
    Validate raw LLM output (dicts or VisualObservation objects) against the
    VisualObservation schema. Invalid items are logged and skipped rather than
    crashing the entire lead — a partial observation set is better than none.
    """
    valid: list[VisualObservation] = []
    for i, item in enumerate(raw):
        if isinstance(item, VisualObservation):
            valid.append(item)
            continue
        try:
            valid.append(VisualObservation.model_validate(item))
        except ValidationError as exc:
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="visual_observations",
                value_before=str(item),
                value_after=None,
                rationale=f"Observation [{i}] failed Pydantic validation and was dropped: {exc}",
            ))
            logger.warning("Node 7: dropped invalid LLM observation [%d]: %s", i, exc)
    return valid


# ---------------------------------------------------------------------------
# Cost Oracle — Python-only dollar math (Spec 1.8.2 Section 8.3)
# ---------------------------------------------------------------------------

def apply_cost_oracle(
    observations: list[VisualObservation],
    audit_entries: list[AuditEntry],
    now_iso: str,
) -> tuple[list[CostLineItem], float]:
    """
    Converts VisualObservation objects into CostLineItem objects using
    the regional pricing matrix. All dollar math is Python-only — the LLM
    never touches prices.

    Returns:
        (rehab_line_items, visible_scope_capex_base)
    """
    line_items: list[CostLineItem] = []

    for obs in observations:
        pricing_key = f"{obs.category}_{obs.unit}"
        unit_price = REGIONAL_PRICING_MATRIX.get(pricing_key)

        if unit_price is None:
            # Fallback to exterior_unknown_flat for unrecognized categories
            fallback_key = "exterior_unknown_flat"
            unit_price = REGIONAL_PRICING_MATRIX[fallback_key]

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="rehab_line_items",
                value_before=pricing_key,
                value_after=fallback_key,
                rationale=(
                    f"Pricing key '{pricing_key}' not found in matrix — "
                    f"falling back to '{fallback_key}' at ${unit_price:.2f}"
                ),
            ))

        total_cost = round(obs.estimated_quantity * unit_price, 2)

        line_items.append(CostLineItem(
            category=obs.category,
            oracle_unit_price=unit_price,
            estimated_quantity=obs.estimated_quantity,
            unit=obs.unit,
            total_cost=total_cost,
        ))

    visible_scope_capex_base = round(
        sum(item.total_cost for item in line_items), 2
    )

    return line_items, visible_scope_capex_base


def compute_hidden_scope_reserve(
    visible_capex: float,
    year_built: Optional[int],
) -> tuple[float, float]:
    """
    Compute hidden scope reserve per Spec 1.8.2 Section 7.2.
    Returns (reserve_pct, reserve_amount).
    """
    settings = get_settings()

    if year_built is not None and year_built < 1940:
        pct = settings.HIDDEN_SCOPE_RESERVE_PCT_PRE1940
    elif year_built is not None and year_built < 1965:
        pct = settings.HIDDEN_SCOPE_RESERVE_PCT_PRE1965
    else:
        pct = settings.HIDDEN_SCOPE_RESERVE_PCT_DEFAULT

    reserve = round(visible_capex * pct, 2)
    return pct, reserve


def determine_capex_confidence(
    image_count: int,
    observations: list[VisualObservation],
) -> str:
    """
    Determine capex_confidence per Spec 1.8.2 Section 8.3:
      - < 2 usable images → "low"
      - 2-3 usable images → "medium"
      - 4+ usable images AND avg observation confidence >= threshold → "high"
      - Otherwise → "medium"
    """
    settings = get_settings()

    if image_count < settings.VISION_IMAGE_COUNT_MEDIUM:
        return "low"

    if image_count >= settings.VISION_IMAGE_COUNT_HIGH and observations:
        avg_conf = statistics.mean(obs.confidence for obs in observations)
        if avg_conf >= settings.VISION_AVG_CONFIDENCE_MIN_HIGH:
            return "high"

    return "medium"


# ---------------------------------------------------------------------------
# Failure helper
# ---------------------------------------------------------------------------

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
    """Shared helper: fail the lead to 'error' state."""
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


# ---------------------------------------------------------------------------
# Main vision analysis entry point
# ---------------------------------------------------------------------------

def run_vision_analysis(
    run_id: str,
    db_path: Optional[str | Path] = None,
) -> dict[str, int]:
    """
    Node 7 — The Cost Oracle.

    Reads:  pipeline_stage = 'enriched'
    Writes: remains at 'enriched' (no stage change on success)
            pipeline_stage = 'error' on unexpected exception only
    Idempotent: skips leads with NODE_NAME already in data_sources.
    """
    leads = get_leads_by_stage(run_id, "enriched", db_path)

    metrics: dict[str, int] = {
        "processed": 0,
        "analyzed": 0,
        "no_images": 0,
        "skipped": 0,
        "errors": 0,
    }

    for state in leads:
        # Idempotency guard
        if NODE_NAME in state.data_sources:
            metrics["skipped"] += 1
            continue

        # Sequential guard: Node 6 (images) must have run first
        if "node_06_images" not in state.data_sources:
            metrics["skipped"] += 1
            continue

        metrics["processed"] += 1
        now_iso = utc_now_iso()
        audit_entries: list[AuditEntry] = []

        try:
            # --- Missing image gate ---
            if not state.image_urls:
                state.capex_confidence = "low"  # type: ignore[assignment]
                state.visible_scope_capex_base = None
                state.visual_observations = []
                state.rehab_line_items = []
                state.hidden_scope_reserve = None
                state.total_rehab_estimate = None

                if "NO_IMAGES_FOR_VISION" not in state.manual_review_reasons:
                    state.manual_review_reasons.append("NO_IMAGES_FOR_VISION")

                audit_entries.append(AuditEntry(
                    timestamp=now_iso,
                    node=NODE_NAME,
                    field_mutated="capex_confidence",
                    value_before=None,
                    value_after="low",
                    rationale="No images available — cannot perform vision analysis",
                ))
                audit_entries.append(AuditEntry(
                    timestamp=now_iso,
                    node=NODE_NAME,
                    field_mutated="visible_scope_capex_base",
                    value_before=None,
                    value_after=None,
                    rationale="No images — capex left as None to signal missing data",
                ))

                metrics["no_images"] += 1

                state.data_sources.append(NODE_NAME)
                state.audit_log.extend(audit_entries)
                upsert_lead(state, db_path)

                logger.info(
                    "Node 7: no images for property %s — "
                    "flagged NO_IMAGES_FOR_VISION + LOW_CAPEX_CONFIDENCE",
                    state.property_id,
                )
                continue

            # --- 1. Analyze images with LLM (observations only, no $) ---
            raw_observations = analyze_images_with_llm(state.image_urls)
            observations = _validate_llm_observations(raw_observations, audit_entries, now_iso)

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="visual_observations",
                value_before=None,
                value_after=f"{len(observations)} observations",
                rationale=(
                    f"LLM vision analysis produced {len(observations)} findings "
                    f"from {len(state.image_urls)} images"
                ),
            ))
            state.visual_observations = observations

            # --- 2. Cost Oracle — Python-only dollar math ---
            line_items, visible_capex = apply_cost_oracle(
                observations, audit_entries, now_iso,
            )

            state.rehab_line_items = line_items
            state.visible_scope_capex_base = visible_capex

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="rehab_line_items",
                value_before=None,
                value_after=f"{len(line_items)} line items",
                rationale=(
                    f"Cost Oracle produced {len(line_items)} line items "
                    f"totaling ${visible_capex:,.2f}"
                ),
            ))
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="visible_scope_capex_base",
                value_before=None,
                value_after=visible_capex,
                rationale=(
                    f"Sum of {len(line_items)} line items from "
                    f"regional pricing matrix"
                ),
            ))

            # --- 3. Hidden scope reserve (Spec 1.8.2 Section 7.2) ---
            reserve_pct, reserve_amount = compute_hidden_scope_reserve(
                visible_capex, state.year_built,
            )
            state.hidden_scope_reserve = reserve_amount

            total_rehab = round(visible_capex + reserve_amount, 2)
            state.total_rehab_estimate = total_rehab

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="hidden_scope_reserve",
                value_before=None,
                value_after=reserve_amount,
                rationale=(
                    f"Reserve={reserve_pct*100:.0f}% of visible_capex=${visible_capex:,.2f} "
                    f"(year_built={state.year_built})"
                ),
            ))
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="total_rehab_estimate",
                value_before=None,
                value_after=total_rehab,
                rationale=(
                    f"visible_capex=${visible_capex:,.2f} + "
                    f"hidden_scope_reserve=${reserve_amount:,.2f}"
                ),
            ))

            # --- 4. Capex confidence (Spec 1.8.2 Section 8.3) ---
            confidence = determine_capex_confidence(
                len(state.image_urls), observations,
            )
            state.capex_confidence = confidence  # type: ignore[assignment]

            if confidence == "low":
                if "LOW_CAPEX_CONFIDENCE" not in state.manual_review_reasons:
                    state.manual_review_reasons.append("LOW_CAPEX_CONFIDENCE")

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="capex_confidence",
                value_before=None,
                value_after=confidence,
                rationale=(
                    f"{len(state.image_urls)} images, "
                    f"{len(observations)} observations: "
                    f"<2=low, 2-3=medium, 4+ with avg_conf>=0.75=high"
                ),
            ))

            # --- 5. Finalize — no stage change per Sprint 1 override ---
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="pipeline_stage",
                value_before=state.pipeline_stage,
                value_after=state.pipeline_stage,
                rationale="Vision analysis + Cost Oracle complete; stage unchanged",
            ))

            state.data_sources.append(NODE_NAME)
            state.audit_log.extend(audit_entries)
            upsert_lead(state, db_path)
            metrics["analyzed"] += 1

            logger.info(
                "Node 7: analyzed property %s (APN=%s) "
                "observations=%d visible_capex=$%.2f "
                "hidden_reserve=$%.2f total_rehab=$%.2f confidence=%s",
                state.property_id,
                state.apn,
                len(observations),
                visible_capex,
                reserve_amount,
                total_rehab,
                confidence,
            )

        except Exception as exc:
            metrics["errors"] += 1
            _fail_lead(
                state,
                audit_entries,
                error_code="VISION_MODEL_UNAVAILABLE",
                message=(
                    f"Exception during vision analysis for "
                    f"property_id={state.property_id}: {exc}"
                ),
                review_reason="VISION_MODEL_UNAVAILABLE",
                rationale=f"Unexpected error: {exc}",
                now_iso=now_iso,
                db_path=db_path,
            )
            logger.exception(
                "Node 7 Error: %s",
                exc,
                extra={"property_id": state.property_id},
            )

    logger.info("Node 7 complete. Metrics: %s", metrics)
    return metrics


if __name__ == "__main__":
    from core.database import init_db

    logging.basicConfig(level=logging.INFO)
    init_db()
    print(run_vision_analysis("manual-vision-check"))
