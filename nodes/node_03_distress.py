"""
Node 3 — Distress Enrichment & Scoring

Responsibilities per SPEC.md Section 7, Node 3:
  - Fetch all leads at pipeline_stage='normalized' for the current run.
  - Calculate distress score from weighted signals (Spec 1.8.2 Section 8.1).
  - Assign distress_tier based on score thresholds.
  - Reject leads below 20-point threshold.
  - Passing leads remain at 'normalized' for Node 4 sequential pickup.
  - On failure: attach PipelineError, set pipeline_stage='error', persist, continue.
"""

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional, cast

from core.database import get_leads_by_stage, upsert_lead
from core.models import AuditEntry, DistressTier, JCMPropertyState, PipelineError

logger = logging.getLogger(__name__)
NODE_NAME = "node_03_distress"

# Spec 1.8.2 Weights
WEIGHTS: dict[str, float] = {
    "preforeclosure": 0.60,
    "tax_delinquency": 0.50,
    "probate": 0.40,
    "code_violation": 0.30,
    "vacancy": 0.25,
    "days_on_market": 0.20,
}
MAX_POSSIBLE_SUM: float = 2.25


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def calculate_distress_score(state: JCMPropertyState) -> tuple[float, str, bool]:
    """Calculates distress score per Spec 1.8.2 math."""
    if not state.distress_signals:
        return 0.0, "none", False

    type_max_severity: dict[str, float] = {}
    for sig in state.distress_signals:
        # Omit DOM if off-market
        if sig.signal_type == "days_on_market" and state.listing_status in ("off_market", "sold", "unknown", None):
            continue
        
        current_max = type_max_severity.get(sig.signal_type, 0.0)
        type_max_severity[sig.signal_type] = max(current_max, sig.severity)

    weighted_sum = sum(severity * WEIGHTS.get(sig_type, 0.0) for sig_type, severity in type_max_severity.items())
    
    corroboration_applied = sum(1 for sig in state.distress_signals if sig.corroborated) >= 2
    if corroboration_applied:
        weighted_sum += 0.10

    score = min(100.0, round((weighted_sum / MAX_POSSIBLE_SUM) * 100))
    
    if score == 0:
        tier = "none"
    elif score < 25:
        tier = "weak"
    elif score < 50:
        tier = "moderate"
    else:
        tier = "strong"
        
    return score, tier, corroboration_applied


def run_distress_scoring(run_id: str, db_path: Optional[str | Path] = None) -> dict[str, int]:
    """Node 3 — Distress Enrichment & Scoring."""
    leads = get_leads_by_stage(run_id, "normalized", db_path)
    metrics = {"processed": 0, "scored": 0, "rejected": 0, "skipped": 0, "errors": 0}

    for state in leads:
        # Prevent re-running if already processed by this node
        if NODE_NAME in state.data_sources:
            metrics["skipped"] += 1
            continue

        metrics["processed"] += 1
        now_iso = utc_now_iso()
        
        try:
            score, tier, corroboration_applied = calculate_distress_score(state)
            
            state.audit_log.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="distress_score",
                value_before=state.distress_score,
                value_after=score,
                rationale=f"Calculated via max-severity weights (Tier: {tier})",
            ))
            
            state.distress_score = score
            state.distress_tier = cast(DistressTier, tier)
            state.data_sources.append(NODE_NAME)

            prior_stage = state.pipeline_stage

            # Hardcoded 20-point threshold per Spec Section 8.1.
            # Low distress is a routing decision, not a pipeline error — record
            # it on manual_review_reasons and advance stage to 'rejected'.
            if score < 20:
                skip_reason = f"LOW_DISTRESS_SCORE:{score}"
                if skip_reason not in state.manual_review_reasons:
                    state.manual_review_reasons.append(skip_reason)

                state.audit_log.append(AuditEntry(
                    timestamp=now_iso,
                    node=NODE_NAME,
                    field_mutated="pipeline_stage",
                    value_before=prior_stage,
                    value_after="rejected",
                    rationale=f"Distress score {score} below minimum threshold of 20",
                ))
                state.pipeline_stage = "rejected"
                metrics["rejected"] += 1
            else:
                # Do NOT advance the pipeline_stage to 'enriched' here.
                # Leave it as 'normalized' so Node 4 can pick it up sequentially.
                metrics["scored"] += 1
            
            upsert_lead(state, db_path)

        except Exception as e:
            logger.error(f"Error in {NODE_NAME} for property_id={state.property_id}: {e}")
            metrics["errors"] += 1
            now_iso = utc_now_iso()
            prior_stage = state.pipeline_stage
            
            err = PipelineError(
                timestamp=now_iso,
                node=NODE_NAME,
                error_code="DISTRESS_SCORING_FAILED",
                message=str(e),
                is_fatal=False
            )
            state.errors.append(err)
            
            state.audit_log.append(AuditEntry(
                timestamp=now_iso, 
                node=NODE_NAME, 
                field_mutated="pipeline_stage",
                value_before=prior_stage, 
                value_after="error", 
                rationale=f"Exception during scoring: {e}"
            ))
            state.pipeline_stage = "error"
            upsert_lead(state, db_path)

    logger.info("Node 3 complete. Metrics: %s", metrics)
    return metrics
