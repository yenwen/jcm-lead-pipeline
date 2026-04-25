# nodes/node_09_publisher.py
"""
Node 9 — CRM Publisher

Reads:  pipeline_stage = 'underwritten'
Writes: pipeline_stage = 'published'

Responsibilities per SPEC.md Section 7, Node 9:
  - Read leads at pipeline_stage='underwritten' with node_08_underwriting complete.
  - Publish lead data to external CRM (Airtable — mock for Sprint 1).
  - Assign crm_record_id from API response.
  - Advance pipeline_stage to 'published'.
  - On failure: _fail_lead with CRM_PUBLISH_FAILED.
"""

import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional

from core.database import get_leads_for_publishing, upsert_lead
from core.models import AuditEntry, JCMPropertyState, PipelineError
from core.settings import get_settings
from core.utils import retry_with_backoff

logger = logging.getLogger(__name__)
NODE_NAME = "node_09_publisher"
NODE_08_NAME = "node_08_underwriting"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# CRM API (mock for Sprint 1)
# ---------------------------------------------------------------------------

@retry_with_backoff(max_retries=3)
def publish_to_crm(state: JCMPropertyState) -> str:
    """
    Mock function representing an Airtable CRM API publish call.
    Returns a mock Airtable Record ID.

    Sprint 1: returns a deterministic mock ID based on property_id.
    TODO: Replace with real Airtable API call using pyairtable in v1.1.
    """
    if not get_settings().USE_MOCK_APIS:
        raise NotImplementedError(
            "Real Airtable CRM API not implemented. Set USE_MOCK_APIS=true or implement publish_to_crm() with pyairtable."
        )
    digest = hashlib.sha256(state.property_id.encode("utf-8")).hexdigest()[:10]
    return f"recMockAirtableId{digest}"


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
    """Shared helper: fail lead to 'error' state."""
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
# Main publisher entry point
# ---------------------------------------------------------------------------

def run_publisher(
    run_id: str,
    db_path: Optional[str | Path] = None,
) -> dict[str, int]:
    """
    Node 9 — CRM Publisher.

    Reads:  pipeline_stage = 'underwritten'
    Writes: pipeline_stage = 'published' (success)
            pipeline_stage = 'error' (on exception)
    Idempotent: skips leads with NODE_NAME already in data_sources.
    """
    settings = get_settings()
    all_underwritten = get_leads_for_publishing(run_id, db_path)

    metrics: dict[str, int] = {
        "processed": 0,
        "published": 0,
        "skipped": 0,
        "rejected": 0,
        "errors": 0,
    }

    # Split rejected leads off first — they never touch the CRM.
    # Advance their stage to 'rejected' so dashboards and future Node 9
    # runs don't see them as pending publish work.
    publishable: list[JCMPropertyState] = []
    for state in all_underwritten:
        recommendation = state.decision.recommendation if state.decision else None
        if recommendation == "rejected":
            if state.pipeline_stage != "rejected":
                prior_stage = state.pipeline_stage
                now_iso = utc_now_iso()
                state.pipeline_stage = "rejected"
                state.audit_log.append(AuditEntry(
                    timestamp=now_iso,
                    node=NODE_NAME,
                    field_mutated="pipeline_stage",
                    value_before=prior_stage,
                    value_after="rejected",
                    rationale="Underwriting decision=rejected — not published to CRM",
                ))
                upsert_lead(state, db_path)
            metrics["rejected"] += 1
            continue
        if recommendation in ("approved", "manual_review"):
            publishable.append(state)
        else:
            # No decision or unexpected value — skip rather than publish.
            metrics["skipped"] += 1

    # Enforce MAX_PUBLISH_PER_RUN. get_leads_for_publishing() already sorts
    # approved-before-manual_review and by descending final_rank_score.
    leads = publishable[: settings.MAX_PUBLISH_PER_RUN]
    if len(publishable) > settings.MAX_PUBLISH_PER_RUN:
        deferred = len(publishable) - settings.MAX_PUBLISH_PER_RUN
        metrics["skipped"] += deferred
        logger.info(
            "Node 9: deferring %d lead(s) beyond MAX_PUBLISH_PER_RUN=%d",
            deferred,
            settings.MAX_PUBLISH_PER_RUN,
        )

    for state in leads:
        # Idempotency guard
        if NODE_NAME in state.data_sources:
            metrics["skipped"] += 1
            continue

        # Sequential guard: Node 8 must have run
        if NODE_08_NAME not in state.data_sources:
            metrics["skipped"] += 1
            continue

        metrics["processed"] += 1
        now_iso = utc_now_iso()
        audit_entries: list[AuditEntry] = []

        try:
            # --- 1. Publish to CRM ---
            crm_payload_id = publish_to_crm(state)
            state.crm_payload_id = crm_payload_id

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="crm_payload_id",
                value_before=None,
                value_after=crm_payload_id,
                rationale=(
                    f"Published to Airtable CRM — "
                    f"record_id={crm_payload_id}"
                ),
            ))

            # --- 2. Advance stage to 'published' ---
            prior_stage = state.pipeline_stage
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="pipeline_stage",
                value_before=prior_stage,
                value_after="published",
                rationale="CRM publish successful — pipeline complete",
            ))

            state.pipeline_stage = "published"
            state.data_sources.append(NODE_NAME)
            state.audit_log.extend(audit_entries)
            upsert_lead(state, db_path)
            metrics["published"] += 1

            logger.info(
                "Node 9: published property %s (APN=%s) "
                "crm_payload_id=%s decision=%s",
                state.property_id,
                state.apn,
                crm_payload_id,
                state.decision.recommendation if state.decision else "N/A",
            )

        except Exception as exc:
            metrics["errors"] += 1
            _fail_lead(
                state,
                audit_entries,
                error_code="CRM_PUBLISH_FAILED",
                message=(
                    f"Exception during CRM publish for "
                    f"property_id={state.property_id}: {exc}"
                ),
                review_reason="CRM_PUBLISH_FAILED",
                rationale=f"Unexpected error: {exc}",
                now_iso=now_iso,
                db_path=db_path,
            )
            logger.exception(
                "Node 9 Error: %s",
                exc,
                extra={"property_id": state.property_id},
            )

    logger.info("Node 9 complete. Metrics: %s", metrics)
    return metrics


if __name__ == "__main__":
    from core.database import init_db

    logging.basicConfig(level=logging.INFO)
    init_db()
    print(run_publisher("manual-publish-check"))
