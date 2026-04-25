# adapters/common.py
"""
Shared adapter helpers — standardized metrics, run lifecycle, and dedupe policy.

Every adapter should use these helpers to maintain consistent behavior:
  - Metrics: standard keys returned by every adapter
  - Run lifecycle: init → started → adapter_complete|failed
  - Dry-run: zero DB writes, predictable preview metrics
  - Dedupe: consistent same-run + cross-run policy
"""

import logging
from datetime import UTC, datetime
from typing import Optional

from core.database import (
    find_recent_lead_by_property_id,
    init_db,
    insert_run,
    update_run_status,
    upsert_lead,
)
from core.models import JCMPropertyState
from core.settings import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Standardized metrics
# ---------------------------------------------------------------------------

# Every adapter MUST return at least these keys.
STANDARD_METRIC_KEYS = (
    "items_fetched",
    "leads_mapped",
    "leads_ingested_new",
    "duplicates_skipped",
    "mapping_errors",
    "fetch_errors",
)


def make_metrics(**extras: int) -> dict[str, int]:
    """
    Return a fresh metrics dict with all standard keys initialized to 0.

    Extra adapter-specific keys can be passed as keyword arguments:
        make_metrics(filtered_out=0)
    """
    m: dict[str, int] = {k: 0 for k in STANDARD_METRIC_KEYS}
    m.update(extras)
    return m


# ---------------------------------------------------------------------------
# Run lifecycle helpers
# ---------------------------------------------------------------------------

def generate_run_id(adapter_prefix: str) -> str:
    """Generate a timestamped run ID: e.g. ``adapter_cl_20260423_181500``."""
    return f"{adapter_prefix}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"


def start_run(run_id: str, notes: str, *, dry_run: bool) -> None:
    """
    Initialize DB and register a new run record.

    In dry-run mode this is a no-op — no DB side effects.
    """
    if dry_run:
        logger.info("[DRY RUN] Skipping DB init and run registration for %s", run_id)
        return
    init_db()
    insert_run(run_id=run_id, status="started", notes=notes)
    logger.info("Run registered: %s", run_id)


def complete_run(run_id: str, metrics: dict[str, int], *, dry_run: bool) -> None:
    """
    Mark a run as ``adapter_complete``.

    In dry-run mode this is a no-op.
    """
    if dry_run:
        return
    now_iso = datetime.now(UTC).isoformat()
    summary = ", ".join(f"{k}={v}" for k, v in metrics.items())
    update_run_status(
        run_id=run_id,
        status="adapter_complete",
        completed_at=now_iso,
        notes=summary,
    )


def fail_run(run_id: str, reason: str, *, dry_run: bool) -> None:
    """
    Mark a run as ``failed``.

    In dry-run mode this is a no-op.
    """
    if dry_run:
        return
    now_iso = datetime.now(UTC).isoformat()
    update_run_status(
        run_id=run_id,
        status="failed",
        completed_at=now_iso,
        notes=reason,
    )


# ---------------------------------------------------------------------------
# Persist helper
# ---------------------------------------------------------------------------

def persist_lead(state: JCMPropertyState, *, dry_run: bool) -> None:
    """
    Compute source confidence and upsert a lead into the database.

    Source confidence is always attached (even in dry-run) so metrics
    and logging reflect the quality signal.

    In dry-run mode the DB write is skipped.
    """
    # Attach source confidence report
    try:
        from core.source_confidence import compute_source_confidence
        sc = compute_source_confidence(state)
        state.source_confidence = sc.model_dump()
    except Exception as exc:
        logger.debug("Source confidence computation failed: %s", exc)

    if dry_run:
        return
    upsert_lead(state)


# ---------------------------------------------------------------------------
# Dedupe policy
# ---------------------------------------------------------------------------
#
# Consistent dedupe contract across all adapters:
#
#   SAME RUN + SAME property_id  →  duplicate (skip)
#   CROSS-RUN within lookback window:
#     - prior lead in terminal state (published, rejected)  →  skip
#     - prior lead in reprocessable state (error, sourced, normalized, etc.)  →  allow refresh
#     - no prior lead  →  allow
#
# Source-specific identity:
#   - APN-based where available (firecrawl_county uses find_recent_lead_by_apn)
#   - property_id-based everywhere else
#

# Terminal stages: lead has been fully processed — do not re-ingest within lookback.
TERMINAL_STAGES = frozenset({"published", "rejected"})


def should_skip_lead(
    property_id: str,
    run_id: str,
    *,
    seen_this_run: set[str],
) -> bool:
    """
    Evaluate dedupe policy for a property_id-based adapter.

    Args:
        property_id: Deterministic ID for this lead.
        run_id:      Current adapter run ID.
        seen_this_run: Mutable set tracking property_ids already processed
                       in this invocation. Caller must maintain this.

    Returns:
        True if the lead should be skipped (duplicate).
    """
    # Intra-run dedupe: same property_id seen in this adapter invocation
    if property_id in seen_this_run:
        return True
    seen_this_run.add(property_id)

    # Cross-run dedupe: check recent history
    settings = get_settings()
    try:
        recent = find_recent_lead_by_property_id(
            property_id=property_id,
            lookback_days=settings.CROSS_RUN_LOOKBACK_DAYS,
        )
    except Exception as exc:
        logger.error("Dedupe lookup failed for %s: %s", property_id, exc)
        return False  # fail-open: allow ingestion

    if recent is None:
        return False

    # Same run collision (shouldn't happen with seen_this_run, but defensive)
    if recent.run_id == run_id:
        return True

    # Cross-run: block only terminal stages
    return recent.pipeline_stage in TERMINAL_STAGES
