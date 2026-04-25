# nodes/node_06_images.py
"""
Node 6 — Property Image Retrieval

Reads:  pipeline_stage = 'enriched'
Writes: remains at 'enriched' (no stage change on success)
"""

import logging
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
from core.models import AuditEntry, JCMPropertyState, PipelineError
from core.settings import get_settings
from core.utils import retry_with_backoff

logger = logging.getLogger(__name__)
NODE_NAME = "node_06_images"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@retry_with_backoff(max_retries=3)
def fetch_property_images(address: str) -> list[str]:
    """Mock function representing a property image API call."""
    if not get_settings().USE_MOCK_APIS:
        raise NotImplementedError(
            "Real image API not implemented. Set USE_MOCK_APIS=true or implement fetch_property_images() with a real provider."
        )
    return [
        "https://mock.propstream.com/images/front.jpg",
        "https://mock.propstream.com/images/side.jpg",
    ]


def fetch_images_with_cache(
    apn: str,
    county: str,
    address: str,
    db_path: Optional[str | Path] = None,
) -> tuple[list[str], bool]:
    """Wraps image API call with TTL cache."""
    settings = get_settings()
    cache_key = f"images:{apn}:{county}"

    cached = get_cache(cache_key, db_path=db_path)
    if cached is not None:
        return cached, True

    images = fetch_property_images(address)
    if images:
        set_cache(
            cache_key,
            images,
            make_expires_at(settings.CACHE_TTL_IMAGES_DAYS),
            db_path=db_path,
        )

    return images, False


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
    """Shared helper to elegantly fail the lead."""
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


def run_image_retrieval(
    run_id: str,
    db_path: Optional[str | Path] = None,
) -> dict[str, int]:
    """Node 6 — Property Image Retrieval."""
    leads = get_leads_by_stage(run_id, "enriched", db_path)

    metrics: dict[str, int] = {
        "processed": 0,
        "images_found": 0,
        "no_images": 0,
        "skipped": 0,
        "errors": 0,
        "cache_hits": 0,
    }

    for state in leads:
        if NODE_NAME in state.data_sources:
            metrics["skipped"] += 1
            continue

        if "node_05_arv" not in state.data_sources:
            metrics["skipped"] += 1
            continue

        metrics["processed"] += 1
        now_iso = utc_now_iso()
        audit_entries: list[AuditEntry] = []
        
        was_cached = False
        images: list[str] = []

        try:
            lookup_address = state.normalized_address or state.raw_address
            apn = state.apn or ""
            county = state.county or ""

            # --- Missing address/APN/County path ---
            if not lookup_address or not apn or not county:
                state.image_urls = []
                state.image_capture_dates = []
                if "NO_IMAGES_FOUND" not in state.manual_review_reasons:
                    state.manual_review_reasons.append("NO_IMAGES_FOUND")

                audit_entries.append(AuditEntry(
                    timestamp=now_iso, node=NODE_NAME, field_mutated="image_urls",
                    value_before=None, value_after=[], rationale="Missing address, APN, or county for lookup",
                ))
                metrics["no_images"] += 1

                state.data_sources.append(NODE_NAME)
                state.audit_log.extend(audit_entries)
                upsert_lead(state, db_path)
                continue

            # --- 1. Fetch images with cache ---
            images, was_cached = fetch_images_with_cache(apn, county, lookup_address, db_path)
            if was_cached:
                metrics["cache_hits"] += 1

            # --- 2. Handle empty image response ---
            if not images:
                state.image_urls = []
                state.image_capture_dates = []
                if "NO_IMAGES_FOUND" not in state.manual_review_reasons:
                    state.manual_review_reasons.append("NO_IMAGES_FOUND")

                audit_entries.append(AuditEntry(
                    timestamp=now_iso, node=NODE_NAME, field_mutated="image_urls",
                    value_before=None, value_after=[], rationale=f"API returned no images for: {lookup_address}",
                ))
                metrics["no_images"] += 1

            else:
                # --- 3. Images found ---
                state.image_urls = images
                state.image_capture_dates = [now_iso[:10]] * len(images)  # Mock today's date for Sprint 1
                
                audit_entries.append(AuditEntry(
                    timestamp=now_iso, node=NODE_NAME, field_mutated="image_urls",
                    value_before=None,
                    value_after=images,
                    rationale=f"Retrieved {len(images)} images (cache_hit={was_cached})",
                ))
                audit_entries.append(AuditEntry(
                    timestamp=now_iso, node=NODE_NAME, field_mutated="image_capture_dates",
                    value_before=None,
                    value_after=state.image_capture_dates,
                    rationale="Assigned mock capture dates for retrieved images",
                ))
                metrics["images_found"] += 1

            # --- 4. Finalize (Explicit stage-unchanged audit) ---
            audit_entries.append(AuditEntry(
                timestamp=now_iso, node=NODE_NAME, field_mutated="pipeline_stage",
                value_before=state.pipeline_stage, value_after=state.pipeline_stage,
                rationale="Image collection complete; stage unchanged — Node 7 reads 'enriched' leads",
            ))

            state.data_sources.append(NODE_NAME)
            state.audit_log.extend(audit_entries)
            upsert_lead(state, db_path)

            logger.info("Node 6: processed property %s (APN=%s) images=%d cache_hit=%s",
                state.property_id, apn, len(state.image_urls), was_cached
            )

        except Exception as exc:
            metrics["errors"] += 1
            _fail_lead(
                state, audit_entries, "IMAGE_UNAVAILABLE",
                f"Exception during image retrieval: {exc}",
                "IMAGE_UNAVAILABLE", f"Unexpected error: {exc}",
                now_iso, db_path
            )
            logger.exception("Node 6 Error: %s", exc, extra={"property_id": state.property_id})

    logger.info("Node 6 complete. Metrics: %s", metrics)
    return metrics


if __name__ == "__main__":
    from core.database import init_db
    logging.basicConfig(level=logging.INFO)
    init_db()
    print(run_image_retrieval("manual-image-check"))
