"""
Node 1 — Lead Source Node (Ingestion Shield)

Responsibilities per SPEC.md Section 7, Node 1:
  - Query PropStream API (ATTOM as fallback) using sourcing policy filters.
  - Validate every payload through the Pydantic ingestion shield.
  - Deduplicate within-run (local set) and cross-run (database lookback).
  - Apply cross-run dedup rules from SPEC.md Section 5.4.
  - Instantiate JCMPropertyState and persist to SQLite.
  - Halt when MAX_LEADS_PER_RUN is reached and log RUN_LIMIT_REACHED.
"""

import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional

import requests
from pydantic import ValidationError

from core.database import find_recent_lead_by_apn, upsert_lead
from core.models import (
    AuditEntry,
    DistressSignal,
    JCMPropertyState,
    PipelineError,
    PropStreamPayload,
)
from core.settings import get_settings
from core.utils import retry_with_backoff

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PropStream API fetch
# ---------------------------------------------------------------------------

def fetch_propstream_data_mock(county: str) -> list[dict[str, Any]]:
    """
    Mock PropStream API returning realistic Bay Area leads.
    Sprint 1: used in place of the real API until PROPSTREAM_API_KEY is set.
    TODO: swap back to fetch_propstream_data() when API key is available.
    """
    if not get_settings().USE_MOCK_APIS:
        raise NotImplementedError(
            "Real PropStream API not implemented. Set USE_MOCK_APIS=true or implement fetch_propstream_data()."
        )
    if county == "Santa Clara":
        return [
            {
                "parcel_number": "APN-SC-001",
                "property_address": "1600 Saratoga Ave, San Jose, CA 95129",
                "county_name": "Santa Clara",
                "list_price": 750000.0,
                "sqft": 1800,
                "year_built": 1985,
                "beds": 3,
                "baths": 2.0,
                "status": "Active",
                "property_type": "SFR",
                "tax_delinquent": True,
                "pre_foreclosure": False,
                "code_violation": None,
            },
            {
                "parcel_number": "APN-SC-002",
                "property_address": "255 N 1st St, San Jose, CA 95113",
                "county_name": "Santa Clara",
                "list_price": 1200000.0,
                "sqft": 2200,
                "year_built": 2005,
                "beds": 4,
                "baths": 3.0,
                "status": "Active",
                "property_type": "SFR",
                "tax_delinquent": False,
                "pre_foreclosure": False,
                "code_violation": False,
            },
        ]
    elif county == "Alameda":
        return [
            {
                "parcel_number": "APN-AL-003",
                "property_address": "1200 Lakeshore Ave, Oakland, CA 94606",
                "county_name": "Alameda",
                "list_price": 600000.0,
                "sqft": 1200,
                "year_built": 1955,
                "beds": 2,
                "baths": 1.0,
                "status": "Off Market",
                "property_type": "Condo",
                "tax_delinquent": True,
                "pre_foreclosure": True,
                "code_violation": True,
            },
        ]
    return []


@retry_with_backoff(max_retries=3)
def fetch_attom_data(county: str) -> list[dict[str, Any]]:
    """
    ATTOM Data fallback per SPEC.md Section 7, Node 1.
    Returns an empty list until the ATTOM adapter is fully implemented.
    Replace this stub with the real ATTOM API call in Phase 2.
    """
    settings = get_settings()

    if not settings.ATTOM_API_KEY:
        logger.warning("ATTOM_API_KEY not configured — fallback unavailable.")
        return []

    # TODO: implement real ATTOM API call
    logger.warning(
        "ATTOM fallback stub called for county=%s — returning empty list.", county
    )
    return []


# ---------------------------------------------------------------------------
# Distress signal extraction
# ---------------------------------------------------------------------------

def _extract_distress_signals(record: dict[str, Any]) -> list[DistressSignal]:
    """
    Extract initial distress signals from a raw API record dict.
    Accepts the raw dict (not PropStreamPayload) because the Pydantic model
    strips extra fields like tax_delinquent, pre_foreclosure, code_violation.

    This is a best-effort extraction from top-level payload fields.
    Deep signal enrichment happens in Node 3 (Distress Enrichment Node).
    """
    signals: list[DistressSignal] = []

    if record.get("tax_delinquent"):
        signals.append(
            DistressSignal(
                signal_type="tax_delinquency",
                source="PropStream",
                severity=0.8,
                corroborated=False,
            )
        )
    if record.get("pre_foreclosure") or record.get("preforeclosure"):
        signals.append(
            DistressSignal(
                signal_type="preforeclosure",
                source="PropStream",
                severity=0.9,
                corroborated=False,
            )
        )
    if record.get("code_violation"):
        signals.append(
            DistressSignal(
                signal_type="code_violation",
                source="PropStream",
                severity=0.5,
                corroborated=False,
            )
        )

    return signals


# ---------------------------------------------------------------------------
# Main ingestion entry point
# ---------------------------------------------------------------------------

def process_ingestion(
    run_id: str,
    db_path: Optional[str | Path] = None,
) -> dict[str, int]:
    """
    Node 1 — Lead Source Node.

    Queries PropStream (with ATTOM fallback) for each target county,
    shields and deduplicates payloads, and persists new leads to SQLite.

    Returns a metrics dict with keys:
      - leads_attempted: total records received from APIs
      - leads_ingested:  successfully persisted new leads
      - leads_skipped_dedup: suppressed by within-run or cross-run dedup
      - leads_skipped_validation: dropped by Pydantic ingestion shield
      - leads_skipped_geography: dropped for county/type mismatch
    """
    settings = get_settings()
    target_counties: list[str] = settings.TARGET_COUNTIES
    allowed_counties: set[str] = set(settings.TARGET_COUNTIES)

    local_seen: set[tuple[str, str]] = set()
    metrics: dict[str, int] = {
        "leads_attempted": 0,
        "leads_ingested": 0,
        "leads_skipped_dedup": 0,
        "leads_skipped_validation": 0,
        "leads_skipped_geography": 0,
    }

    for county in target_counties:
        if metrics["leads_ingested"] >= settings.MAX_LEADS_PER_RUN:
            break

        # --- Fetch from PropStream, fall back to ATTOM on failure ---
        records: list[dict[str, Any]] = []
        source_name = "PropStream"

        try:
            records = fetch_propstream_data_mock(county)
        except requests.RequestException as e:
            logger.error(
                "PropStream fetch failed for county=%s: %s. Attempting ATTOM fallback.",
                county, e,
            )
            source_name = "ATTOM"
            try:
                records = fetch_attom_data(county)
            except requests.RequestException as e2:
                err = PipelineError(
                    timestamp=datetime.now(UTC).isoformat(),
                    node="node_01_ingestion",
                    error_code="SOURCE_UNAVAILABLE",
                    message=f"Both PropStream and ATTOM failed for county={county}: {e2}",
                    is_fatal=False,
                )
                logger.error(err.model_dump_json())
                continue
        except ValueError as e:
            err = PipelineError(
                timestamp=datetime.now(UTC).isoformat(),
                node="node_01_ingestion",
                error_code="SOURCE_UNAVAILABLE",
                message=f"PropStream returned unexpected envelope for county={county}: {e}",
                is_fatal=False,
            )
            logger.error(err.model_dump_json())
            continue

        # --- Process each record ---
        for record in records:
            if metrics["leads_ingested"] >= settings.MAX_LEADS_PER_RUN:
                err = PipelineError(
                    timestamp=datetime.now(UTC).isoformat(),
                    node="node_01_ingestion",
                    error_code="RUN_LIMIT_REACHED",
                    message=(
                        f"Hard cap of {settings.MAX_LEADS_PER_RUN} leads reached. "
                        "Halting ingestion."
                    ),
                    is_fatal=False,
                )
                logger.info(err.model_dump_json())
                break

            metrics["leads_attempted"] += 1

            # 1. Ingestion Shield
            try:
                payload = PropStreamPayload.model_validate(record)
            except ValidationError as e:
                err = PipelineError(
                    timestamp=datetime.now(UTC).isoformat(),
                    node="node_01_ingestion",
                    error_code="API_PAYLOAD_MISMATCH",
                    message=f"PropStreamPayload validation failed: {e}",
                    is_fatal=False,
                )
                logger.warning(err.model_dump_json())
                metrics["leads_skipped_validation"] += 1
                continue

            apn: str = payload.apn
            county_name: str = payload.county

            # 2. Geography guard — reject outside target counties immediately
            if county_name not in allowed_counties:
                logger.debug(
                    "Skipping APN=%s — county '%s' not in target counties.",
                    apn, county_name,
                )
                metrics["leads_skipped_geography"] += 1
                continue

            # 3. Within-run deduplication
            dedup_key = (apn, county_name)
            if dedup_key in local_seen:
                metrics["leads_skipped_dedup"] += 1
                continue
            local_seen.add(dedup_key)

            # 4. Cross-run deduplication (SPEC.md Section 5.4)
            recent_lead = find_recent_lead_by_apn(
                apn=apn,
                county=county_name,
                lookback_days=settings.CROSS_RUN_LOOKBACK_DAYS,
            )
            if recent_lead is not None and recent_lead.run_id != run_id:
                if recent_lead.pipeline_stage in ("published", "rejected"):
                    logger.info(
                        "Skipping APN=%s — recently reached terminal stage '%s' in run %s.",
                        apn, recent_lead.pipeline_stage, recent_lead.run_id,
                    )
                    metrics["leads_skipped_dedup"] += 1
                    continue
                # Allow reprocessing across runs for: error, underwritten, normalized, enriched
                logger.info(
                    "Reprocessing APN=%s — prior run %s stage was '%s'.",
                    apn, recent_lead.run_id, recent_lead.pipeline_stage,
                )

            # 5. Extract initial distress signals from raw record
            distress_signals = _extract_distress_signals(record)

            # 6. State instantiation
            property_id = str(uuid.uuid4())
            now_iso = datetime.now(UTC).isoformat()

            state = JCMPropertyState(
                run_id=run_id,
                property_id=property_id,
                pipeline_stage="sourced",
                county=county_name,  # type: ignore[arg-type]
                apn=apn,
                raw_address=payload.raw_address,
                asking_price=payload.asking_price,
                building_sqft=payload.building_sqft,
                year_built=payload.year_built,
                beds=payload.beds,
                baths=payload.baths,
                distress_signals=distress_signals,
                data_sources=[source_name],
                audit_log=[
                    AuditEntry(
                        timestamp=now_iso,
                        node="node_01_ingestion",
                        rationale="Instantiated state from API payload"
                    )
                ]
            )

            # 7. Persistence
            upsert_lead(state)
            metrics["leads_ingested"] += 1
            logger.info("Successfully ingested and persisted lead %s (APN: %s).", property_id, apn)

    return metrics
