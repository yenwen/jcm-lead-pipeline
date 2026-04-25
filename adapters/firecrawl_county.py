# adapters/firecrawl_county.py
"""
Adapter: Firecrawl County Tax Records -- Santa Clara County

Extracts delinquent tax records from the Santa Clara County tax sale page
via the Firecrawl extract API, maps each record into a JCMPropertyState
at the "sourced" stage with a pre-attached tax_delinquency DistressSignal.

If FIRECRAWL_API_KEY is not set, falls back to mock data so the pipeline
can run end-to-end without external credentials.

Environment variables:
    FIRECRAWL_API_KEY  -- Required for live runs. Falls back to mock if unset.

Implementation notes for the current JCM schema/database:
    - JCMPropertyState and DistressSignal fields match core/models.py exactly.
    - upsert_lead() in core/database.py returns None, so we do not branch on
      an "inserted"/"updated" return value.
    - The leads table is keyed by (run_id, property_id), so deterministic
      property_id alone does NOT prevent cross-run duplicates. To guard
      against duplicate county rows across runs, this adapter performs a
      recent-history lookup by APN + county before inserting a new row
      via find_recent_lead_by_apn() from core.database.
    - Firecrawl extract is handled as an async job: create extract job,
      poll status endpoint, then read structured data when completed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import time
from datetime import UTC, datetime, timedelta
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core.database import (
    find_recent_lead_by_apn,
    get_lead,
    init_db,
    insert_run,
    update_run_status,
    upsert_lead,
)
from core.models import AuditEntry, DistressSignal, JCMPropertyState, PipelineError

from adapters.common import (
    make_metrics,
    start_run as _start_run,
    complete_run as _complete_run,
    fail_run as _fail_run,
    persist_lead as _persist_lead,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ADAPTER_NAME = "adapter_firecrawl"
NODE_NAME = "adapter_firecrawl_county"

FIRECRAWL_CREATE_EXTRACT_URL = "https://api.firecrawl.dev/v1/extract"
FIRECRAWL_STATUS_URL_PREFIX = "https://api.firecrawl.dev/v1/extract"

# Target URLs for Santa Clara County tax sale data
# Primary: County DTAC publications page with tax-defaulted property notices
# Secondary: Bid4Assets auction platform where actual sales happen
COUNTY_TAX_SALE_URLS = [
    "https://dtac.sccgov.org/tax-information/publications",
    "https://www.bid4assets.com/auction/santaclara",
]
COUNTY_TAX_SALE_URL = COUNTY_TAX_SALE_URLS[0]  # backward compat

COUNTY = "Santa Clara"

# Assessor real-property search page — user pastes APN manually
ASSESSOR_LOOKUP_URL = "https://www.sccassessor.org/online-services/property-search/real-property"

# ---------------------------------------------------------------------------
# APN book-prefix → city mapping for Santa Clara County
# ---------------------------------------------------------------------------
APN_CITY_MAP: dict[str, str] = {
    "070": "San Jose",   "161": "San Jose",   "167": "San Jose",
    "336": "Santa Clara", "381": "San Jose",   "412": "San Jose",
    "486": "San Jose",   "497": "San Jose",   "544": "San Jose",
    "558": "San Jose",   "562": "San Jose",   "587": "San Jose",
    "589": "San Jose",   "241": "Sunnyvale",  "249": "Sunnyvale",
    "654": "Gilroy",     "659": "Gilroy",     "684": "Morgan Hill",
    "690": "Morgan Hill","726": "Campbell",   "742": "Los Gatos",
    "779": "Cupertino",  "835": "Milpitas",   "865": "Mountain View",
    "484": "San Jose",   "383": "San Jose",   "264": "Sunnyvale",
}


def city_from_apn(apn: str) -> str:
    """Infer city from the first 3 digits (book number) of an APN."""
    prefix = apn[:3] if len(apn) >= 3 else ""
    return APN_CITY_MAP.get(prefix, "")


# ---------------------------------------------------------------------------
# Property type mapping (use code → PropertyType enum)
# ---------------------------------------------------------------------------
_USE_CODE_MAP: dict[str, str] = {
    "sfr": "SFR", "single family": "SFR", "residential": "SFR",
    "single-family": "SFR", "single family residence": "SFR",
    "duplex": "Duplex", "2-unit": "Duplex", "two-unit": "Duplex",
    "triplex": "Triplex", "3-unit": "Triplex", "three-unit": "Triplex",
    "fourplex": "Fourplex", "4-unit": "Fourplex", "four-unit": "Fourplex",
    "condo": "Condo", "condominium": "Condo",
    "townhome": "Townhome", "townhouse": "Townhome",
}


def infer_property_type(raw: Any) -> str | None:
    """Map a use-code / property-type string to a PropertyType enum value."""
    if not raw:
        return None
    key = str(raw).strip().lower()
    return _USE_CODE_MAP.get(key)


# Firecrawl extraction schema + prompt
EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "delinquent_properties": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "apn": {"type": "string", "description": "Assessor Parcel Number"},
                    "address": {"type": "string", "description": "Full property address including city"},
                    "tax_due": {
                        "type": ["number", "string", "null"],
                        "description": "Total delinquent tax amount in USD",
                    },
                    "owner_name": {"type": "string", "description": "Property owner name"},
                    "tax_year": {"type": "string", "description": "Tax year(s) delinquent"},
                    "property_type": {
                        "type": "string",
                        "description": "Property use type: SFR, Duplex, Triplex, Fourplex, Condo, Townhome, or Residential",
                    },
                },
                "required": ["apn", "address"],
            },
        }
    },
}

EXTRACT_PROMPT = (
    "Find delinquent or tax-defaulted property records in Santa Clara County, California. "
    "Search for properties with unpaid property taxes, tax liens, or scheduled for tax sale auction. "
    "For each property found, extract the APN (assessor parcel number), full street address including city, "
    "total tax amount due, property owner name, the delinquent tax year(s), "
    "and the property type (SFR, Duplex, Triplex, Fourplex, Condo, or Townhome). "
    "Include properties from county tax sale notices, Bid4Assets listings, "
    "or any public records of tax-defaulted real estate in Santa Clara County."
)

# Severity scaling cap -- tax liens at or above this amount receive severity 1.0
TAX_SEVERITY_CAP = 25_000.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(UTC).isoformat()


def stable_property_id_from_apn(apn: str, county: str = COUNTY) -> str:
    """
    Derive a deterministic 32-char hex ID from APN + county.
    """
    key = f"{county}:{apn}".lower().strip()
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


def clean_apn(raw: Any) -> Optional[str]:
    """
    Normalize an APN by stripping whitespace and hyphens, uppercasing.

    Examples:
        "264-16-027"  ->  "26416027"
        " 415 29 038" ->  "41529038"
    """
    if raw is None:
        return None
    cleaned = re.sub(r"[\s\-]+", "", str(raw).strip()).upper()
    return cleaned if cleaned else None


def extract_tax_due(raw: Any) -> Optional[float]:
    """
    Normalize tax_due into a float.

    Accepts:
        18750
        18750.0
        "$18,750.00"
        "18,750"
        None
    """
    if raw is None:
        return None

    if isinstance(raw, (int, float)):
        value = float(raw)
        return value if value > 0 else None

    text = str(raw).strip()
    if not text:
        return None

    cleaned = re.sub(r"[^\d.]+", "", text)
    if not cleaned:
        return None

    try:
        value = float(cleaned)
        return value if value > 0 else None
    except (ValueError, OverflowError):
        return None


def compute_tax_severity(tax_due: Optional[float]) -> float:
    """
    Scale DistressSignal severity 0.3 - 1.0 based on delinquency amount.
    """
    if not tax_due or tax_due <= 0:
        return 0.3
    if tax_due >= TAX_SEVERITY_CAP:
        return 1.0
    return round(0.3 + (tax_due / TAX_SEVERITY_CAP) * 0.7, 2)


# ---------------------------------------------------------------------------
# HTTP session with retry / backoff
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    """
    Return a requests.Session with exponential backoff retry.
    Retries on both POST (create extract) and GET (poll status).
    """
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist={429, 500, 502, 503, 504},
        allowed_methods={"POST", "GET"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# ---------------------------------------------------------------------------
# Firecrawl fetching -- live API
# ---------------------------------------------------------------------------

def _create_extract_job(session: requests.Session, api_key: str) -> str:
    """
    Create a Firecrawl extract job and return its job ID.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "urls": COUNTY_TAX_SALE_URLS,
        "prompt": EXTRACT_PROMPT,
        "schema": EXTRACT_SCHEMA,
        "enableWebSearch": True,
        "showSources": True,
        "ignoreInvalidURLs": True,
    }

    logger.info("Creating Firecrawl extract job for %s", COUNTY_TAX_SALE_URL)
    response = session.post(
        FIRECRAWL_CREATE_EXTRACT_URL,
        headers=headers,
        json=payload,
        timeout=60.0,
    )
    response.raise_for_status()

    result = response.json()
    if not result.get("success"):
        raise ValueError(f"Firecrawl extract creation failed: {result}")

    job_id = result.get("id")
    if not job_id:
        raise ValueError(f"Firecrawl extract response missing job id: {result}")

    return str(job_id)


def _poll_extract_job(
    session: requests.Session,
    api_key: str,
    job_id: str,
    poll_interval_seconds: float = 2.0,
    max_wait_seconds: float = 90.0,
) -> dict[str, Any]:
    """
    Poll Firecrawl extract status until completed/failed/cancelled or timeout.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
    }
    status_url = f"{FIRECRAWL_STATUS_URL_PREFIX}/{job_id}"
    deadline = time.monotonic() + max_wait_seconds

    while time.monotonic() < deadline:
        response = session.get(status_url, headers=headers, timeout=30.0)
        response.raise_for_status()

        result = response.json()
        if not result.get("success"):
            raise ValueError(f"Firecrawl extract status check failed: {result}")

        status = str(result.get("status") or "").lower()
        if status == "completed":
            return result

        if status in {"failed", "cancelled"}:
            raise ValueError(
                f"Firecrawl extract job ended with status={status}: {result}"
            )

        time.sleep(poll_interval_seconds)

    raise TimeoutError(
        f"Firecrawl extract job {job_id} did not complete "
        f"within {max_wait_seconds} seconds"
    )


def fetch_firecrawl_data(api_key: str) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Create and poll a Firecrawl extract job, then return extracted tax records
    and the list of source URLs that Firecrawl scraped.

    Returns:
        (records, source_urls) — records are the parsed property dicts,
        source_urls are the actual pages Firecrawl crawled.
    """
    session = _build_session()
    job_id = _create_extract_job(session=session, api_key=api_key)
    result = _poll_extract_job(session=session, api_key=api_key, job_id=job_id)

    # Debug: dump raw response structure so we can verify parsing
    import json as _json
    logger.info("Firecrawl raw response keys: %s", list(result.keys()))
    data = result.get("data", {})
    logger.info("Firecrawl data type: %s, keys: %s",
                type(data).__name__,
                list(data.keys()) if isinstance(data, dict) else "N/A")
    logger.debug("Firecrawl raw data: %s", _json.dumps(data, indent=2, default=str)[:5000])

    # Extract source URLs — the actual pages Firecrawl crawled
    raw_sources = result.get("sources", [])
    source_urls: list[str] = []
    if isinstance(raw_sources, list):
        for src in raw_sources:
            if isinstance(src, str):
                source_urls.append(src)
            elif isinstance(src, dict):
                u = src.get("url") or src.get("source") or ""
                if u:
                    source_urls.append(str(u))
    # If Firecrawl didn't return explicit sources, fall back to the target
    # URLs we sent — those are the pages we asked it to crawl.
    if not source_urls:
        source_urls = list(COUNTY_TAX_SALE_URLS)
    logger.info("Firecrawl source URLs: %s", source_urls)

    # Try multiple extraction paths
    records: list[dict[str, Any]] = []
    if isinstance(data, dict):
        records = data.get("delinquent_properties", [])
        if not records:
            # Firecrawl may return the data directly under "data" as a list
            for key, val in data.items():
                if isinstance(val, list) and val:
                    logger.info("Found data under key '%s' with %d items", key, len(val))
                    records = val
                    break
    elif isinstance(data, list):
        records = data

    if not isinstance(records, list):
        records = []

    logger.info("Firecrawl returned %d tax records.", len(records))
    return records, source_urls


# ---------------------------------------------------------------------------
# Mock fallback
# ---------------------------------------------------------------------------

def fetch_mock_firecrawl_data() -> list[dict[str, Any]]:
    """
    Mock Santa Clara County delinquent tax records for local testing.
    """
    logger.info("FIRECRAWL_API_KEY not set -- using mock county tax records.")
    return [
        {
            "apn": "264-16-027",
            "address": "645 Meridian Ave, San Jose, CA 95126",
            "tax_due": 18750.00,
            "owner_name": "ESTATE OF J. MARTINEZ",
            "tax_year": "2023-2024",
        },
        {
            "apn": "415-29-038",
            "address": "3280 Cabrillo Ave, Santa Clara, CA 95051",
            "tax_due": 12400.00,
            "owner_name": "NGUYEN FAMILY TRUST",
            "tax_year": "2022-2024",
        },
        {
            "apn": "467-33-011",
            "address": "1190 Branham Ln, San Jose, CA 95118",
            "tax_due": 31200.00,
            "owner_name": "SILVA, ROBERT A",
            "tax_year": "2021-2024",
        },
    ]


# ---------------------------------------------------------------------------
# Cross-run duplicate guard
# ---------------------------------------------------------------------------

def should_skip_existing_apn(existing: JCMPropertyState) -> bool:
    """
    Decide whether a previously seen APN should block a new insert.

    Policy:
        - Skip duplicates for recent APN+county matches in any non-error stage.
        - Allow re-ingestion only when the prior attempt ended in "error".
    """
    return existing.pipeline_stage != "error"


# ---------------------------------------------------------------------------
# Mapping: Firecrawl record -> JCMPropertyState
# ---------------------------------------------------------------------------

def map_record_to_state(
    record: dict[str, Any],
    run_id: str,
    county: str = COUNTY,
    source_urls: list[str] | None = None,
) -> JCMPropertyState:
    """
    Map a parsed Firecrawl tax record into a JCMPropertyState.

    A tax_delinquency DistressSignal is pre-attached so Node 3 has an
    immediate distress signal without re-scraping.

    Improvements applied at ingestion time:
        - City inferred from APN book prefix
        - Property type mapped from Firecrawl use_code
        - Signal marked corroborated (official county source)
        - Owner name preserved in audit_log rationale (not in raw_address)

    Args:
        source_urls: The actual page URLs Firecrawl scraped to find this record.
                     Stored first in data_sources so the dashboard link goes
                     to the original source page.
    """
    # NOTE: Distress scoring is handled by Node 3 downstream.
    # The adapter attaches the DistressSignal; Node 3 computes the score.

    now_iso = utc_now_iso()

    raw_apn = record.get("apn")
    apn = clean_apn(raw_apn)
    if not apn:
        raise ValueError(f"Missing or invalid APN: {raw_apn!r}")

    address    = str(record.get("address") or "").strip()
    tax_due    = extract_tax_due(record.get("tax_due"))
    owner_name = str(record.get("owner_name") or "").strip()
    tax_year   = str(record.get("tax_year") or "").strip()

    property_id = stable_property_id_from_apn(apn, county)

    # --- Fix #4: Infer city from APN book prefix ---
    city = city_from_apn(apn)

    # --- Fix #5: Property type from Firecrawl schema ---
    prop_type = infer_property_type(record.get("property_type"))

    # raw_address stays address-like — no owner metadata appended.
    # Owner name is preserved in the audit_log rationale for downstream visibility.
    has_real_address = bool(address) and bool(re.search(r"\d", address))
    if has_real_address:
        raw_address = address
    else:
        raw_address = f"APN {apn}, {city or county + ' County'}, CA"

    severity = compute_tax_severity(tax_due)
    tax_note = f"tax_due=${tax_due:,.2f}" if tax_due is not None else "tax_due=unknown"

    # --- Fix #2: Corroborated = True for official county tax collector data ---
    tax_signal = DistressSignal(
        signal_type="tax_delinquency",
        source=f"{county} County Tax Collector",
        severity=severity,
        signal_date=now_iso,
        corroborated=True,
    )

    rationale = (
        f"Ingested from {county} County tax records via Firecrawl. "
        f"APN={apn} | {tax_note} | tax_year={tax_year!r} | owner={owner_name!r}"
    )

    audit_entry = AuditEntry(
        timestamp=now_iso,
        node=NODE_NAME,
        field_mutated="pipeline_stage",
        value_before=None,
        value_after="sourced",
        rationale=rationale,
    )

    # --- Fix #1: Link to real property search page (no APN formatting needed) ---
    assessor_url = ASSESSOR_LOOKUP_URL

    # data_sources: source page URLs first (for dashboard link), then assessor lookup
    sources: list[str] = [ADAPTER_NAME]
    if source_urls:
        sources.extend(source_urls)
    sources.append(assessor_url)

    state = JCMPropertyState(
        run_id=run_id,
        property_id=property_id,
        pipeline_stage="sourced",
        county=county,
        city=city or None,
        property_type=prop_type,
        apn=apn,
        raw_address=raw_address,
        distress_signals=[tax_signal],
        data_sources=sources,
        audit_log=[audit_entry],
    )

    return state




# ---------------------------------------------------------------------------
# Main adapter entry point
# ---------------------------------------------------------------------------

def run_firecrawl_county_adapter(
    run_id: Optional[str] = None,
    dry_run: bool = False,
    max_items: Optional[int] = None,
    dedupe_lookback_days: int = 30,
) -> dict[str, int]:
    """
    Execute the Firecrawl County Tax Records adapter end-to-end.

    Args:
        run_id:               Optional explicit run identifier.
        dry_run:              Parse and map all records but skip all DB writes.
        max_items:            Cap the number of records processed.
        dedupe_lookback_days: Window for recent duplicate detection by APN+county.

    Returns:
        A metrics dict with ingestion counts.
    """
    if run_id is None:
        run_id = f"{ADAPTER_NAME}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"

    metrics = make_metrics(leads_already_existed=0)

    if dry_run:
        logger.info("DRY RUN mode -- no database writes will occur.")

    _start_run(run_id, notes="Firecrawl county tax records adapter run", dry_run=dry_run)

    logger.info("Adapter run: %s", run_id)

    try:
        api_key = os.environ.get("FIRECRAWL_API_KEY", "").strip()

        # ----------------------------------------------------------------
        # Fetch
        # ----------------------------------------------------------------
        try:
            if api_key:
                records, source_urls = fetch_firecrawl_data(api_key)
            else:
                records = fetch_mock_firecrawl_data()
                source_urls = []  # mock data has no real source pages
        except (requests.RequestException, ValueError, TimeoutError) as exc:
            logger.error(
                PipelineError(
                    timestamp=utc_now_iso(),
                    node=NODE_NAME,
                    error_code="FIRECRAWL_FETCH_FAILED",
                    message=f"Failed to fetch Firecrawl data: {exc}",
                    is_fatal=True,
                ).model_dump_json()
            )
            if not dry_run:
                update_run_status(
                    run_id=run_id,
                    status="failed",
                    completed_at=utc_now_iso(),
                    notes=f"Firecrawl fetch failed: {exc}",
                )
            return metrics

        if max_items is not None:
            records = records[:max_items]

        metrics["items_fetched"] = len(records)
        logger.info("Processing %d county tax records.", len(records))

        # ----------------------------------------------------------------
        # Map + persist
        # ----------------------------------------------------------------
        for record in records:
            try:
                state = map_record_to_state(record, run_id, COUNTY, source_urls=source_urls)
                metrics["leads_mapped"] += 1

                if dry_run:
                    metrics["leads_ingested_new"] += 1
                    severity = (
                        state.distress_signals[0].severity
                        if state.distress_signals
                        else "N/A"
                    )
                    logger.debug(
                        "[DRY RUN] Would ingest: property_id=%s apn=%s "
                        "raw_address=%r severity=%s",
                        state.property_id,
                        state.apn,
                        state.raw_address,
                        severity,
                    )
                    continue

                # Cross-run dedupe: skip if same APN+county seen recently in
                # a non-error stage from a different run.
                recent_existing = find_recent_lead_by_apn(
                    apn=state.apn or "",
                    county=COUNTY,
                    lookback_days=dedupe_lookback_days,
                )

                if (
                    recent_existing is not None
                    and recent_existing.run_id != run_id
                    and should_skip_existing_apn(recent_existing)
                ):
                    metrics["leads_already_existed"] += 1
                    logger.debug(
                        "Skipping duplicate tax record apn=%s "
                        "existing_stage=%s existing_run=%s",
                        state.apn,
                        recent_existing.pipeline_stage,
                        recent_existing.run_id,
                    )
                    continue

                # Intra-run dedupe: same APN appearing twice in one extract result.
                same_run_existing = get_lead(
                    run_id=run_id,
                    property_id=state.property_id,
                )

                upsert_lead(state)

                if same_run_existing is None:
                    metrics["leads_ingested_new"] += 1
                    logger.info(
                        "Upserted new tax lead: apn=%s address=%r severity=%s",
                        state.apn,
                        state.raw_address,
                        state.distress_signals[0].severity
                        if state.distress_signals
                        else "N/A",
                    )
                else:
                    metrics["leads_already_existed"] += 1
                    logger.debug(
                        "Intra-run duplicate refreshed: apn=%s", state.apn
                    )

            except Exception as exc:
                logger.warning(
                    PipelineError(
                        timestamp=utc_now_iso(),
                        node=NODE_NAME,
                        error_code="RECORD_MAPPING_FAILED",
                        message=(
                            f"Failed to map/persist tax record: {exc}. "
                            f"APN: {record.get('apn', 'N/A')}"
                        ),
                        is_fatal=False,
                    ).model_dump_json()
                )
                metrics["mapping_errors"] += 1
                continue

    except Exception as exc:
        logger.exception(
            "Firecrawl county adapter: unhandled exception in run %s", run_id
        )
        _fail_run(run_id, f"Unhandled exception: {exc}", dry_run=dry_run)
        raise

    _complete_run(run_id, metrics, dry_run=dry_run)

    logger.info("Firecrawl county adapter complete: %s", metrics)
    return metrics


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="JCM Adapter -- Firecrawl Santa Clara County Tax Records"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and map records but skip all database writes.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N records.",
    )
    parser.add_argument(
        "--dedupe-lookback-days",
        type=int,
        default=30,
        metavar="DAYS",
        help="Skip recently seen APN+county values from the last N days.",
    )
    args = parser.parse_args()

    print("\n" + "=" * 58)
    print("  JCM ADAPTER: Firecrawl County Tax Records")
    if args.dry_run:
        print("  *** DRY RUN -- no DB writes ***")
    print("=" * 58)

    result = run_firecrawl_county_adapter(
        dry_run=args.dry_run,
        max_items=args.max_items,
        dedupe_lookback_days=args.dedupe_lookback_days,
    )

    print("-" * 58)
    for key, val in result.items():
        label = key.replace("_", " ").title()
        print(f"  {label:<35} {val}")
    print("=" * 58 + "\n")
