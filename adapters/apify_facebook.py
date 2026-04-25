# adapters/apify_facebook.py
"""
Adapter: Apify Facebook Marketplace — Bay Area Real Estate

Fetches Facebook Marketplace real estate listings via the Apify API,
parses the JSON dataset, and maps each listing into a JCMPropertyState
at the "sourced" stage.

If APIFY_API_TOKEN is not set, falls back to mock data.
"""

import argparse
import hashlib
import logging
import os
import re
import uuid
from datetime import UTC, datetime
from typing import Any, Optional
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core.models import AuditEntry, JCMPropertyState, PipelineError
from core.settings import get_settings

from adapters.common import (
    generate_run_id,
    make_metrics,
    persist_lead,
    should_skip_lead,
    start_run,
    complete_run,
    fail_run,
)

logger = logging.getLogger(__name__)

ADAPTER_NAME = "adapter_apify_fb"
NODE_NAME = "adapter_apify_facebook"
APIFY_BASE_URL = "https://api.apify.com/v2"
APIFY_ACTOR_ID = os.environ.get("APIFY_ACTOR_ID", "apify/facebook-marketplace-scraper")
PRICE_RE = re.compile(r"\$?\s*([\d,]+(?:\.\d{1,2})?)")

# Regex to extract a street-address-like fragment from a location string
# e.g. "123 Main St, San Jose, CA" -> "123 Main St"
ADDRESS_FRAGMENT_RE = re.compile(
    r"\d+\s+[\w\s]+(?:St|Ave|Blvd|Dr|Rd|Ct|Ln|Way|Pl|Cir|Pkwy|Hwy)\b",
    re.IGNORECASE,
)

CITY_COUNTY_MAP: dict[str, str] = {
    "san jose": "Santa Clara", "sunnyvale": "Santa Clara", "santa clara": "Santa Clara",
    "mountain view": "Santa Clara", "cupertino": "Santa Clara", "palo alto": "Santa Clara",
    "milpitas": "Santa Clara", "los gatos": "Santa Clara", "saratoga": "Santa Clara",
    "campbell": "Santa Clara", "morgan hill": "Santa Clara", "gilroy": "Santa Clara",
    "oakland": "Alameda", "fremont": "Alameda", "berkeley": "Alameda", "hayward": "Alameda",
    "san leandro": "Alameda", "pleasanton": "Alameda", "dublin": "Alameda",
    "livermore": "Alameda", "union city": "Alameda", "newark": "Alameda",
    "alameda": "Alameda", "castro valley": "Alameda",
}

def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()

def normalize_actor_id(actor_id: str) -> str:
    actor_id = actor_id.strip()
    if not actor_id or "~" in actor_id: return actor_id
    if "/" in actor_id:
        owner, name = actor_id.split("/", 1)
        return f"{owner.strip()}~{name.strip()}"
    return actor_id

def encoded_actor_id(actor_id: str) -> str:
    return quote(normalize_actor_id(actor_id), safe="~")

def stable_property_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]

def extract_price(raw: Any) -> Optional[float]:
    if raw is None: return None
    if isinstance(raw, (int, float)): return float(raw) if raw > 0 else None
    match = PRICE_RE.search(str(raw).strip())
    if not match: return None
    try:
        price = float(match.group(1).replace(",", ""))
        return price if price > 0 else None
    except (ValueError, OverflowError):
        return None

def infer_city_and_county(location: str) -> tuple[Optional[str], Optional[str]]:
    if not location: return None, None
    location_lower = location.lower().strip()
    for city_key, county in CITY_COUNTY_MAP.items():
        if city_key in location_lower:
            return city_key.title(), county
    parts = [p.strip() for p in location.split(",") if p.strip()]
    if parts: return parts[0].title(), None
    return None, None

def extract_raw_address(location: str) -> Optional[str]:
    if not location: return None
    match = ADDRESS_FRAGMENT_RE.search(location)
    if match: return match.group(0).strip(" ,;|-")
    return None

def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist={429, 500, 502, 503, 504})
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_apify_data(api_token: str) -> list[dict[str, Any]]:
    actor_id = encoded_actor_id(APIFY_ACTOR_ID)
    url = f"{APIFY_BASE_URL}/acts/{actor_id}/runs/last/dataset/items"
    params = {"token": api_token, "status": "SUCCEEDED", "format": "json", "clean": "1"}
    
    session = _build_session()
    response = session.get(url, params=params, timeout=30.0)
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else []

def fetch_mock_apify_data() -> list[dict[str, Any]]:
    logger.info("APIFY_API_TOKEN not set -- using mock Facebook Marketplace data.")
    return [
        {
            "title": "3BR/2BA Fixer-Upper -- Great Investment Opportunity",
            "price": "$685,000",
            "url": "https://www.facebook.com/marketplace/item/1001001001",
            "location": "San Jose, CA",
            "description": "Investor special! Needs TLC -- roof and paint. Tax delinquent, motivated seller."
        },
        {
            "title": "Oakland Duplex -- Off Market Deal",
            "price": "$520,000",
            "url": "https://www.facebook.com/marketplace/item/2002002002",
            "location": "Oakland, CA",
            "description": "Off-market duplex in East Oakland. Both units occupied. Pre-foreclosure."
        }
    ]


# ---------------------------------------------------------------------------
# Main adapter entry point
# ---------------------------------------------------------------------------

def run_apify_facebook_adapter(
    dry_run: bool = False,
    max_items: Optional[int] = None,
) -> dict[str, int]:
    """
    Execute the Apify Facebook Marketplace adapter end-to-end.

    Dry-run contract:
        - Fetch + parse + map all run normally
        - Dedupe evaluated via read-only DB lookups
        - Zero DB writes (no init_db, insert_run, upsert_lead)
        - leads_ingested_new reflects "would ingest" count
    """
    run_id = generate_run_id("adapter_fb")
    metrics = make_metrics()

    start_run(run_id, notes="Apify Facebook Marketplace adapter run", dry_run=dry_run)

    # --- Fetch ---
    try:
        api_token = os.environ.get("APIFY_API_TOKEN", "").strip()
        items = fetch_apify_data(api_token) if api_token else fetch_mock_apify_data()
    except Exception as exc:
        logger.exception("Apify fetch failed: %s", exc)
        metrics["fetch_errors"] += 1
        fail_run(run_id, f"Fetch failed: {exc}", dry_run=dry_run)
        return metrics

    if max_items is not None:
        items = items[:max_items]
    metrics["items_fetched"] = len(items)

    # --- Map + persist ---
    seen_this_run: set[str] = set()

    for item in items:
        try:
            url = str(item.get("url") or "").strip()
            property_id = stable_property_id(url) if url else str(uuid.uuid4())

            # Dedupe: read-only check (works in dry-run too)
            if should_skip_lead(property_id, run_id, seen_this_run=seen_this_run):
                metrics["duplicates_skipped"] += 1
                continue

            title = str(item.get("title") or "").strip()
            location = str(item.get("location") or "").strip()
            asking_price = extract_price(item.get("price"))
            city, county = infer_city_and_county(location)
            raw_address = extract_raw_address(location)
            now_iso = utc_now_iso()

            state = JCMPropertyState(
                run_id=run_id,
                property_id=property_id,
                pipeline_stage="sourced",
                county=county,  # type: ignore
                city=city,
                raw_address=raw_address or title, # Use title as fallback if no street address
                asking_price=asking_price,
                data_sources=[ADAPTER_NAME, url] if url else [ADAPTER_NAME],
                audit_log=[
                    AuditEntry(
                        timestamp=now_iso, node=NODE_NAME, field_mutated="pipeline_stage",
                        value_before=None, value_after="sourced",
                        rationale="Ingested from Apify Facebook Marketplace adapter"
                    )
                ]
            )

            metrics["leads_mapped"] += 1
            persist_lead(state, dry_run=dry_run)
            metrics["leads_ingested_new"] += 1

        except Exception as exc:
            logger.warning("Failed to map FB item: %s", exc)
            metrics["mapping_errors"] += 1

    # --- Finalize run ---
    try:
        complete_run(run_id, metrics, dry_run=dry_run)
    except Exception as exc:
        logger.warning("Failed to mark run complete: %s", exc)

    return metrics


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="JCM Adapter -- Apify FB")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-items", type=int, default=None)
    args = parser.parse_args()

    print("\n" + "=" * 58)
    print("  JCM ADAPTER: Apify Facebook Marketplace")
    if args.dry_run:
        print("  *** DRY RUN — no DB writes ***")
    print("=" * 58)
    metrics = run_apify_facebook_adapter(dry_run=args.dry_run, max_items=args.max_items)
    print("-" * 58)
    for key, val in metrics.items():
        label = key.replace("_", " ").title()
        print(f"  {label:<35} {val}")
    print("=" * 58 + "\n")
