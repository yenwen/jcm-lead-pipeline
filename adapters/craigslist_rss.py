# adapters/craigslist_rss.py
"""
Adapter: Craigslist — South Bay Real Estate

Primary engine: Firecrawl Extract API (bypasses Craigslist's bot blocking).
Fallback: Direct RSS fetch (may fail with 403).
Last resort: Mock data for development/testing.

Uses the same Firecrawl Extract API pattern as firecrawl_county.py but
targets Craigslist search results instead of county tax records.
"""

import argparse
import hashlib
import html
import json
import logging
import os
import re
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional, Any

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

ADAPTER_NAME = "adapter_craigslist"
NODE_NAME = "adapter_craigslist_rss"

# ---------------------------------------------------------------------------
# Craigslist target URLs
# ---------------------------------------------------------------------------
CRAIGSLIST_SEARCH_URL = "https://sfbay.craigslist.org/search/sby/rea"
CRAIGSLIST_RSS_URL = f"{CRAIGSLIST_SEARCH_URL}?format=rss"
DC_NS = "http://purl.org/dc/elements/1.1/"

# ---------------------------------------------------------------------------
# Firecrawl extraction schema for Craigslist listings
# ---------------------------------------------------------------------------
FIRECRAWL_CREATE_URL = "https://api.firecrawl.dev/v1/extract"
FIRECRAWL_STATUS_PREFIX = "https://api.firecrawl.dev/v1/extract"

CL_EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "listings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string", "description": "Listing title"},
                    "price": {
                        "type": ["number", "null"],
                        "description": "Asking price in USD",
                    },
                    "address": {
                        "type": "string",
                        "description": "Property address or neighborhood",
                    },
                    "link": {
                        "type": "string",
                        "description": "Full URL to the Craigslist listing",
                    },
                    "beds": {
                        "type": ["number", "null"],
                        "description": "Number of bedrooms",
                    },
                    "sqft": {
                        "type": ["integer", "null"],
                        "description": "Square footage",
                    },
                    "property_type": {
                        "type": "string",
                        "description": "Type: house, condo, townhouse, duplex, etc.",
                    },
                },
                "required": ["title"],
            },
        }
    },
}

CL_EXTRACT_PROMPT = (
    "Extract all real estate property listings from this Craigslist page. "
    "For each listing, get the title, asking price in USD, property address or "
    "neighborhood location, the full link/URL to the individual listing, "
    "number of bedrooms, square footage, and property type "
    "(house, condo, townhouse, duplex, apartment, etc.). "
    "Only include residential property listings for sale."
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PRICE_RE = re.compile(r"\$\s*([\d,]+(?:\.\d{1,2})?)")
HTML_TAG_RE = re.compile(r"<[^>]+>")


def _get_cache_file() -> Path:
    """Return cache path co-located with the database file so it survives deployments."""
    return get_settings().DATABASE_PATH.parent / ".craigslist_rss_cache.json"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def stable_property_id(link: str) -> str:
    """Derive a deterministic 32-char hex ID from the listing URL."""
    return hashlib.sha256(link.encode("utf-8")).hexdigest()[:32]


def extract_price_from_title(title: str) -> Optional[float]:
    match = PRICE_RE.search(title)
    if not match:
        return None
    try:
        price = float(match.group(1).replace(",", ""))
        return price if price > 0 else None
    except (ValueError, OverflowError):
        return None


def _safe_text(element: Optional[ET.Element]) -> Optional[str]:
    if element is None or element.text is None:
        return None
    text = element.text.strip()
    return text if text else None


def _strip_html(text: Optional[str]) -> str:
    if not text:
        return ""
    no_tags = HTML_TAG_RE.sub(" ", text)
    unescaped = html.unescape(no_tags)
    return re.sub(r"\s{2,}", " ", unescaped).strip()


_PROP_TYPE_MAP: dict[str, str] = {
    "house": "SFR", "single family": "SFR", "sfr": "SFR",
    "duplex": "Duplex", "triplex": "Triplex", "fourplex": "Fourplex",
    "condo": "Condo", "condominium": "Condo",
    "townhouse": "Townhome", "townhome": "Townhome",
}


def _map_property_type(raw: Any) -> Optional[str]:
    if not raw:
        return None
    return _PROP_TYPE_MAP.get(str(raw).strip().lower())


# ---------------------------------------------------------------------------
# Firecrawl-powered fetch
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist={429, 500, 502, 503, 504})
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_via_firecrawl(api_key: str) -> tuple[list[dict], list[str]]:
    """
    Use Firecrawl Extract API to pull structured listings from Craigslist.

    Returns:
        (listings, source_urls)
    """
    session = _build_session()

    # Create extract job
    payload = {
        "urls": [CRAIGSLIST_SEARCH_URL],
        "prompt": CL_EXTRACT_PROMPT,
        "schema": CL_EXTRACT_SCHEMA,
        "enableWebSearch": False,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    logger.info("Creating Firecrawl extract job for Craigslist: %s", CRAIGSLIST_SEARCH_URL)
    resp = session.post(FIRECRAWL_CREATE_URL, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    job = resp.json()

    job_id = job.get("id") or job.get("jobId") or ""
    if not job_id:
        # Synchronous response — data already available
        data = job.get("data", {})
        listings = data.get("listings", []) if isinstance(data, dict) else []
        return listings, [CRAIGSLIST_SEARCH_URL]

    # Poll for completion
    poll_url = f"{FIRECRAWL_STATUS_PREFIX}/{job_id}"
    logger.info("Polling Firecrawl job %s ...", job_id)

    for attempt in range(30):
        time.sleep(3)
        poll_resp = session.get(poll_url, headers=headers, timeout=15)
        poll_resp.raise_for_status()
        result = poll_resp.json()

        status = result.get("status", "")
        if status == "completed":
            data = result.get("data", {})
            listings = []
            if isinstance(data, dict):
                listings = data.get("listings", [])
                if not listings:
                    for key, val in data.items():
                        if isinstance(val, list) and val:
                            listings = val
                            break
            elif isinstance(data, list):
                listings = data

            # Extract source URLs
            source_urls = [CRAIGSLIST_SEARCH_URL]
            raw_sources = result.get("sources", [])
            for src in raw_sources:
                if isinstance(src, str) and src:
                    source_urls.append(src)
                elif isinstance(src, dict):
                    u = src.get("url") or src.get("source") or ""
                    if u:
                        source_urls.append(str(u))

            logger.info("Firecrawl returned %d Craigslist listings.", len(listings))
            return listings, source_urls

        if status == "failed":
            raise ValueError(f"Firecrawl job {job_id} failed: {result}")

    raise TimeoutError(f"Firecrawl job {job_id} timed out after 90s")


# ---------------------------------------------------------------------------
# RSS fallback (may get 403'd)
# ---------------------------------------------------------------------------

def fetch_rss(url: str = CRAIGSLIST_RSS_URL) -> Optional[str]:
    """Fetch RSS with ETag caching. Returns None on 304 or failure."""
    cache_file = _get_cache_file()
    cache = {}
    if cache_file.exists():
        try:
            cache = json.loads(cache_file.read_text())
        except Exception:
            logger.debug("Could not read Craigslist RSS ETag cache; proceeding without it.")

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.7",
    }
    if cache.get("etag"):
        headers["If-None-Match"] = cache["etag"]

    logger.info("Fetching Craigslist RSS from %s", url)
    session = _build_session()
    response = session.get(url, timeout=15.0, headers=headers)

    if response.status_code == 304:
        logger.info("RSS feed unchanged (HTTP 304). No new items.")
        return None
    response.raise_for_status()

    new_cache = {}
    if response.headers.get("ETag"):
        new_cache["etag"] = response.headers["ETag"]
    if new_cache:
        try:
            cache_file.write_text(json.dumps(new_cache, indent=2))
        except Exception:
            logger.debug("Could not write Craigslist RSS ETag cache; continuing without it.")

    return response.text


def parse_rss_items(xml_text: str) -> list[dict]:
    items = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return items

    channel = root.find("channel")
    if channel is None:
        return items

    for item_el in channel.findall("item"):
        title = _safe_text(item_el.find("title"))
        link = _safe_text(item_el.find("link"))
        description = _safe_text(item_el.find("description"))
        dc_date = _safe_text(item_el.find(f"{{{DC_NS}}}date"))
        if title:
            items.append({
                "title": title,
                "link": link,
                "description": description,
                "dc_date": dc_date,
            })
    return items


# ---------------------------------------------------------------------------
# Mock data fallback
# ---------------------------------------------------------------------------

def fetch_mock_craigslist_data() -> list[dict]:
    """Return mock Craigslist listings for development/testing."""
    logger.info("Using mock Craigslist listings (no API key, RSS blocked).")
    return [
        {
            "title": "$749,000 — 3BR/2BA Ranch in Cambrian Park",
            "price": 749000,
            "address": "1842 Hicks Ave, San Jose, CA 95124",
            "link": "https://sfbay.craigslist.org/sby/rea/mock-001",
            "beds": 3,
            "sqft": 1450,
            "property_type": "house",
        },
        {
            "title": "$1,199,000 — Spacious 4BR Near Downtown Sunnyvale",
            "price": 1199000,
            "address": "456 Carroll St, Sunnyvale, CA 94086",
            "link": "https://sfbay.craigslist.org/sby/rea/mock-002",
            "beds": 4,
            "sqft": 2100,
            "property_type": "house",
        },
        {
            "title": "$589,000 — Updated 2BR Condo in Santa Clara",
            "price": 589000,
            "address": "2901 Moorpark Ave #12, Santa Clara, CA 95051",
            "link": "https://sfbay.craigslist.org/sby/rea/mock-003",
            "beds": 2,
            "sqft": 980,
            "property_type": "condo",
        },
        {
            "title": "$925,000 — Charming Townhome Near Santana Row",
            "price": 925000,
            "address": "3350 Stevens Creek Blvd #7, San Jose, CA 95117",
            "link": "https://sfbay.craigslist.org/sby/rea/mock-004",
            "beds": 3,
            "sqft": 1600,
            "property_type": "townhouse",
        },
    ]


# ---------------------------------------------------------------------------
# Main adapter entry point
# ---------------------------------------------------------------------------

def run_craigslist_adapter(
    dry_run: bool = False,
    max_items: Optional[int] = None,
) -> dict[str, int]:
    """
    Execute the Craigslist adapter end-to-end.

    Fetch strategy (in order):
        1. Firecrawl Extract API (if FIRECRAWL_API_KEY is set)
        2. Direct RSS fetch (may fail with 403)
        3. Mock data fallback

    Dry-run contract:
        - Fetch + parse + map all run normally
        - Dedupe evaluated via read-only DB lookups
        - Zero DB writes (no init_db, insert_run, upsert_lead)
        - leads_ingested_new reflects "would ingest" count
    """
    run_id = generate_run_id("adapter_cl")
    metrics = make_metrics()

    start_run(run_id, notes="Craigslist adapter run", dry_run=dry_run)

    # --- Fetch listings ---
    items: list[dict] = []
    source_urls: list[str] = []
    source_method = "mock"

    api_key = os.environ.get("FIRECRAWL_API_KEY", "").strip()

    if api_key:
        # Strategy 1: Firecrawl
        try:
            items, source_urls = fetch_via_firecrawl(api_key)
            source_method = "firecrawl"
            logger.info("Firecrawl returned %d Craigslist listings.", len(items))
        except Exception as exc:
            logger.warning("Firecrawl fetch failed: %s — trying RSS fallback", exc)
            metrics["fetch_errors"] += 1
            items = []

    if not items:
        # Strategy 2: Direct RSS
        try:
            xml_text = fetch_rss()
            if xml_text:
                rss_items = parse_rss_items(xml_text)
                # Convert RSS items to the Firecrawl-style dict format
                items = [
                    {
                        "title": r.get("title", ""),
                        "link": r.get("link", ""),
                        "price": extract_price_from_title(r.get("title", "")),
                        "address": None,  # RSS titles are not real addresses
                    }
                    for r in rss_items
                ]
                source_urls = [CRAIGSLIST_RSS_URL]
                source_method = "rss"
                logger.info("RSS returned %d items.", len(items))
        except Exception as exc:
            logger.warning("RSS fetch also failed (%s) — falling back to mock data.", exc)
            metrics["fetch_errors"] += 1

    if not items:
        # Strategy 3: Mock data
        items = fetch_mock_craigslist_data()
        source_method = "mock"

    if max_items:
        items = items[:max_items]

    metrics["items_fetched"] = len(items)
    logger.info(
        "Processing %d Craigslist listings (source: %s).",
        len(items), source_method,
    )

    # --- Map + persist ---
    seen_this_run: set[str] = set()

    for item in items:
        try:
            title = item.get("title") or ""
            link = item.get("link") or ""
            price = item.get("price")
            address = item.get("address") or ""
            beds = item.get("beds")
            sqft = item.get("sqft")
            prop_type_raw = item.get("property_type")

            # Parse price from title if not provided directly
            if price is None and title:
                price = extract_price_from_title(title)
            if isinstance(price, str):
                price = extract_price_from_title(str(price))

            property_id = stable_property_id(link) if link else str(uuid.uuid4())

            # Dedupe: read-only check (works in dry-run too)
            if should_skip_lead(property_id, run_id, seen_this_run=seen_this_run):
                metrics["duplicates_skipped"] += 1
                continue

            now_iso = utc_now_iso()
            prop_type = _map_property_type(prop_type_raw)

            # Build data_sources with clickable link
            sources: list[str] = [ADAPTER_NAME]
            if source_urls:
                sources.extend(source_urls)
            if link:
                sources.append(link)

            # Use address if available, otherwise title
            raw_address = address if address and address != title else title

            state = JCMPropertyState(
                run_id=run_id,
                property_id=property_id,
                pipeline_stage="sourced",
                county="Santa Clara",
                raw_address=raw_address,
                asking_price=price if isinstance(price, (int, float)) else None,
                property_type=prop_type,
                beds=beds if isinstance(beds, (int, float)) else None,
                building_sqft=int(sqft) if sqft else None,
                data_sources=sources,
                audit_log=[
                    AuditEntry(
                        timestamp=now_iso,
                        node=NODE_NAME,
                        field_mutated="pipeline_stage",
                        value_before=None,
                        value_after="sourced",
                        rationale=(
                            f"Ingested from Craigslist via {source_method}. "
                            f"Title={title!r}"
                        ),
                    )
                ],
            )

            metrics["leads_mapped"] += 1
            persist_lead(state, dry_run=dry_run)
            metrics["leads_ingested_new"] += 1
            logger.info(
                "%sIngested CL lead: %s price=%s address=%r",
                "[DRY RUN] Would ingest — " if dry_run else "",
                property_id[:8], price, raw_address[:60],
            )

        except Exception as exc:
            logger.warning("Failed to map Craigslist item: %s", exc)
            metrics["mapping_errors"] += 1

    # --- Finalize run ---
    try:
        complete_run(run_id, metrics, dry_run=dry_run)
    except Exception as exc:
        logger.warning("Failed to mark run complete: %s", exc)

    return metrics


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="JCM Adapter — Craigslist South Bay (Firecrawl-powered)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Parse but skip DB writes.")
    parser.add_argument("--max-items", type=int, default=None, help="Process at most N items.")
    args = parser.parse_args()

    print("\n" + "=" * 58)
    print("  JCM ADAPTER: Craigslist South Bay Real Estate")
    if args.dry_run:
        print("  *** DRY RUN — no DB writes ***")
    print("=" * 58)
    metrics = run_craigslist_adapter(dry_run=args.dry_run, max_items=args.max_items)
    print("-" * 58)
    for key, val in metrics.items():
        label = key.replace("_", " ").title()
        print(f"  {label:<35} {val}")
    print("=" * 58 + "\n")
