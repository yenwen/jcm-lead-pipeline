# adapters/zillow_scrape.py
"""
Adapter: Zillow — South Bay Real Estate Listings (via Firecrawl)

Uses Firecrawl's /v1/scrape endpoint with extract mode to pull structured
property listings from Zillow search pages. Falls back to mock data when
FIRECRAWL_API_KEY is not set.

Search criteria (cities, price range, beds, property type) can be customized
via CLI args, environment variables, or the dashboard command center.
"""

import argparse
import hashlib
import json
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

from core.database import find_recent_lead_by_property_id
from core.models import AuditEntry, JCMPropertyState
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

ADAPTER_NAME = "adapter_zillow"
NODE_NAME = "adapter_zillow_scrape"

# ---------------------------------------------------------------------------
# Available cities for Zillow search
# ---------------------------------------------------------------------------
AVAILABLE_CITIES = [
    "San Jose",
    "Sunnyvale",
    "Santa Clara",
    "Campbell",
    "Milpitas",
    "Cupertino",
    "Mountain View",
    "Los Gatos",
    "Saratoga",
    "Gilroy",
    "Morgan Hill",
    "Palo Alto",
    "Fremont",
    "Oakland",
]

DEFAULT_CITIES = ["San Jose", "Sunnyvale"]


def _city_to_zillow_slug(city: str) -> str:
    """Convert a city name to a Zillow URL slug. e.g. 'San Jose' -> 'san-jose-ca'."""
    return city.strip().lower().replace(" ", "-") + "-ca"


def build_zillow_url(
    city: str,
    min_price: int | None = None,
    max_price: int | None = None,
    min_beds: int | None = None,
    property_types: list[str] | None = None,
) -> str:
    """
    Build a Zillow search URL with filters encoded in the path/query.

    Zillow supports path-based filters like:
        /san-jose-ca/3-_beds/500000-1500000_price/
    """
    slug = _city_to_zillow_slug(city)
    path_parts = [slug]

    # Beds filter: "3-_beds" means 3+ beds
    if min_beds and min_beds > 0:
        path_parts.append(f"{min_beds}-_beds")

    # Price filter: "500000-1500000_price" or "500000-_price" (min only)
    if min_price and max_price:
        path_parts.append(f"{min_price}-{max_price}_price")
    elif min_price:
        path_parts.append(f"{min_price}-_price")
    elif max_price:
        path_parts.append(f"0-{max_price}_price")

    # Property type filter via path
    type_slugs = {
        "SFR": "house", "Condo": "condo", "Townhome": "townhouse",
        "Duplex": "duplex", "Multi-Family": "multi-family",
    }
    if property_types and len(property_types) == 1:
        ztype = type_slugs.get(property_types[0])
        if ztype:
            path_parts.append(f"{ztype}_type")

    url = "https://www.zillow.com/" + "/".join(path_parts) + "/"
    return url


# ---------------------------------------------------------------------------
# Firecrawl scrape + extract config
# ---------------------------------------------------------------------------
FIRECRAWL_SCRAPE_URL = "https://api.firecrawl.dev/v1/scrape"

ZILLOW_EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "listings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "address": {
                        "type": "string",
                        "description": "Full property address including city and zip",
                    },
                    "price": {
                        "type": ["number", "null"],
                        "description": "Listing price in USD",
                    },
                    "beds": {
                        "type": ["number", "null"],
                        "description": "Number of bedrooms",
                    },
                    "baths": {
                        "type": ["number", "null"],
                        "description": "Number of bathrooms",
                    },
                    "sqft": {
                        "type": ["integer", "null"],
                        "description": "Building square footage",
                    },
                    "listing_url": {
                        "type": "string",
                        "description": "Full URL to the Zillow listing detail page",
                    },
                    "property_type": {
                        "type": "string",
                        "description": "Property type: house, condo, townhouse, multi-family",
                    },
                    "days_on_zillow": {
                        "type": ["integer", "null"],
                        "description": "Days the listing has been on Zillow",
                    },
                },
                "required": ["address"],
            },
        }
    },
}

ZILLOW_EXTRACT_PROMPT = (
    "Extract all property listings for sale from this Zillow search page. "
    "For each property, get the full address including city and zip code, "
    "listing price, number of bedrooms, number of bathrooms, square footage, "
    "the full Zillow listing URL, property type (house, condo, townhouse, etc.), "
    "and how many days it has been listed on Zillow."
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def stable_property_id(url_or_address: str) -> str:
    """Derive a deterministic 32-char hex ID."""
    return hashlib.sha256(url_or_address.encode("utf-8")).hexdigest()[:32]


_PROP_TYPE_MAP: dict[str, str] = {
    "house": "SFR", "single family": "SFR", "sfr": "SFR",
    "single-family": "SFR", "residential": "SFR",
    "duplex": "Duplex", "triplex": "Triplex", "fourplex": "Fourplex",
    "multi-family": "Duplex",
    "condo": "Condo", "condominium": "Condo",
    "townhouse": "Townhome", "townhome": "Townhome",
}


def _map_property_type(raw: Any) -> Optional[str]:
    if not raw:
        return None
    return _PROP_TYPE_MAP.get(str(raw).strip().lower())


def _extract_city(address: str) -> Optional[str]:
    """Extract city from Zillow address like '123 Main St, San Jose, CA 95125'."""
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 3:
        # "123 Main St, San Jose, CA 95125" → "San Jose"
        return parts[-2].strip()
    elif len(parts) == 2:
        # "San Jose, CA" → "San Jose" (parts[0], not parts[-1] which is "CA")
        return parts[0].strip()
    return None


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist={429, 500, 502, 503, 504})
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# ---------------------------------------------------------------------------
# Firecrawl fetch
# ---------------------------------------------------------------------------

def fetch_via_firecrawl(
    api_key: str,
    urls: list[str] | None = None,
) -> tuple[list[dict], list[str]]:
    """
    Use Firecrawl /v1/scrape with extract to get structured Zillow listings.

    Returns:
        (all_listings, source_urls)
    """
    if urls is None:
        urls = [build_zillow_url(c) for c in DEFAULT_CITIES]

    session = _build_session()
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    all_listings: list[dict] = []
    source_urls: list[str] = []

    for url in urls:
        logger.info("Scraping Zillow page: %s", url)
        payload = {
            "url": url,
            "formats": ["extract"],
            "extract": {
                "prompt": ZILLOW_EXTRACT_PROMPT,
                "schema": ZILLOW_EXTRACT_SCHEMA,
            },
        }

        try:
            resp = session.post(
                FIRECRAWL_SCRAPE_URL,
                json=payload,
                headers=headers,
                timeout=60,
            )
            resp.raise_for_status()
            result = resp.json()

            data = result.get("data", {})
            extract = data.get("extract", {})
            listings = extract.get("listings", []) if isinstance(extract, dict) else []

            logger.info("Zillow %s → %d listings extracted.", url, len(listings))
            all_listings.extend(listings)
            source_urls.append(url)

        except Exception as exc:
            logger.warning("Failed to scrape %s: %s", url, exc)

    return all_listings, source_urls


# ---------------------------------------------------------------------------
# Mock data
# ---------------------------------------------------------------------------

def fetch_mock_zillow_data() -> list[dict]:
    """Return mock Zillow listings for development/testing."""
    logger.info("Using mock Zillow listings (FIRECRAWL_API_KEY not set).")
    return [
        {
            "address": "711 Chatsworth Pl, San Jose, CA 95128",
            "price": 1749000, "beds": 4, "baths": 2, "sqft": 1823,
            "listing_url": "https://www.zillow.com/homedetails/711-Chatsworth-Pl-San-Jose-CA-95128/19590325_zpid/",
            "property_type": "house",
        },
        {
            "address": "1060 Shandwick Ct, San Jose, CA 95136",
            "price": 1799999, "beds": 4, "baths": 3, "sqft": 1860,
            "listing_url": "https://www.zillow.com/homedetails/1060-Shandwick-Ct-San-Jose-CA-95136/19703566_zpid/",
            "property_type": "house",
        },
        {
            "address": "3133 Acorn Ct, San Jose, CA 95117",
            "price": 1398000, "beds": 3, "baths": 3, "sqft": 1587,
            "listing_url": "https://www.zillow.com/homedetails/3133-Acorn-Ct-San-Jose-CA-95117/19604976_zpid/",
            "property_type": "house",
        },
        {
            "address": "691 N Capitol Ave Unit 4, San Jose, CA 95133",
            "price": 975000, "beds": 3, "baths": 3, "sqft": 1522,
            "listing_url": "https://www.zillow.com/homedetails/691-N-Capitol-Ave-UNIT-4-San-Jose-CA-95133/122248369_zpid/",
            "property_type": "townhouse",
        },
    ]




# ---------------------------------------------------------------------------
# Main adapter
# ---------------------------------------------------------------------------

def run_zillow_adapter(
    dry_run: bool = False,
    max_items: int | None = None,
    cities: list[str] | None = None,
    min_price: int | None = None,
    max_price: int | None = None,
    min_beds: int | None = None,
    property_types: list[str] | None = None,
) -> dict[str, int]:
    """
    Execute the Zillow adapter end-to-end.

    Args:
        cities: List of city names to search (default: San Jose, Sunnyvale)
        min_price: Minimum listing price
        max_price: Maximum listing price
        min_beds: Minimum bedrooms
        property_types: Filter by property type(s)
    """
    run_id = generate_run_id("adapter_zillow")

    # Build search description for run notes
    search_cities = cities or DEFAULT_CITIES
    search_desc_parts = [f"Cities: {', '.join(search_cities)}"]
    if min_price:
        search_desc_parts.append(f"Min: ${min_price:,}")
    if max_price:
        search_desc_parts.append(f"Max: ${max_price:,}")
    if min_beds:
        search_desc_parts.append(f"Beds: {min_beds}+")
    if property_types:
        search_desc_parts.append(f"Types: {', '.join(property_types)}")
    search_desc = " | ".join(search_desc_parts)

    start_run(run_id, notes=f"Zillow adapter — {search_desc}", dry_run=dry_run)

    metrics = make_metrics()

    # --- Build Zillow URLs from search criteria ---
    search_urls = [
        build_zillow_url(city, min_price, max_price, min_beds, property_types)
        for city in search_cities
    ]
    logger.info("Search URLs: %s", search_urls)

    # --- Fetch ---
    items: list[dict] = []
    source_urls: list[str] = []
    source_method = "mock"

    api_key = os.environ.get("FIRECRAWL_API_KEY", "").strip()

    if api_key:
        try:
            items, source_urls = fetch_via_firecrawl(api_key, urls=search_urls)
            source_method = "firecrawl"
        except Exception as exc:
            logger.warning("Firecrawl fetch failed: %s — using mock data", exc)
            metrics["fetch_errors"] += 1

    if not items:
        items = fetch_mock_zillow_data()
        source_method = "mock"

    # --- Client-side price filter (belt-and-suspenders with URL filter) ---
    if min_price:
        items = [i for i in items if (i.get("price") or 0) >= min_price]
    if max_price:
        items = [i for i in items if (i.get("price") or float("inf")) <= max_price]

    if max_items:
        items = items[:max_items]

    metrics["items_fetched"] = len(items)
    logger.info("Processing %d Zillow listings (source: %s).", len(items), source_method)

    # --- Map + persist ---
    seen_this_run: set[str] = set()

    for item in items:
        try:
            address = str(item.get("address") or "").strip()
            price = item.get("price")
            beds = item.get("beds")
            baths = item.get("baths")
            sqft = item.get("sqft")
            listing_url = str(item.get("listing_url") or "").strip()
            prop_type_raw = item.get("property_type")
            dom = item.get("days_on_zillow")

            # Price normalization
            if isinstance(price, str):
                try:
                    price = float(re.sub(r"[^\d.]", "", price))
                except ValueError:
                    price = None

            # Stable ID from listing URL (preferred) or address
            id_key = listing_url if listing_url else address
            if not id_key:
                continue
            property_id = stable_property_id(id_key)

            if should_skip_lead(property_id, run_id, seen_this_run=seen_this_run):
                metrics["duplicates_skipped"] += 1
                continue

            now_iso = utc_now_iso()
            prop_type = _map_property_type(prop_type_raw)
            city = _extract_city(address)

            # Build data_sources
            sources: list[str] = [ADAPTER_NAME]
            if source_urls:
                sources.extend(source_urls)
            if listing_url:
                sources.append(listing_url)

            state = JCMPropertyState(
                run_id=run_id,
                property_id=property_id,
                pipeline_stage="sourced",
                county="Santa Clara",
                city=city,
                raw_address=address,
                asking_price=price if isinstance(price, (int, float)) else None,
                property_type=prop_type,
                beds=beds if isinstance(beds, (int, float)) else None,
                baths=baths if isinstance(baths, (int, float)) else None,
                building_sqft=int(sqft) if sqft else None,
                days_on_market=int(dom) if dom else None,
                data_sources=sources,
                audit_log=[
                    AuditEntry(
                        timestamp=now_iso,
                        node=NODE_NAME,
                        field_mutated="pipeline_stage",
                        value_before=None,
                        value_after="sourced",
                        rationale=(
                            f"Ingested from Zillow via {source_method}. "
                            f"Address={address!r} Price={price}"
                        ),
                    )
                ],
            )

            metrics["leads_mapped"] += 1
            persist_lead(state, dry_run=dry_run)
            metrics["leads_ingested_new"] += 1
            logger.info(
                "%sIngested Zillow lead: %s $%s %s",
                "[DRY RUN] Would ingest — " if dry_run else "",
                property_id[:8],
                f"{price:,.0f}" if price else "N/A",
                address[:50],
            )

        except Exception as exc:
            logger.warning("Failed to map Zillow item: %s", exc)
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
        description="JCM Adapter — Zillow South Bay (Firecrawl-powered)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Parse but skip DB writes.")
    parser.add_argument("--max-items", type=int, default=None, help="Process at most N items.")
    parser.add_argument(
        "--cities", nargs="+", default=None,
        help="Cities to search (e.g. 'San Jose' 'Sunnyvale').",
    )
    parser.add_argument("--min-price", type=int, default=None, help="Minimum price filter.")
    parser.add_argument("--max-price", type=int, default=None, help="Maximum price filter.")
    parser.add_argument("--min-beds", type=int, default=None, help="Minimum bedrooms.")
    args = parser.parse_args()

    print("\n" + "=" * 58)
    print("  JCM ADAPTER: Zillow South Bay Real Estate")
    print("=" * 58)
    metrics = run_zillow_adapter(
        dry_run=args.dry_run,
        max_items=args.max_items,
        cities=args.cities,
        min_price=args.min_price,
        max_price=args.max_price,
        min_beds=args.min_beds,
    )
    print("-" * 58)
    print(f"  Items Fetched          {metrics['items_fetched']}")
    print(f"  Leads Ingested         {metrics['leads_ingested']}")
    print(f"  Duplicates Skipped     {metrics['duplicates_skipped']}")
    print(f"  Errors                 {metrics['errors']}")
    print("=" * 58 + "\n")
