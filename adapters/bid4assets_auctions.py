# adapters/bid4assets_auctions.py
"""
Adapter: Bid4Assets — Real Estate Auction Listings (via Firecrawl)

Scrapes tax sale and foreclosure auction listings from Bid4Assets.
These are distressed properties by definition (tax delinquent, foreclosure,
bank-owned) and will score well in Node 3's distress scoring.

Uses Firecrawl /v1/scrape with extract mode.
"""

import argparse
import hashlib
import logging
import os
import re
from datetime import UTC, datetime
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core.database import find_recent_lead_by_property_id
from core.models import AuditEntry, DistressSignal, JCMPropertyState
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

ADAPTER_NAME = "adapter_bid4assets"
NODE_NAME = "adapter_bid4assets"

# ---------------------------------------------------------------------------
# Bid4Assets search URLs
# ---------------------------------------------------------------------------
BID4ASSETS_BASE_URL = "https://www.bid4assets.com/real-estate-auctions"

# States to target (configurable from dashboard)
AVAILABLE_STATES = [
    "CA", "TX", "FL", "AZ", "NV", "GA", "NC", "OH",
    "PA", "MI", "IN", "TN", "MO", "AL", "AR",
]
DEFAULT_STATES: list[str] = []  # Empty = all states (national)

# ---------------------------------------------------------------------------
# Firecrawl config
# ---------------------------------------------------------------------------
FIRECRAWL_SCRAPE_URL = "https://api.firecrawl.dev/v1/scrape"

BID4ASSETS_EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "auctions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "address": {
                        "type": "string",
                        "description": "Property address or description",
                    },
                    "city": {
                        "type": "string",
                        "description": "City name",
                    },
                    "state": {
                        "type": "string",
                        "description": "2-letter state code (e.g. CA, TX)",
                    },
                    "county": {
                        "type": "string",
                        "description": "County name",
                    },
                    "starting_bid": {
                        "type": ["number", "null"],
                        "description": "Starting bid price in USD",
                    },
                    "current_bid": {
                        "type": ["number", "null"],
                        "description": "Current high bid in USD",
                    },
                    "num_bids": {
                        "type": ["integer", "null"],
                        "description": "Number of bids placed",
                    },
                    "auction_date": {
                        "type": "string",
                        "description": "Auction end date or time remaining",
                    },
                    "sale_type": {
                        "type": "string",
                        "description": "Tax sale, foreclosure, bank-owned, land, etc.",
                    },
                    "apn": {
                        "type": "string",
                        "description": "Parcel or APN number if available",
                    },
                    "listing_url": {
                        "type": "string",
                        "description": "Full URL to the Bid4Assets listing",
                    },
                },
                "required": ["address"],
            },
        }
    },
}

BID4ASSETS_EXTRACT_PROMPT = (
    "Extract all real estate auction listings from this page. "
    "For each listing get: the property address or title description, "
    "city, 2-letter state code, county, starting bid price in USD, "
    "current high bid in USD, number of bids, auction end date or time remaining, "
    "type of sale (tax sale, foreclosure, bank-owned, land, etc.), "
    "parcel/APN number if shown, and the full URL to the listing detail page."
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def stable_property_id(url_or_address: str) -> str:
    return hashlib.sha256(url_or_address.encode("utf-8")).hexdigest()[:32]


_SALE_TYPE_TO_DISTRESS: dict[str, str] = {
    "tax sale": "tax_delinquency",
    "tax lien": "tax_delinquency",
    "tax deed": "tax_delinquency",
    "foreclosure": "preforeclosure",
    "bank-owned": "preforeclosure",
    "reo": "preforeclosure",
    "bank owned": "preforeclosure",
    "sheriff sale": "preforeclosure",
}

_PROP_TYPE_MAP: dict[str, str] = {
    "house": "SFR", "single family": "SFR", "sfr": "SFR",
    "residential": "SFR",
    "condo": "Condo", "townhouse": "Townhome",
    "duplex": "Duplex", "multi-family": "Duplex",
    # Land, Commercial, etc. are NOT in the model Literal —
    # return None so they default to unset
}

# Valid counties per the Pydantic model — loaded from settings
from core.settings import get_settings as _get_settings
_VALID_COUNTIES = set(_get_settings().TARGET_COUNTIES)


def _map_property_type(raw: Any) -> Optional[str]:
    if not raw:
        return None
    return _PROP_TYPE_MAP.get(str(raw).strip().lower())


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

def fetch_via_firecrawl(api_key: str) -> tuple[list[dict], list[str]]:
    """Scrape Bid4Assets auction listings via Firecrawl."""
    session = _build_session()
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    url = BID4ASSETS_BASE_URL
    logger.info("Scraping Bid4Assets: %s", url)

    payload = {
        "url": url,
        "formats": ["extract"],
        "extract": {
            "prompt": BID4ASSETS_EXTRACT_PROMPT,
            "schema": BID4ASSETS_EXTRACT_SCHEMA,
        },
    }

    resp = session.post(
        FIRECRAWL_SCRAPE_URL,
        json=payload,
        headers=headers,
        timeout=120,
    )
    resp.raise_for_status()
    result = resp.json()

    data = result.get("data", {})
    extract = data.get("extract", {})
    auctions = extract.get("auctions", []) if isinstance(extract, dict) else []

    logger.info("Bid4Assets → %d auction listings extracted.", len(auctions))
    return auctions, [url]


# ---------------------------------------------------------------------------
# Mock data
# ---------------------------------------------------------------------------

def fetch_mock_auctions() -> list[dict]:
    """Mock auction data for development."""
    logger.info("Using mock Bid4Assets listings.")
    return [
        {
            "address": "123 Elm St",
            "city": "San Jose",
            "state": "CA",
            "county": "Santa Clara",
            "starting_bid": 45000,
            "current_bid": 67000,
            "auction_date": "2026-05-15",
            "sale_type": "Tax Sale",
            "listing_url": "https://www.bid4assets.com/auction/mock-001",
        },
        {
            "address": "456 Oak Ave",
            "city": "Sunnyvale",
            "state": "CA",
            "county": "Santa Clara",
            "starting_bid": 120000,
            "current_bid": 155000,
            "auction_date": "2026-05-20",
            "sale_type": "Foreclosure",
            "listing_url": "https://www.bid4assets.com/auction/mock-002",
        },
        {
            "address": "789 Pine Blvd",
            "city": "Dallas",
            "state": "TX",
            "county": "Dallas",
            "starting_bid": 15000,
            "current_bid": 22000,
            "auction_date": "2026-05-10",
            "sale_type": "Tax Sale",
            "listing_url": "https://www.bid4assets.com/auction/mock-003",
        },
    ]




# ---------------------------------------------------------------------------
# Main adapter
# ---------------------------------------------------------------------------

def run_bid4assets_adapter(
    dry_run: bool = False,
    max_items: int | None = None,
    states: list[str] | None = None,
) -> dict[str, int]:
    """
    Execute the Bid4Assets adapter end-to-end.

    Args:
        states: Filter to specific states (e.g. ["CA", "TX"]). None = all.
    """
    run_id = generate_run_id("adapter_b4a")

    filter_desc = f"States: {', '.join(states)}" if states else "All states"
    start_run(run_id, notes=f"Bid4Assets adapter — {filter_desc}", dry_run=dry_run)

    metrics = make_metrics(filtered_out=0)

    # --- Fetch ---
    items: list[dict] = []
    source_urls: list[str] = []
    source_method = "mock"

    api_key = os.environ.get("FIRECRAWL_API_KEY", "").strip()

    if api_key:
        try:
            items, source_urls = fetch_via_firecrawl(api_key)
            source_method = "firecrawl"
        except Exception as exc:
            logger.warning("Firecrawl fetch failed: %s — using mock data", exc)

    if not items:
        items = fetch_mock_auctions()
        source_method = "mock"

    metrics["items_fetched"] = len(items)

    # --- State filter (client-side) ---
    if states:
        state_set = {s.upper() for s in states}
        filtered = [i for i in items if (i.get("state") or "").upper() in state_set]
        metrics["filtered_out"] = len(items) - len(filtered)
        items = filtered
        logger.info("After state filter (%s): %d of %d items.", state_set, len(items), metrics["items_fetched"])

    if max_items:
        items = items[:max_items]

    logger.info("Processing %d Bid4Assets auctions (source: %s).", len(items), source_method)

    # --- Map + persist ---
    seen_this_run: set[str] = set()

    for item in items:
        try:
            address = str(item.get("address") or "").strip()
            city = str(item.get("city") or "").strip()
            state = str(item.get("state") or "").strip()
            county = str(item.get("county") or "").strip()
            starting_bid = item.get("starting_bid")
            current_bid = item.get("current_bid")
            auction_date = str(item.get("auction_date") or "").strip()
            sale_type = str(item.get("sale_type") or "").strip()
            apn = str(item.get("apn") or "").strip()
            listing_url = str(item.get("listing_url") or "").strip()
            prop_type_raw = item.get("property_type")

            # Price: use current_bid if available, else starting_bid
            price = current_bid if current_bid else starting_bid
            if isinstance(price, str):
                try:
                    price = float(re.sub(r"[^\d.]", "", price))
                except ValueError:
                    price = None

            # Stable ID
            id_key = listing_url if listing_url else f"{address}-{city}-{state}"
            if not id_key:
                continue
            property_id = stable_property_id(id_key)

            if should_skip_lead(property_id, run_id, seen_this_run=seen_this_run):
                metrics["duplicates_skipped"] += 1
                continue

            now_iso = utc_now_iso()

            # Build full address
            addr_parts = [address]
            if city:
                addr_parts.append(city)
            if state:
                addr_parts.append(state)
            raw_address = ", ".join(addr_parts)

            # Map sale type to distress signal
            distress_signals: list[DistressSignal] = []
            distress_type = _SALE_TYPE_TO_DISTRESS.get(sale_type.lower(), "tax_delinquency")
            distress_signals.append(DistressSignal(
                signal_type=distress_type,
                severity=0.80,  # Auctions are high-severity by definition
                source="bid4assets.com",
                detected_at=now_iso,
                details=f"Active {sale_type} auction on Bid4Assets. Date: {auction_date}",
                corroborated=True,  # Public auction = corroborated
            ))

            # Data sources
            sources: list[str] = [ADAPTER_NAME]
            if source_urls:
                sources.extend(source_urls)
            if listing_url:
                sources.append(listing_url)

            # Map county — model only accepts known counties
            model_county = county if county in _VALID_COUNTIES else "Santa Clara"

            state_obj = JCMPropertyState(
                run_id=run_id,
                property_id=property_id,
                pipeline_stage="sourced",
                county=model_county,
                city=city or None,
                raw_address=raw_address,
                asking_price=price if isinstance(price, (int, float)) else None,
                property_type=_map_property_type(prop_type_raw),
                apn=apn or None,
                distress_signals=distress_signals,
                data_sources=sources,
                audit_log=[
                    AuditEntry(
                        timestamp=now_iso,
                        node=NODE_NAME,
                        field_mutated="pipeline_stage",
                        value_before=None,
                        value_after="sourced",
                        rationale=(
                            f"Ingested from Bid4Assets ({sale_type} auction). "
                            f"Starting bid=${starting_bid}, Current bid=${current_bid}. "
                            f"Address={raw_address!r}"
                        ),
                    )
                ],
            )

            metrics["leads_mapped"] += 1
            persist_lead(state_obj, dry_run=dry_run)
            metrics["leads_ingested_new"] += 1
            logger.info(
                "%sIngested B4A lead: %s $%s %s (%s)",
                "[DRY RUN] Would ingest — " if dry_run else "",
                property_id[:8],
                f"{price:,.0f}" if price else "N/A",
                raw_address[:40],
                sale_type,
            )

        except Exception as exc:
            logger.warning("Failed to map B4A item: %s", exc)
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
        description="JCM Adapter — Bid4Assets Auctions (Firecrawl-powered)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Parse but skip DB writes.")
    parser.add_argument("--max-items", type=int, default=None, help="Process at most N items.")
    parser.add_argument(
        "--states", nargs="+", default=None,
        help="Filter to specific states (e.g. CA TX FL). Default=all.",
    )
    args = parser.parse_args()

    print("\n" + "=" * 58)
    print("  JCM ADAPTER: Bid4Assets Real Estate Auctions")
    print("=" * 58)
    metrics = run_bid4assets_adapter(
        dry_run=args.dry_run,
        max_items=args.max_items,
        states=args.states,
    )
    print("-" * 58)
    print(f"  Items Fetched          {metrics['items_fetched']}")
    print(f"  Leads Ingested         {metrics['leads_ingested']}")
    print(f"  Duplicates Skipped     {metrics['duplicates_skipped']}")
    print(f"  Filtered Out           {metrics['filtered_out']}")
    print(f"  Errors                 {metrics['errors']}")
    print("=" * 58 + "\n")
