"""
Node 2 — Address Normalization & Geocoding

Responsibilities per SPEC.md Section 7, Node 2:
  - Fetch all leads at pipeline_stage='sourced' for the current run.
  - Normalize raw_address into a canonical USPS Pub 28 format (uppercased).
  - Geocode the normalized address to lat/lon with confidence band.
  - Classify property_type from raw data when missing.
  - Persist updated state with pipeline_stage='normalized'.
  - On failure: attach PipelineError, set pipeline_stage='error', persist, continue.
"""

import logging
import re
from datetime import UTC, datetime
from typing import Any, Optional, cast

import requests

from core.database import (
    get_cache,
    get_leads_by_stage,
    make_expires_at,
    set_cache,
    upsert_lead,
)
from core.models import AuditEntry, JCMPropertyState, PipelineError, PropertyType
from core.settings import get_settings
from core.utils import retry_with_backoff

logger = logging.getLogger(__name__)

TARGET_COUNTIES = set(get_settings().TARGET_COUNTIES)
NODE_NAME = "node_02_normalization"
VALID_PROPERTY_TYPES = {"SFR", "Duplex", "Triplex", "Fourplex", "Townhome", "Condo"}


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_address(address: str) -> str:
    """
    Conservative USPS Pub 28-style normalization.
    """
    if not address:
        return ""

    normalized = address.strip().upper()
    normalized = re.sub(r"\s+", " ", normalized)

    replacements = {
        r"\bSTREET\b": "ST",
        r"\bAVENUE\b": "AVE",
        r"\bBOULEVARD\b": "BLVD",
        r"\bDRIVE\b": "DR",
        r"\bROAD\b": "RD",
        r"\bLANE\b": "LN",
        r"\bCOURT\b": "CT",
        r"\bPLACE\b": "PL",
        r"\bTERRACE\b": "TER",
        r"\bPARKWAY\b": "PKWY",
        r"\bHIGHWAY\b": "HWY",
        r"\bAPARTMENT\b": "APT",
        r"\bSUITE\b": "STE",
    }

    for pattern, replacement in replacements.items():
        normalized = re.sub(pattern, replacement, normalized)

    normalized = re.sub(r"\s*,\s*", ", ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def extract_zip_from_address(address: str) -> Optional[str]:
    if not address:
        return None

    match = re.search(r"\b(\d{5})(?:-\d{4})?\b", address)
    return match.group(1) if match else None


def extract_city_from_address(address: str, county: Optional[str]) -> Optional[str]:
    if not address:
        return None

    bay_area_cities_by_county: dict[str, list[str]] = {
        "Santa Clara": [
            "SAN JOSE",
            "SUNNYVALE",
            "SANTA CLARA",
            "MOUNTAIN VIEW",
            "CUPERTINO",
            "PALO ALTO",
            "MILPITAS",
            "LOS GATOS",
            "SARATOGA",
            "CAMPBELL",
            "MORGAN HILL",
            "GILROY",
        ],
        "Alameda": [
            "FREMONT",
            "OAKLAND",
            "BERKELEY",
            "HAYWARD",
            "SAN LEANDRO",
            "PLEASANTON",
            "DUBLIN",
            "LIVERMORE",
            "UNION CITY",
            "NEWARK",
            "ALAMEDA",
            "CASTRO VALLEY",
        ],
    }

    address_upper = address.upper()
    for city in bay_area_cities_by_county.get(county or "", []):
        # Enforce regex word boundaries to prevent substring matching
        if re.search(rf"\b{re.escape(city)}\b", address_upper):
            return city.title()

    parts = [part.strip() for part in address_upper.split(",") if part.strip()]
    if len(parts) >= 2:
        possible_city = parts[-2]
        if possible_city and not re.search(r"\d", possible_city):
            return possible_city.title()

    return None


def classify_property_type(
    raw_address: Optional[str],
    beds: Optional[int],
    building_sqft: Optional[int],
) -> Optional[str]:
    """
    Conservative heuristic only.
    Avoid guessing multifamily from bedroom count alone.
    """
    address = (raw_address or "").upper()

    if any(token in address for token in ["CONDO", "CONDOMINIUM"]):
        return "Condo"

    if any(token in address for token in ["TOWNHOME", "TOWNHOUSE"]):
        return "Townhome"

    if any(token in address for token in ["DUPLEX", "TRIPLEX", "FOURPLEX"]):
        if "FOURPLEX" in address:
            return "Fourplex"
        if "TRIPLEX" in address:
            return "Triplex"
        if "DUPLEX" in address:
            return "Duplex"

    if building_sqft is not None and building_sqft < 500:
        return None

    return "SFR"


def add_manual_review_reason(state: JCMPropertyState, reason: str) -> None:
    if reason not in state.manual_review_reasons:
        state.manual_review_reasons.append(reason)


@retry_with_backoff(max_retries=3)
def geocode_address(address: str) -> Optional[dict[str, Any]]:
    """
    Geocode via US Census single-line address endpoint.
    Confidence is conservative:
    - high: exactly one match
    - medium: multiple matches
    """
    if not address:
        return None

    url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
    params = {
        "address": address,
        "benchmark": "Public_AR_Current",
        "format": "json",
    }

    response = requests.get(url, params=params, timeout=(5, 20))
    response.raise_for_status()

    payload = response.json()
    result = payload.get("result", {})
    matches = result.get("addressMatches", [])

    if not matches:
        return None

    best = matches[0]
    coordinates = best.get("coordinates", {})
    latitude = coordinates.get("y")
    longitude = coordinates.get("x")

    if latitude is None or longitude is None:
        return None

    confidence = "high" if len(matches) == 1 else "medium"

    return {
        "latitude": latitude,
        "longitude": longitude,
        "confidence": confidence,
        "matched_address": best.get("matchedAddress"),
    }


def geocode_with_cache(address: str) -> tuple[Optional[dict[str, Any]], bool]:
    settings = get_settings()
    cache_key = f"geocode:{address.lower().strip()}"

    cached = get_cache(cache_key)
    if cached is not None:
        return cached, True

    result = geocode_address(address)
    if result is not None:
        set_cache(cache_key, result, make_expires_at(settings.CACHE_TTL_GEOCODE_DAYS))

    return result, False


def _fail_lead(
    state: JCMPropertyState,
    audit_entries: list[AuditEntry],
    error_code: str,
    message: str,
    review_reason: str,
    rationale: str,
) -> None:
    """
    Shared helper: append error, set stage to error, flush audit log, upsert.
    Avoids duplicating the same 10-line block across multiple failure paths.
    """
    prior_stage = state.pipeline_stage
    now = utc_now_iso()

    err = PipelineError(
        timestamp=now,
        node=NODE_NAME,
        error_code=error_code,
        message=message,
        is_fatal=False,
    )
    state.errors.append(err)
    add_manual_review_reason(state, review_reason)
    state.pipeline_stage = "error"
    state.audit_log.extend(audit_entries)
    state.audit_log.append(
        AuditEntry(
            timestamp=now,
            node=NODE_NAME,
            field_mutated="pipeline_stage",
            value_before=prior_stage,
            value_after="error",
            rationale=rationale,
        )
    )
    upsert_lead(state)


def process_normalization(run_id: str) -> dict[str, int]:
    """
    Node 2 — Address Normalization & Geocoding.
    """
    sourced_leads = get_leads_by_stage(run_id, "sourced")

    metrics: dict[str, int] = {
        "leads_processed": 0,
        "leads_normalized": 0,
        "leads_errored": 0,
        "geocode_hits": 0,
        "geocode_misses": 0,
        "cache_hits": 0,
    }

    for state in sourced_leads:
        metrics["leads_processed"] += 1
        now_iso = utc_now_iso()

        try:
            audit_entries: list[AuditEntry] = []

            # --- 1. Normalize address ---
            if state.raw_address:
                normalized = normalize_address(state.raw_address)
                if normalized != state.normalized_address:
                    audit_entries.append(
                        AuditEntry(
                            timestamp=now_iso,
                            node=NODE_NAME,
                            field_mutated="normalized_address",
                            value_before=state.normalized_address,
                            value_after=normalized,
                            rationale="Address normalized via USPS Pub 28-style rules",
                        )
                    )
                    state.normalized_address = normalized
            else:
                state.normalized_address = None

            # --- 2. Extract ZIP and city ---
            address_to_parse = state.normalized_address or state.raw_address or ""

            if not state.zip_code:
                extracted_zip = extract_zip_from_address(address_to_parse)
                if extracted_zip:
                    audit_entries.append(
                        AuditEntry(
                            timestamp=now_iso,
                            node=NODE_NAME,
                            field_mutated="zip_code",
                            value_before=state.zip_code,
                            value_after=extracted_zip,
                            rationale="ZIP code extracted from address string",
                        )
                    )
                    state.zip_code = extracted_zip

            if not state.city:
                extracted_city = extract_city_from_address(address_to_parse, state.county)
                if extracted_city:
                    audit_entries.append(
                        AuditEntry(
                            timestamp=now_iso,
                            node=NODE_NAME,
                            field_mutated="city",
                            value_before=state.city,
                            value_after=extracted_city,
                            rationale="City extracted from address string",
                        )
                    )
                    state.city = extracted_city

            # --- 3. Geocode ---
            if state.latitude is None or state.longitude is None:
                geocode_address_str = state.normalized_address or state.raw_address

                # Hard failure: no address string at all
                if not geocode_address_str:
                    metrics["geocode_misses"] += 1
                    _fail_lead(
                        state, audit_entries,
                        error_code="ADDRESS_UNRESOLVED",
                        message=f"No address string available for property_id={state.property_id}",
                        review_reason="ADDRESS_MISSING",
                        rationale="Lead has no usable address for geocoding",
                    )
                    metrics["leads_errored"] += 1
                    continue

                geo_result, was_cached = geocode_with_cache(geocode_address_str)

                if was_cached:
                    metrics["cache_hits"] += 1

                if geo_result:
                    old_lat = state.latitude
                    old_lon = state.longitude

                    state.latitude = geo_result["latitude"]
                    state.longitude = geo_result["longitude"]
                    state.geocode_confidence = geo_result["confidence"]
                    metrics["geocode_hits"] += 1

                    audit_entries.append(AuditEntry(
                        timestamp=now_iso,
                        node=NODE_NAME,
                        field_mutated="latitude,longitude",
                        value_before=f"{old_lat},{old_lon}",
                        value_after=f"{geo_result['latitude']},{geo_result['longitude']}",
                        rationale=f"Geocoded via Census geocoder (confidence={geo_result['confidence']})",
                    ))

                    matched = geo_result.get("matched_address")
                    if matched and matched != state.normalized_address:
                        audit_entries.append(AuditEntry(
                            timestamp=now_iso,
                            node=NODE_NAME,
                            field_mutated="normalized_address",
                            value_before=state.normalized_address,
                            value_after=matched,
                            rationale="Replaced with Census geocoder matched address",
                        ))
                        state.normalized_address = matched

                else:
                    # Geocode returned no match
                    metrics["geocode_misses"] += 1
                    _fail_lead(
                        state, audit_entries,
                        error_code="ADDRESS_UNRESOLVED",
                        message=f"Geocode failed for address: {geocode_address_str}",
                        review_reason="GEOCODE_FAILED",
                        rationale="Address could not be geocoded",
                    )
                    metrics["leads_errored"] += 1
                    continue

            # --- 4. Geography constraint ---
            # Hard gate: county must be present AND in target set.
            if not state.county or state.county not in TARGET_COUNTIES:
                _fail_lead(
                    state, audit_entries,
                    error_code="ADDRESS_UNRESOLVED",
                    message=f"County '{state.county}' is missing or outside target geography for property_id={state.property_id}.",
                    review_reason="COUNTY_OUTSIDE_TARGET",
                    rationale="Lead has no verified county or is outside target geography",
                )
                metrics["leads_errored"] += 1
                continue

            # --- 5. Classify property type ---
            if state.property_type is None:
                classified = classify_property_type(
                    raw_address=state.raw_address,
                    beds=state.beds,
                    building_sqft=state.building_sqft,
                )
                if classified:
                    if classified in VALID_PROPERTY_TYPES:
                        audit_entries.append(AuditEntry(
                            timestamp=now_iso,
                            node=NODE_NAME,
                            field_mutated="property_type",
                            value_before=None,
                            value_after=classified,
                            rationale="Property type classified from conservative heuristics",
                        ))
                        # Explicit typing cast for strict environments
                        state.property_type = cast(PropertyType, classified)
                    else:
                        logger.warning(
                            "Invalid property type returned by classifier for property_id=%s: %s",
                            state.property_id,
                            classified,
                        )

            # --- 6. Advance to normalized ---
            prior_stage = state.pipeline_stage
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="pipeline_stage",
                value_before=prior_stage,
                value_after="normalized",
                rationale="Normalization complete",
            ))
            state.pipeline_stage = "normalized"
            state.audit_log.extend(audit_entries)

            if NODE_NAME not in state.data_sources:
                state.data_sources.append(NODE_NAME)

            upsert_lead(state)
            metrics["leads_normalized"] += 1
            logger.info("Normalized lead %s (APN=%s).", state.property_id, state.apn)

        except Exception as e:
            now_iso = utc_now_iso()
            prior_stage = state.pipeline_stage

            err = PipelineError(
                timestamp=now_iso,
                node=NODE_NAME,
                error_code="NORMALIZATION_FAILURE",
                message=f"Normalization exception for property_id={state.property_id}: {e}",
                is_fatal=False,
            )
            logger.error(err.model_dump_json())
            state.errors.append(err)
            add_manual_review_reason(state, f"Normalization exception: {e}")
            state.pipeline_stage = "error"
            state.audit_log.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="pipeline_stage",
                value_before=prior_stage,
                value_after="error",
                rationale=f"Normalization error: {e}",
            ))
            upsert_lead(state)
            metrics["leads_errored"] += 1

    logger.info("Node 2 complete. Metrics: %s", metrics)
    return metrics
