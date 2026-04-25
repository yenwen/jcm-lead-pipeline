"""
JCM Global Pipeline Dashboard

Streamlit-based live view of the JCM pipeline database.
Run with: streamlit run dashboard.py

Features:
    - Top-level KPIs: total leads, by-stage counts, avg distress score
    - Pipeline stage funnel (Plotly bar chart, ordered by stage)
    - Leads by county (Plotly bar chart)
    - Filterable lead table with key underwriting fields
    - Per-lead detail inspector: audit log, distress signals, errors, raw state
    - Run history table: adapter and pipeline run statuses
    - Sidebar adapter trigger buttons (Craigslist, Apify, Firecrawl)
    - Manual refresh button with last-updated timestamp
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import datetime
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from core.settings import get_settings

# ---------------------------------------------------------------------------
# Page config -- must be the first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="JCM Global | Pipeline",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Pipeline stage order -- aligned to core/models.py
# ---------------------------------------------------------------------------

STAGE_ORDER = [
    "NEW",
    "SOURCED",
    "NORMALIZED",
    "ENRICHED",
    "UNDERWRITTEN",
    "PUBLISHED",
    "REJECTED",
    "ERROR",
]

ADAPTER_MODULES = {
    "Craigslist": "adapters.craigslist_rss",
    "Facebook": "adapters.apify_facebook",
    "County Records (Firecrawl)": "adapters.firecrawl_county",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_json_loads(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _coerce_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=False)


def _currency(value: Any) -> Any:
    return value if isinstance(value, (int, float)) else None


def _pick_arv(state: dict[str, Any]) -> Any:
    return state.get("risk_adjusted_arv") or state.get("estimated_arv_base")


def _pick_recommendation(state: dict[str, Any]) -> str:
    decision = state.get("decision") or {}
    if isinstance(decision, dict):
        return decision.get("recommendation") or "--"
    return "--"


def _pick_summary(state: dict[str, Any]) -> str:
    decision = state.get("decision") or {}
    if isinstance(decision, dict):
        return decision.get("summary") or "--"
    return "--"


def _run_subprocess(cmd: list[str], label: str, timeout: int) -> None:
    with st.spinner(f"Running {label}..."):
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    if result.returncode == 0:
        st.success(f"{label} complete")
        stdout = (result.stdout or "").strip()
        if stdout:
            st.code(stdout[-3000:], language="text")
    else:
        st.error(f"{label} failed")
        stderr = (result.stderr or result.stdout or "").strip()
        if stderr:
            st.code(stderr[-3000:], language="text")


def clear_cache_and_rerun() -> None:
    load_leads_data.clear()
    load_runs_data.clear()
    st.rerun()


def _check_pipeline_health() -> list[dict[str, str]]:
    """
    Probe each pipeline component and return its status.

    Returns a list of dicts with keys: component, status, detail.
    Status is one of: "live", "mock", "error", "missing".
    """
    import os

    checks: list[dict[str, str]] = []

    # --- Adapters / Data Sources ---
    fc_key = os.environ.get("FIRECRAWL_API_KEY", "").strip()

    # Zillow — primary Firecrawl source
    if fc_key:
        checks.append({
            "component": "Zillow (Firecrawl)",
            "status": "live",
            "detail": f"API key configured (••••••••)",
        })
    else:
        checks.append({
            "component": "Zillow (Firecrawl)",
            "status": "mock",
            "detail": "FIRECRAWL_API_KEY not set — using mock Zillow data",
        })

    # County Tax — re-enabled
    if fc_key:
        checks.append({
            "component": "County Tax (Firecrawl)",
            "status": "live",
            "detail": f"API key configured (••••••••)",
        })
    else:
        checks.append({
            "component": "County Tax (Firecrawl)",
            "status": "mock",
            "detail": "FIRECRAWL_API_KEY not set — using mock tax records",
        })

    # Craigslist — blocked by Firecrawl
    checks.append({
        "component": "Craigslist",
        "status": "error",
        "detail": "⛔ Blocked by Firecrawl — CL is on their blocklist",
    })

    # Bid4Assets — uses same Firecrawl key
    if fc_key:
        checks.append({
            "component": "Bid4Assets (Firecrawl)",
            "status": "live",
            "detail": f"API key configured (••••••••)",
        })
    else:
        checks.append({
            "component": "Bid4Assets (Firecrawl)",
            "status": "mock",
            "detail": "FIRECRAWL_API_KEY not set — using mock auctions",
        })

    apify_key = os.environ.get("APIFY_API_TOKEN", "").strip()
    if apify_key:
        checks.append({
            "component": "Apify (Facebook)",
            "status": "live",
            "detail": "API token configured (••••••••)",
        })
    else:
        checks.append({
            "component": "Apify (Facebook)",
            "status": "mock",
            "detail": "APIFY_API_TOKEN not set — using mock listings",
        })

    # --- Node Dependencies ---
    ps_key = os.environ.get("PROPSTREAM_API_KEY", "").strip()
    if ps_key:
        checks.append({
            "component": "PropStream (Enrichment)",
            "status": "live",
            "detail": "API key configured (••••••••)",
        })
    else:
        checks.append({
            "component": "PropStream (Enrichment)",
            "status": "mock",
            "detail": "PROPSTREAM_API_KEY not set — Node 4 uses mock property data",
        })

    oai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if oai_key:
        checks.append({
            "component": "OpenAI Vision (Rehab AI)",
            "status": "live",
            "detail": "API key configured (••••••••)",
        })
    else:
        checks.append({
            "component": "OpenAI Vision (Rehab AI)",
            "status": "mock",
            "detail": "OPENAI_API_KEY not set — Node 7 uses mock vision analysis",
        })

    at_key = os.environ.get("AIRTABLE_ACCESS_TOKEN", "").strip()
    at_base = os.environ.get("AIRTABLE_BASE_ID", "").strip()
    if at_key and at_base:
        checks.append({
            "component": "Airtable (CRM Publish)",
            "status": "live",
            "detail": f"Token + Base ID configured",
        })
    else:
        missing = []
        if not at_key:
            missing.append("AIRTABLE_ACCESS_TOKEN")
        if not at_base:
            missing.append("AIRTABLE_BASE_ID")
        checks.append({
            "component": "Airtable (CRM Publish)",
            "status": "mock",
            "detail": f"{', '.join(missing)} not set — Node 9 uses mock CRM",
        })

    # --- Database ---
    try:
        settings = get_settings()
        db_path = settings.DATABASE_PATH
        if db_path.exists():
            size_kb = db_path.stat().st_size / 1024
            checks.append({
                "component": "SQLite Database",
                "status": "live",
                "detail": f"{db_path.name} ({size_kb:.0f} KB)",
            })
        else:
            checks.append({
                "component": "SQLite Database",
                "status": "missing",
                "detail": f"{db_path.name} not found — run python main.py first",
            })
    except Exception as exc:
        checks.append({
            "component": "SQLite Database",
            "status": "error",
            "detail": str(exc),
        })

    return checks


def _classify_source(run_id: str, data_sources: list[str]) -> str:
    """Classify a lead as Mock or Live based on run_id and data sources."""
    # Firecrawl live data has county/auction/assessor URLs in data_sources
    _live_markers = ("sccassessor.org", "dtac.sccgov.org", "bid4assets.com")
    if any(marker in s for s in data_sources for marker in _live_markers):
        return "Live (County Tax)"
    # Zillow live data has zillow.com URLs
    if any("zillow.com" in s for s in data_sources):
        return "Live (Zillow)"
    # Bid4Assets live data
    if any("bid4assets.com" in s for s in data_sources):
        return "Live (Bid4Assets)"
    # Adapter runs with mock data
    if run_id.startswith("adapter_b4a"):
        return "Mock (Bid4Assets)"
    if run_id.startswith("adapter_zillow"):
        return "Mock (Zillow)"
    if run_id.startswith("adapter_firecrawl"):
        return "Mock (Firecrawl)"
    if run_id.startswith("adapter_fb"):
        return "Mock (Facebook)"
    if run_id.startswith("adapter_cl"):
        return "Mock (Craigslist)"
    # Full pipeline mock data from Node 1
    if run_id.startswith("run_"):
        return "Mock (PropStream)"
    return "Unknown"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30)
def load_leads_data() -> pd.DataFrame:
    """
    Load all leads from SQLite and flatten the state_json blob into columns.
    Returns an empty DataFrame if the DB or table does not yet exist.
    """
    settings = get_settings()

    try:
        with sqlite3.connect(settings.DATABASE_PATH) as conn:
            raw_df = pd.read_sql_query(
                """
                SELECT property_id, run_id, pipeline_stage, state_json, updated_at
                FROM leads
                ORDER BY updated_at DESC
                """,
                conn,
            )
    except Exception:
        return pd.DataFrame()

    if raw_df.empty:
        return raw_df

    extracted: list[dict[str, Any]] = []

    for _, row in raw_df.iterrows():
        try:
            state = _safe_json_loads(row["state_json"])

            distress_signals = state.get("distress_signals") or []
            audit_log = state.get("audit_log") or []
            errors = state.get("errors") or []

            distress_score = state.get("distress_score")
            asking_price = _currency(state.get("asking_price"))
            arv = _currency(_pick_arv(state))
            max_offer = _currency(state.get("max_offer_price"))
            rehab_cost = _currency(state.get("total_rehab_estimate"))
            expected_profit = _currency(state.get("expected_profit"))
            expected_roi = state.get("expected_roi")

            # Determine data source type (Mock vs Live)
            data_sources_list = state.get("data_sources") or []
            run_id_val = str(row["run_id"])
            source_type = _classify_source(run_id_val, data_sources_list)

            # Extract clickable lookup URL if present
            lookup_url = next(
                (s for s in data_sources_list if s.startswith("http")),
                None,
            )

            extracted.append(
                {
                    # Identification
                    "property_id": row["property_id"],
                    "property_id_short": str(row["property_id"])[:8],
                    "run_id": row["run_id"],
                    # Pipeline
                    "Stage": str(row["pipeline_stage"]).upper(),
                    # Source classification
                    "Source": source_type,
                    # Location
                    "County": state.get("county") or "Unknown",
                    "City": state.get("city") or "--",
                    "Address": (
                        state.get("normalized_address")
                        or state.get("raw_address")
                        or "No Address"
                    ),
                    # Property
                    "Property Type": state.get("property_type") or "Unknown",
                    "APN": state.get("apn") or "--",
                    "Beds": state.get("beds"),
                    "Baths": state.get("baths"),
                    "Building SqFt": state.get("building_sqft"),
                    "Year Built": state.get("year_built"),
                    # Financials
                    "Asking Price": asking_price,
                    "ARV": arv,
                    "Max Offer": max_offer,
                    "Rehab Cost": rehab_cost,
                    "Expected Profit": expected_profit,
                    "Expected ROI": expected_roi,
                    "Recommendation": _pick_recommendation(state),
                    "Decision Summary": _pick_summary(state),
                    # Distress
                    "Distress Score": (
                        round(float(distress_score), 2)
                        if distress_score is not None
                        else None
                    ),
                    "Distress Signals": len(distress_signals),
                    # Meta
                    "Data Sources": ", ".join(data_sources_list),
                    "Lookup URL": lookup_url,
                    "Updated At": row["updated_at"],
                    # Source Confidence
                    "Src Confidence": (
                        (state.get("source_confidence") or {}).get("confidence_score")
                    ),
                    "Src Freshness": (
                        (state.get("source_confidence") or {}).get("freshness", "--")
                    ),
                    "Src Completeness": (
                        (state.get("source_confidence") or {}).get("completeness_score")
                    ),
                    "Src Missing Fields": (
                        ", ".join(
                            (state.get("source_confidence") or {}).get("missing_fields", [])
                        ) or "--"
                    ),
                    # Raw blobs for drill-down
                    "_audit_log": audit_log,
                    "_distress_signals": distress_signals,
                    "_errors": errors,
                    "_source_confidence": state.get("source_confidence") or {},
                    "_full_state": state,
                }
            )
        except Exception as exc:
            logger.warning("Failed to extract row for lead: %s", exc)
            continue

    df = pd.DataFrame(extracted)
    if not df.empty and "Updated At" in df.columns:
        df["Updated At"] = _coerce_datetime(df["Updated At"])

    return df


@st.cache_data(ttl=30)
def load_runs_data() -> pd.DataFrame:
    """
    Load adapter/pipeline run history from the runs table.
    """
    settings = get_settings()

    try:
        with sqlite3.connect(settings.DATABASE_PATH) as conn:
            df = pd.read_sql_query(
                """
                SELECT run_id, status, notes, started_at, updated_at, completed_at
                FROM runs
                ORDER BY started_at DESC
                LIMIT 50
                """,
                conn,
            )
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df

    for col in ["started_at", "updated_at", "completed_at"]:
        if col in df.columns:
            df[col] = _coerce_datetime(df[col])

    return df


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("JCM Global")
    st.caption("Pipeline Command Center")

    st.divider()

    if st.button("Refresh Data", use_container_width=True):
        clear_cache_and_rerun()
    st.caption(f"Last loaded: {datetime.now().strftime('%I:%M:%S %p')}")

    st.divider()
    st.subheader("🏠 Zillow Search")
    st.caption("Configure search criteria, then scrape.")

    # City selection
    from adapters.zillow_scrape import AVAILABLE_CITIES, DEFAULT_CITIES
    zillow_cities = st.multiselect(
        "Cities",
        options=AVAILABLE_CITIES,
        default=DEFAULT_CITIES,
        help="Select one or more cities to search on Zillow.",
    )

    # Price range
    zp_col1, zp_col2 = st.columns(2)
    with zp_col1:
        zillow_min_price = st.number_input(
            "Min Price ($)",
            min_value=0,
            max_value=10_000_000,
            value=300_000,
            step=50_000,
            format="%d",
        )
    with zp_col2:
        zillow_max_price = st.number_input(
            "Max Price ($)",
            min_value=0,
            max_value=10_000_000,
            value=2_500_000,
            step=50_000,
            format="%d",
        )

    # Beds + Max results
    zb_col1, zb_col2 = st.columns(2)
    with zb_col1:
        zillow_min_beds = st.selectbox("Min Beds", [0, 1, 2, 3, 4, 5], index=0)
    with zb_col2:
        zillow_max_items = st.selectbox("Max Results", [10, 25, 50, 100], index=1)

    # Run Zillow (ingest only — no Nodes 2-9)
    if st.button("🔍 Scrape Zillow", use_container_width=True, type="primary"):
        cmd = [
            sys.executable, "-m", "adapters.zillow_scrape",
            "--max-items", str(zillow_max_items),
        ]
        if zillow_cities:
            cmd.extend(["--cities"] + zillow_cities)
        if zillow_min_price > 0:
            cmd.extend(["--min-price", str(zillow_min_price)])
        if zillow_max_price > 0:
            cmd.extend(["--max-price", str(zillow_max_price)])
        if zillow_min_beds > 0:
            cmd.extend(["--min-beds", str(zillow_min_beds)])

        _run_subprocess(cmd, "Zillow scrape (ingest only)", timeout=300)
        clear_cache_and_rerun()

    st.caption("Ingests to SOURCED stage only. Run Full Pipeline to underwrite.")

    st.divider()
    st.subheader("Other Adapters")

    # Bid4Assets — distressed auction properties
    if st.button("🔨 Bid4Assets Auctions", use_container_width=True):
        cmd = [sys.executable, "-m", "adapters.bid4assets_auctions"]
        _run_subprocess(cmd, "Bid4Assets auction scrape", timeout=300)
        clear_cache_and_rerun()

    st.caption("Tax sales & foreclosures — pre-tagged with distress signals.")

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("📋 County Tax", use_container_width=True):
            _run_subprocess(
                [sys.executable, "main.py", "--mode", "adapters", "--only", "firecrawl_county"],
                "County tax via Firecrawl + Nodes 2-9",
                timeout=600,
            )
            clear_cache_and_rerun()

    with col_b:
        if st.button("Facebook", use_container_width=True):
            _run_subprocess(
                [sys.executable, "-m", ADAPTER_MODULES["Facebook"]],
                "Facebook adapter",
                timeout=300,
            )
            clear_cache_and_rerun()

    st.divider()

    if st.button(
        "▶️ Run Full Pipeline",
        use_container_width=True,
        help="Runs Nodes 2-9 on all SOURCED leads (normalize, enrich, underwrite, publish).",
    ):
        _run_subprocess(
            [sys.executable, "main.py"],
            "Full pipeline",
            timeout=600,
        )
        clear_cache_and_rerun()

    st.divider()
    st.subheader("Filter Leads")

# ---------------------------------------------------------------------------
# Main content -- load data
# ---------------------------------------------------------------------------

df = load_leads_data()

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

with st.sidebar:
    if df.empty:
        available_stages = ["ALL"]
        available_counties = ["ALL"]
    else:
        stages_present = [s for s in STAGE_ORDER if s in df["Stage"].dropna().unique()]
        available_stages = ["ALL"] + stages_present
        available_counties = ["ALL"] + sorted(df["County"].dropna().unique().tolist())

    selected_stage = st.selectbox("Pipeline Stage", available_stages)
    selected_county = st.selectbox("County", available_counties)

    min_distress = st.slider(
        "Min Distress Score",
        min_value=0.0,
        max_value=100.0,
        value=0.0,
        step=5.0,
        help="Filter leads by minimum distress score (0-100).",
    )

# ---------------------------------------------------------------------------
# Empty state
# ---------------------------------------------------------------------------

if df.empty:
    st.title("JCM Global Real Estate Pipeline")
    st.warning(
        "No leads found in the database yet.\n\n"
        "Run an adapter from the sidebar, or run `python main.py` in your terminal."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Apply filters
# ---------------------------------------------------------------------------

filtered_df = df.copy()

if selected_stage != "ALL":
    filtered_df = filtered_df[filtered_df["Stage"] == selected_stage]

if selected_county != "ALL":
    filtered_df = filtered_df[filtered_df["County"] == selected_county]

if min_distress > 0:
    filtered_df = filtered_df[
        filtered_df["Distress Score"].fillna(0) >= min_distress
    ]

# ---------------------------------------------------------------------------
# Header + KPIs
# ---------------------------------------------------------------------------

st.title("JCM Global Real Estate Pipeline")
st.markdown(
    "Live view of Top-of-Funnel ingestion through underwriting and publishing."
)

# ---------------------------------------------------------------------------
# Pipeline Health Status
# ---------------------------------------------------------------------------

health_checks = _check_pipeline_health()
live_count = sum(1 for c in health_checks if c["status"] == "live")
mock_count = sum(1 for c in health_checks if c["status"] == "mock")
bad_count = sum(1 for c in health_checks if c["status"] in ("error", "missing"))

_status_icons = {"live": "🟢", "mock": "🟡", "error": "🔴", "missing": "🔴"}

health_summary = f"🟢 {live_count} Live  ·  🟡 {mock_count} Mock  ·  🔴 {bad_count} Down"

with st.expander(f"**Pipeline Health**  —  {health_summary}", expanded=False):
    # Split into adapters and node dependencies
    adapter_checks = [c for c in health_checks if any(
        k in c["component"] for k in ("Firecrawl", "Apify", "Craigslist")
    )]
    node_checks = [c for c in health_checks if any(
        k in c["component"] for k in ("PropStream", "OpenAI", "Airtable")
    )]
    infra_checks = [c for c in health_checks if any(
        k in c["component"] for k in ("SQLite", "Database")
    )]

    col_a, col_b, col_c = st.columns(3)

    with col_a:
        st.markdown("##### Data Sources")
        for c in adapter_checks:
            icon = _status_icons.get(c["status"], "⚪")
            st.markdown(f"{icon} **{c['component']}**")
            st.caption(c["detail"])

    with col_b:
        st.markdown("##### Node APIs")
        for c in node_checks:
            icon = _status_icons.get(c["status"], "⚪")
            st.markdown(f"{icon} **{c['component']}**")
            st.caption(c["detail"])

    with col_c:
        st.markdown("##### Infrastructure")
        for c in infra_checks:
            icon = _status_icons.get(c["status"], "⚪")
            st.markdown(f"{icon} **{c['component']}**")
            st.caption(c["detail"])

    st.markdown("---")
    st.markdown(
        "🟢 = **Live** (real API connected)  ·  "
        "🟡 = **Mock** (using test data — set API key in `.env` to go live)  ·  "
        "🔴 = **Down** (error or missing)"
    )

kpi_df = filtered_df

col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
with col1:
    st.metric("Visible Leads", len(kpi_df))
with col2:
    st.metric("Sourced", len(kpi_df[kpi_df["Stage"] == "SOURCED"]))
with col3:
    st.metric("Underwritten", len(kpi_df[kpi_df["Stage"] == "UNDERWRITTEN"]))
with col4:
    st.metric("Published", len(kpi_df[kpi_df["Stage"] == "PUBLISHED"]))
with col5:
    avg_distress = kpi_df["Distress Score"].dropna().mean()
    st.metric(
        "Avg Distress",
        f"{avg_distress:.1f}" if pd.notna(avg_distress) else "--",
    )
with col6:
    if "Src Confidence" in kpi_df.columns:
        avg_trust = kpi_df["Src Confidence"].dropna().mean()
        st.metric(
            "Avg Src Trust",
            f"{avg_trust:.0f}" if pd.notna(avg_trust) else "--",
        )
    else:
        st.metric("Avg Src Trust", "--")
with col7:
    st.metric("Errors", len(kpi_df[kpi_df["Stage"] == "ERROR"]))

st.divider()

# ---------------------------------------------------------------------------
# Charts -- stage funnel + county breakdown
# ---------------------------------------------------------------------------

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Pipeline Funnel")

    stage_counts = (
        filtered_df["Stage"]
        .value_counts()
        .reindex(
            [s for s in STAGE_ORDER if s in filtered_df["Stage"].values],
            fill_value=0,
        )
        .reset_index()
    )
    stage_counts.columns = ["Stage", "Count"]

    # Guard: px.bar raises ValueError on empty DataFrame
    if not stage_counts.empty and stage_counts["Count"].sum() > 0:
        fig_funnel = px.bar(
            stage_counts,
            x="Stage",
            y="Count",
            color="Stage",
            text="Count",
        )
        fig_funnel.update_traces(textposition="outside")
        fig_funnel.update_layout(
            showlegend=False,
            margin=dict(t=20, b=20),
            xaxis_title=None,
            yaxis_title="Leads",
        )
        st.plotly_chart(fig_funnel, use_container_width=True)
    else:
        st.caption("No leads match current filters.")

with chart_col2:
    st.subheader("Leads by County")

    county_counts = filtered_df["County"].value_counts().reset_index()
    county_counts.columns = ["County", "Count"]

    # Guard: px.bar raises ValueError on empty DataFrame
    if not county_counts.empty and county_counts["Count"].sum() > 0:
        fig_county = px.bar(
            county_counts,
            x="County",
            y="Count",
            color="County",
            text="Count",
        )
        fig_county.update_traces(textposition="outside")
        fig_county.update_layout(
            showlegend=False,
            margin=dict(t=20, b=20),
            xaxis_title=None,
            yaxis_title="Leads",
        )
        st.plotly_chart(fig_county, use_container_width=True)
    else:
        st.caption("No county data available for current filters.")

st.divider()

# ---------------------------------------------------------------------------
# Lead table
# ---------------------------------------------------------------------------

st.subheader(f"Lead Database ({len(filtered_df)} visible)")

display_cols = [
    "property_id_short",
    "Source",
    "Stage",
    "County",
    "City",
    "Address",
    "Property Type",
    "Asking Price",
    "ARV",
    "Max Offer",
    "Rehab Cost",
    "Expected Profit",
    "Expected ROI",
    "Recommendation",
    "Distress Score",
    "Src Confidence",
    "Src Freshness",
    "Distress Signals",
    "APN",
    "Lookup URL",
    "Updated At",
]
display_cols = [c for c in display_cols if c in filtered_df.columns]

st.dataframe(
    filtered_df[display_cols],
    column_config={
        "property_id_short": st.column_config.TextColumn("ID"),
        "Source": st.column_config.TextColumn("Source"),
        "Asking Price": st.column_config.NumberColumn(
            "Asking Price", format="$%d"
        ),
        "ARV": st.column_config.NumberColumn("ARV", format="$%d"),
        "Max Offer": st.column_config.NumberColumn("Max Offer", format="$%d"),
        "Rehab Cost": st.column_config.NumberColumn("Rehab Cost", format="$%d"),
        "Expected Profit": st.column_config.NumberColumn(
            "Expected Profit", format="$%d"
        ),
        "Expected ROI": st.column_config.NumberColumn(
            "Expected ROI", format="%.2f"
        ),
        "Distress Score": st.column_config.ProgressColumn(
            "Distress Score",
            min_value=0.0,
            max_value=100.0,
            format="%.1f",
        ),
        "Src Confidence": st.column_config.ProgressColumn(
            "Src Trust",
            min_value=0,
            max_value=100,
            format="%d",
        ),
        "Src Freshness": st.column_config.TextColumn("Freshness"),
        "Lookup URL": st.column_config.LinkColumn(
            "County Record",
            display_text="View",
        ),
        "Updated At": st.column_config.DatetimeColumn(
            "Last Updated",
            format="MMM D, YYYY h:mm a",
        ),
    },
    use_container_width=True,
    hide_index=True,
)

# ---------------------------------------------------------------------------
# Per-lead drill-down
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Lead Detail Inspector")

if not filtered_df.empty:

    # Append last 4 chars of full property_id as tiebreaker to prevent
    # duplicate keys when two leads share the same 8-char prefix and address.
    lead_label_map = {
        f"{row['property_id_short']} -- {row['Address']} [{row['property_id'][-4:]}]": (
            row["property_id"]
        )
        for _, row in filtered_df[
            ["property_id_short", "Address", "property_id"]
        ].iterrows()
    }

    selected_label = st.selectbox(
        "Select a lead to inspect",
        list(lead_label_map.keys()),
        help="Expands distress signals, audit log, errors, and full raw state.",
    )

    selected_property_id = lead_label_map[selected_label]
    selected_row = filtered_df[
        filtered_df["property_id"] == selected_property_id
    ].iloc[0]

    summary_col1, summary_col2, summary_col3 = st.columns(3)
    with summary_col1:
        st.markdown(f"**Address**: {selected_row['Address']}")
        st.markdown(
            f"**County / City**: {selected_row['County']} / {selected_row['City']}"
        )
        st.markdown(f"**APN**: {selected_row['APN']}")
        st.markdown(f"**Source**: {selected_row.get('Source', '--')}")
    with summary_col2:
        st.markdown(f"**Stage**: {selected_row['Stage']}")
        st.markdown(f"**Recommendation**: {selected_row['Recommendation']}")
        st.markdown(
            f"**Distress Score**: "
            f"{selected_row['Distress Score'] if pd.notna(selected_row['Distress Score']) else '--'}"
        )
        lookup = selected_row.get("Lookup URL")
        if lookup:
            st.markdown(f"**[View County Record]({lookup})**")
    with summary_col3:
        st.markdown(
            f"**Asking Price**: "
            f"{selected_row['Asking Price'] if pd.notna(selected_row['Asking Price']) else '--'}"
        )
        st.markdown(
            f"**ARV**: "
            f"{selected_row['ARV'] if pd.notna(selected_row['ARV']) else '--'}"
        )
        st.markdown(
            f"**Max Offer**: "
            f"{selected_row['Max Offer'] if pd.notna(selected_row['Max Offer']) else '--'}"
        )

    detail_col1, detail_col2 = st.columns(2)

    with detail_col1:
        st.markdown("**Distress Signals**")
        signals = selected_row.get("_distress_signals", [])
        if signals:
            for sig in signals:
                severity = sig.get("severity", 0)
                try:
                    severity_text = f"{float(severity):.2f}"
                except Exception:
                    severity_text = str(severity)
                st.markdown(
                    f"- `{sig.get('signal_type', 'unknown')}` -- "
                    f"severity **{severity_text}** | "
                    f"source: {sig.get('source', '--')} | "
                    f"corroborated: {sig.get('corroborated', False)}"
                )
        else:
            st.caption("No distress signals recorded.")

        st.markdown("**Errors**")
        errors = selected_row.get("_errors", [])
        if errors:
            for err in errors:
                st.markdown(
                    f"- `{err.get('node', '?')}` / `{err.get('error_code', '?')}` -- "
                    f"{err.get('message', '')}"
                )
        else:
            st.caption("No pipeline errors recorded.")

    with detail_col2:
        st.markdown("**Audit Log**")
        audit = selected_row.get("_audit_log", [])
        if audit:
            for entry in reversed(audit):
                timestamp = str(entry.get("timestamp", ""))[:19]
                node = entry.get("node", "?")
                value_after = entry.get("value_after", "?")
                rationale = str(entry.get("rationale", ""))[:180]
                st.markdown(
                    f"- `{node}` -> **{value_after}** | "
                    f"{timestamp} | _{rationale}_"
                )
        else:
            st.caption("No audit log entries.")

    with st.expander("Source Confidence Report", expanded=True):
        sc = selected_row.get("_source_confidence", {})
        if sc and sc.get("confidence_score") is not None:
            sc_col1, sc_col2, sc_col3, sc_col4 = st.columns(4)
            with sc_col1:
                st.metric("Trust Score", f"{sc.get('confidence_score', 0)}/100")
                st.caption(f"Label: **{sc.get('confidence_label', '--')}**")
            with sc_col2:
                reliability = sc.get('source_reliability', 0)
                st.metric("Source Reliability", f"{reliability:.0%}")
                st.caption(f"{sc.get('source_reliability_label', '--')} ({sc.get('source_adapter', '--')})")
            with sc_col3:
                completeness = sc.get('completeness_score', 0)
                st.metric("Data Completeness", f"{completeness:.0%}")
                present = sc.get('fields_present', 0)
                total = sc.get('fields_total', 0)
                st.caption(f"{present}/{total} critical fields populated")
            with sc_col4:
                st.metric("Freshness", sc.get('freshness', 'unknown').title())
                age = sc.get('data_age_hours')
                st.caption(f"{age:.0f}h old" if age is not None else "Age unknown")

            missing = sc.get('missing_fields', [])
            if missing:
                st.warning(f"Missing fields: **{', '.join(missing)}**")

            st.caption(f"Breakdown: {sc.get('confidence_breakdown', '--')}")
        else:
            st.caption("Source confidence not yet computed for this lead. Re-ingest to populate.")

    with st.expander("Full Raw State JSON"):
        st.json(selected_row.get("_full_state", {}))

# ---------------------------------------------------------------------------
# Run history
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Run History (Last 50)")

runs_df = load_runs_data()
if runs_df.empty:
    st.caption("No run records found.")
else:
    st.dataframe(
        runs_df,
        column_config={
            "run_id": st.column_config.TextColumn("Run ID"),
            "status": st.column_config.TextColumn("Status"),
            "notes": st.column_config.TextColumn("Notes"),
            "started_at": st.column_config.DatetimeColumn(
                "Started",
                format="MMM D, YYYY h:mm a",
            ),
            "updated_at": st.column_config.DatetimeColumn(
                "Updated",
                format="MMM D, YYYY h:mm a",
            ),
            "completed_at": st.column_config.DatetimeColumn(
                "Completed",
                format="MMM D, YYYY h:mm a",
            ),
        },
        use_container_width=True,
        hide_index=True,
    )
