# tests/test_adapter_contract.py
"""
Adapter contract tests — verify standardized behavior across all adapters:
  1. Dry-run performs zero DB writes
  2. Successful run marks status as adapter_complete
  3. Standardized metric keys exist
  4. Dedupe is consistent
  5. Mapping produces valid JCMPropertyState
  6. raw_address stays clean
"""

import sqlite3
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from core.database import init_db, get_connection, insert_run
from core.settings import get_settings
from adapters.common import (
    STANDARD_METRIC_KEYS,
    make_metrics,
    should_skip_lead,
    start_run,
    complete_run,
    fail_run,
    generate_run_id,
    persist_lead,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path: Path, monkeypatch):
    """Create a temporary DB and point settings at it."""
    db_file = tmp_path / "test_pipeline.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_file))
    from core.settings import get_settings
    get_settings.cache_clear()
    init_db(db_file)
    yield db_file
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# 1. Dry-run: zero DB writes for each adapter
# ---------------------------------------------------------------------------

class TestDryRunZeroWrites:
    """Verify dry-run performs zero DB writes for each adapter."""

    def test_craigslist_dry_run_zero_writes(self, tmp_db):
        from adapters.craigslist_rss import run_craigslist_adapter
        metrics = run_craigslist_adapter(dry_run=True, max_items=2)

        # Should have fetched mock data
        assert metrics["items_fetched"] > 0

        # Verify zero rows in leads and runs tables
        with get_connection(tmp_db) as conn:
            leads = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
            runs = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
        assert leads == 0, f"Dry-run wrote {leads} lead rows"
        assert runs == 0, f"Dry-run wrote {runs} run rows"

    def test_apify_dry_run_zero_writes(self, tmp_db):
        from adapters.apify_facebook import run_apify_facebook_adapter
        metrics = run_apify_facebook_adapter(dry_run=True, max_items=2)

        assert metrics["items_fetched"] > 0

        with get_connection(tmp_db) as conn:
            leads = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
            runs = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
        assert leads == 0
        assert runs == 0

    def test_firecrawl_dry_run_zero_writes(self, tmp_db):
        from adapters.firecrawl_county import run_firecrawl_county_adapter
        metrics = run_firecrawl_county_adapter(dry_run=True, max_items=2)

        assert metrics["items_fetched"] > 0

        with get_connection(tmp_db) as conn:
            leads = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
            runs = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
        assert leads == 0
        assert runs == 0


# ---------------------------------------------------------------------------
# 2. Successful run marks status as adapter_complete
# ---------------------------------------------------------------------------

class TestRunLifecycle:

    def test_successful_run_marks_complete(self, tmp_db):
        run_id = "test_lifecycle_001"
        start_run(run_id, notes="test", dry_run=False)
        metrics = make_metrics()
        complete_run(run_id, metrics, dry_run=False)

        with get_connection(tmp_db) as conn:
            row = conn.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        assert row is not None
        assert row["status"] == "adapter_complete"

    def test_failed_run_marks_failed(self, tmp_db):
        run_id = "test_lifecycle_002"
        start_run(run_id, notes="test", dry_run=False)
        fail_run(run_id, "boom", dry_run=False)

        with get_connection(tmp_db) as conn:
            row = conn.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        assert row is not None
        assert row["status"] == "failed"

    def test_dry_run_lifecycle_no_db(self, tmp_db):
        run_id = "test_lifecycle_003"
        start_run(run_id, notes="test", dry_run=True)
        complete_run(run_id, make_metrics(), dry_run=True)

        with get_connection(tmp_db) as conn:
            row = conn.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        assert row is None  # no row created in dry-run


# ---------------------------------------------------------------------------
# 3. Standardized metric keys
# ---------------------------------------------------------------------------

class TestMetricKeys:

    def test_make_metrics_has_standard_keys(self):
        m = make_metrics()
        for key in STANDARD_METRIC_KEYS:
            assert key in m, f"Missing standard key: {key}"
            assert m[key] == 0

    def test_make_metrics_allows_extras(self):
        m = make_metrics(filtered_out=0)
        assert "filtered_out" in m
        for key in STANDARD_METRIC_KEYS:
            assert key in m

    def test_craigslist_returns_standard_keys(self, tmp_db):
        from adapters.craigslist_rss import run_craigslist_adapter
        metrics = run_craigslist_adapter(dry_run=True, max_items=1)
        for key in STANDARD_METRIC_KEYS:
            assert key in metrics, f"CL missing: {key}"

    def test_apify_returns_standard_keys(self, tmp_db):
        from adapters.apify_facebook import run_apify_facebook_adapter
        metrics = run_apify_facebook_adapter(dry_run=True, max_items=1)
        for key in STANDARD_METRIC_KEYS:
            assert key in metrics, f"Apify missing: {key}"

    def test_firecrawl_returns_standard_keys(self, tmp_db):
        from adapters.firecrawl_county import run_firecrawl_county_adapter
        metrics = run_firecrawl_county_adapter(dry_run=True, max_items=1)
        for key in STANDARD_METRIC_KEYS:
            assert key in metrics, f"FC missing: {key}"


# ---------------------------------------------------------------------------
# 4. Dedupe behavior
# ---------------------------------------------------------------------------

class TestDedupe:

    def test_same_run_dedupe(self, tmp_db):
        seen: set[str] = set()
        pid = "test_prop_001"
        rid = "test_run_001"

        # First time: not a duplicate
        assert should_skip_lead(pid, rid, seen_this_run=seen) is False
        # Second time same run: duplicate
        assert should_skip_lead(pid, rid, seen_this_run=seen) is True

    def test_cross_run_terminal_skipped(self, tmp_db):
        """Lead in 'published' stage from another run should be skipped."""
        from core.models import JCMPropertyState
        from core.database import upsert_lead

        pid = "cross_run_pub_001"
        old_run = "old_run_001"
        insert_run(old_run, status="adapter_complete", db_path=tmp_db)

        state = JCMPropertyState(
            run_id=old_run,
            property_id=pid,
            pipeline_stage="published",
            county="Santa Clara",
            raw_address="123 Test St",
        )
        upsert_lead(state, db_path=tmp_db)

        seen: set[str] = set()
        assert should_skip_lead(pid, "new_run_001", seen_this_run=seen) is True

    def test_cross_run_error_allowed(self, tmp_db):
        """Lead in 'error' stage from another run should be allowed to refresh."""
        from core.models import JCMPropertyState
        from core.database import upsert_lead

        pid = "cross_run_err_001"
        old_run = "old_run_002"
        insert_run(old_run, status="adapter_complete", db_path=tmp_db)

        state = JCMPropertyState(
            run_id=old_run,
            property_id=pid,
            pipeline_stage="error",
            county="Santa Clara",
            raw_address="456 Test Ave",
        )
        upsert_lead(state, db_path=tmp_db)

        seen: set[str] = set()
        assert should_skip_lead(pid, "new_run_002", seen_this_run=seen) is False


# ---------------------------------------------------------------------------
# 5. Mapping produces valid JCMPropertyState
# ---------------------------------------------------------------------------

class TestMapping:

    def test_firecrawl_mapping_valid_state(self):
        from adapters.firecrawl_county import map_record_to_state
        record = {
            "apn": "264-16-027",
            "address": "645 Meridian Ave, San Jose, CA 95126",
            "tax_due": 18750.00,
            "owner_name": "ESTATE OF J. MARTINEZ",
            "tax_year": "2023-2024",
        }
        state = map_record_to_state(record, run_id="test_map_001")
        assert state.pipeline_stage == "sourced"
        assert state.county == "Santa Clara"
        assert state.apn == "26416027"
        assert len(state.distress_signals) == 1
        assert state.distress_signals[0].signal_type == "tax_delinquency"

    def test_craigslist_mock_produces_valid_states(self):
        from adapters.craigslist_rss import fetch_mock_craigslist_data
        items = fetch_mock_craigslist_data()
        assert len(items) > 0
        for item in items:
            assert "title" in item
            assert "link" in item


# ---------------------------------------------------------------------------
# 6. raw_address hygiene (Firecrawl)
# ---------------------------------------------------------------------------

class TestRawAddressHygiene:

    def test_raw_address_no_owner_metadata(self):
        """raw_address must not contain owner name."""
        from adapters.firecrawl_county import map_record_to_state
        record = {
            "apn": "415-29-038",
            "address": "3280 Cabrillo Ave, Santa Clara, CA 95051",
            "tax_due": 12400.00,
            "owner_name": "NGUYEN FAMILY TRUST",
            "tax_year": "2022-2024",
        }
        state = map_record_to_state(record, run_id="test_addr_001")
        assert "NGUYEN" not in state.raw_address
        assert "Owner" not in state.raw_address
        assert "Cabrillo" in state.raw_address

    def test_raw_address_no_owner_when_no_street(self):
        """Even without a real address, owner should not be in raw_address."""
        from adapters.firecrawl_county import map_record_to_state
        record = {
            "apn": "264-16-099",
            "address": "",
            "owner_name": "DOE, JOHN",
        }
        state = map_record_to_state(record, run_id="test_addr_002")
        assert "DOE" not in state.raw_address
        assert "Owner" not in state.raw_address
        assert "APN" in state.raw_address  # fallback format


# ---------------------------------------------------------------------------
# 7. Mock fallback still works
# ---------------------------------------------------------------------------

class TestMockFallback:

    def test_craigslist_mock_works(self, tmp_db):
        from adapters.craigslist_rss import run_craigslist_adapter
        metrics = run_craigslist_adapter(dry_run=True, max_items=2)
        assert metrics["items_fetched"] > 0
        assert metrics["leads_ingested_new"] > 0

    def test_apify_mock_works(self, tmp_db):
        from adapters.apify_facebook import run_apify_facebook_adapter
        metrics = run_apify_facebook_adapter(dry_run=True, max_items=2)
        assert metrics["items_fetched"] > 0
        assert metrics["leads_ingested_new"] > 0

    def test_firecrawl_mock_works(self, tmp_db):
        from adapters.firecrawl_county import run_firecrawl_county_adapter
        metrics = run_firecrawl_county_adapter(dry_run=True, max_items=2)
        assert metrics["items_fetched"] > 0
        assert metrics["leads_ingested_new"] > 0
