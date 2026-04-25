"""
SQLite persistence layer for the JCM Distressed Asset Acquisition Pipeline.

Sprint 1: Single-process, single-thread SQLite usage with WAL mode.
If parallel workers are added later, use a connection-per-thread model
or migrate to PostgreSQL per SPEC.md Section 3 (Architecture Decisions).
"""

import json
import logging
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Optional, cast

from core.models import JCMPropertyState, PipelineStage
from core.settings import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(UTC).isoformat()


def make_expires_at(ttl_days: int) -> str:
    """Return an ISO 8601 expiry timestamp offset by ttl_days from now."""
    return (datetime.now(UTC) + timedelta(days=ttl_days)).isoformat()


def _normalize_db_path(db_path: Optional[str | Path] = None) -> Path:
    """Resolve the database path, falling back to settings, and ensure the
    parent directory exists."""
    settings = get_settings()
    resolved = Path(db_path) if db_path is not None else Path(settings.DATABASE_PATH)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


@contextmanager
def get_connection(
    db_path: Optional[str | Path] = None,
) -> Generator[sqlite3.Connection, None, None]:
    """
    Yield a SQLite connection with WAL mode and foreign key enforcement.

    The 'with conn:' block inside provides automatic commit on success
    and rollback on exception. The connection is closed on exit.
    """
    path = _normalize_db_path(db_path)
    conn = sqlite3.connect(path, timeout=15.0)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    try:
        with conn:
            yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------


def init_db(db_path: Optional[str | Path] = None) -> None:
    """
    Create all tables and indexes if they do not exist.
    Safe to call multiple times (idempotent).
    Also purges any expired cache entries on startup.
    """
    with get_connection(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id       TEXT PRIMARY KEY,
                started_at   TEXT NOT NULL,
                completed_at TEXT,
                updated_at   TEXT,
                status       TEXT NOT NULL,
                notes        TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                run_id         TEXT NOT NULL REFERENCES runs(run_id),
                property_id    TEXT NOT NULL,
                apn            TEXT,
                county         TEXT,
                pipeline_stage TEXT NOT NULL,
                created_at     TEXT NOT NULL,
                updated_at     TEXT NOT NULL,
                state_json     TEXT NOT NULL,
                PRIMARY KEY (run_id, property_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache (
                cache_key  TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        # Indexes
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_leads_apn_county
            ON leads (apn, county)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_leads_run_id
            ON leads (run_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_leads_run_id_stage
            ON leads (run_id, pipeline_stage)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cache_expires_at
            ON cache (expires_at)
            """
        )

    # Purge stale cache entries on every init (separate connection is fine in WAL mode).
    purge_expired_cache(db_path)


# ---------------------------------------------------------------------------
# Run operations
# ---------------------------------------------------------------------------


def insert_run(
    run_id: str,
    status: str = "started",
    notes: Optional[str] = None,
    db_path: Optional[str | Path] = None,
) -> None:
    """Insert a new run record. Raises IntegrityError if run_id already exists."""
    now = utc_now_iso()
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO runs (run_id, started_at, completed_at, updated_at, status, notes)
            VALUES (?, ?, NULL, ?, ?, ?)
            """,
            (run_id, now, now, status, notes),
        )


def get_active_run(
    db_path: Optional[str | Path] = None,
) -> Optional[dict[str, Any]]:
    """
    Return the most recently started run that is still in 'started' status,
    or None if no active run exists. Used by the scheduler to enforce
    RUN_LOCKED behavior (only one active run at a time).
    """
    with get_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT run_id, started_at, completed_at, updated_at, status, notes
            FROM runs
            WHERE status = 'started'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    return dict(row) if row else None


def update_run_status(
    run_id: str,
    status: str,
    completed_at: Optional[str] = None,
    notes: Optional[str] = None,
    db_path: Optional[str | Path] = None,
) -> None:
    """
    Update the status of a run. Sets updated_at to now.
    completed_at and notes are only updated when provided.
    """
    now = utc_now_iso()
    with get_connection(db_path) as conn:
        conn.execute(
            """
            UPDATE runs
            SET status       = ?,
                updated_at   = ?,
                completed_at = COALESCE(?, completed_at),
                notes        = COALESCE(?, notes)
            WHERE run_id = ?
            """,
            (status, now, completed_at, notes, run_id),
        )


def list_runs(
    db_path: Optional[str | Path] = None,
) -> list[dict[str, Any]]:
    """Return all runs ordered by most recent first."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT run_id, started_at, completed_at, updated_at, status, notes
            FROM runs
            ORDER BY started_at DESC
            """
        ).fetchall()
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Lead operations
# ---------------------------------------------------------------------------


def upsert_lead(
    state: JCMPropertyState,
    db_path: Optional[str | Path] = None,
) -> None:
    """
    Insert or update a lead row. On conflict (same run_id + property_id),
    updates all mutable fields but preserves the original created_at.
    This supports the resume-from-checkpoint pattern defined in SPEC.md Section 3.
    """
    now = utc_now_iso()
    state_json = state.model_dump_json()
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO leads (
                run_id, property_id, apn, county,
                pipeline_stage, created_at, updated_at, state_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, property_id) DO UPDATE SET
                apn            = excluded.apn,
                county         = excluded.county,
                pipeline_stage = excluded.pipeline_stage,
                updated_at     = excluded.updated_at,
                state_json     = excluded.state_json
            """,
            (
                state.run_id,
                state.property_id,
                state.apn,
                state.county,
                state.pipeline_stage,
                now,
                now,
                state_json,
            ),
        )


def _row_to_state(row: sqlite3.Row) -> JCMPropertyState:
    """Deserialize a leads row into a JCMPropertyState."""
    try:
        payload = json.loads(row["state_json"])
        return JCMPropertyState.model_validate(payload)
    except (json.JSONDecodeError, Exception) as exc:
        prop_id = row["property_id"] if "property_id" in row.keys() else "unknown"
        logger.error("Failed to deserialize lead %s: %s", prop_id, exc)
        raise ValueError(f"Corrupt state_json for lead {prop_id}") from exc


def get_lead(
    run_id: str,
    property_id: str,
    db_path: Optional[str | Path] = None,
) -> Optional[JCMPropertyState]:
    """Return a single lead by run_id + property_id, or None if not found."""
    with get_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT state_json
            FROM leads
            WHERE run_id = ? AND property_id = ?
            """,
            (run_id, property_id),
        ).fetchone()
    if row is None:
        return None
    return _row_to_state(row)


def get_leads_by_stage(
    run_id: str,
    stage: PipelineStage,
    db_path: Optional[str | Path] = None,
) -> list[JCMPropertyState]:
    """Return all leads for a run that are currently at the given pipeline stage."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT state_json
            FROM leads
            WHERE run_id = ? AND pipeline_stage = ?
            ORDER BY updated_at ASC
            """,
            (run_id, stage),
        ).fetchall()
    return [_row_to_state(row) for row in rows]


def list_incomplete_leads(
    run_id: str,
    db_path: Optional[str | Path] = None,
) -> list[JCMPropertyState]:
    """
    Return all leads for a run that have not reached a terminal stage.
    Terminal stages are: published, rejected, error.
    Used by the scheduler to resume interrupted runs.
    See SPEC.md Section 3 (State Persistence).
    """
    # Using fixed placeholders for the three known terminal stages avoids
    # f-string SQL while remaining explicit and linter-safe.
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT state_json
            FROM leads
            WHERE run_id = ?
              AND pipeline_stage NOT IN (?, ?, ?)
            ORDER BY updated_at ASC
            """,
            (run_id, "published", "rejected", "error"),
        ).fetchall()
    return [_row_to_state(row) for row in rows]


def find_recent_lead_by_apn(
    apn: str,
    county: str,
    lookback_days: int,
    db_path: Optional[str | Path] = None,
) -> Optional[JCMPropertyState]:
    """
    Return the most recent lead for this APN+county updated within lookback_days,
    regardless of pipeline_stage or run_id.

    IMPORTANT: This function does NOT filter by pipeline_stage. Callers are
    responsible for applying the cross-run dedup rules defined in SPEC.md
    Section 5.4:
      - If pipeline_stage in ('published', 'rejected') → skip the new lead.
      - If pipeline_stage in ('error', 'underwritten', 'normalized', 'enriched') → allow reprocessing.
    """
    cutoff = (datetime.now(UTC) - timedelta(days=lookback_days)).isoformat()
    with get_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT state_json
            FROM leads
            WHERE apn = ? AND county = ? AND updated_at >= ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (apn, county, cutoff),
        ).fetchone()
    if row is None:
        return None
    return _row_to_state(row)


def find_recent_lead_by_property_id(
    property_id: str,
    lookback_days: int,
    db_path: Optional[str | Path] = None,
) -> Optional[JCMPropertyState]:
    """
    Return the most recent lead for this property_id updated within lookback_days,
    regardless of pipeline_stage or run_id. Used by URL-keyed adapters (Apify,
    Craigslist) that don't have an APN at ingestion time.
    """
    cutoff = (datetime.now(UTC) - timedelta(days=lookback_days)).isoformat()
    with get_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT state_json
            FROM leads
            WHERE property_id = ? AND updated_at >= ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (property_id, cutoff),
        ).fetchone()
    if row is None:
        return None
    return _row_to_state(row)


def get_leads_for_publishing(
    run_id: str,
    db_path: Optional[str | Path] = None,
) -> list[JCMPropertyState]:
    """
    Return all underwritten leads for a run, sorted for publishing priority:
      1. 'approved' before 'manual_review' (ties broken by final_rank_score).
      2. Descending final_rank_score within each recommendation group.

    The caller (Node 9) is responsible for slicing to MAX_PUBLISH_PER_RUN.
    See SPEC.md Section 9 (Node 9 — CRM Publisher).
    """
    leads = get_leads_by_stage(run_id, cast(PipelineStage, "underwritten"), db_path)

    def sort_key(state: JCMPropertyState) -> tuple[int, float]:
        recommendation = state.decision.recommendation if state.decision else "rejected"
        approved_priority = 0 if recommendation == "approved" else 1
        rank = state.final_rank_score or 0.0
        return (approved_priority, -rank)

    return sorted(leads, key=sort_key)


# ---------------------------------------------------------------------------
# Cache operations
# ---------------------------------------------------------------------------


def get_cache(
    key: str,
    db_path: Optional[str | Path] = None,
) -> Optional[Any]:
    """
    Return a cached value by key, or None if missing or expired.
    Expired entries are deleted lazily on read (best-effort).
    Use purge_expired_cache() for bulk cleanup.
    """
    now = utc_now_iso()
    with get_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT value_json, expires_at
            FROM cache
            WHERE cache_key = ?
            """,
            (key,),
        ).fetchone()

        if row is None:
            return None

        if row["expires_at"] <= now:
            # Lazy delete — best-effort within this connection.
            conn.execute(
                "DELETE FROM cache WHERE cache_key = ?",
                (key,),
            )
            return None

        return json.loads(row["value_json"])


def set_cache(
    key: str,
    value: Any,
    expires_at: str,
    db_path: Optional[str | Path] = None,
) -> None:
    """
    Insert or replace a cache entry. expires_at must be an ISO 8601 UTC string.
    Use make_expires_at(ttl_days) to generate the expiry timestamp.
    See SPEC.md Section 6.1 for TTL values by data type.
    """
    now = utc_now_iso()
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO cache (cache_key, value_json, expires_at, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                value_json = excluded.value_json,
                expires_at = excluded.expires_at
            """,
            (key, json.dumps(value), expires_at, now)
        )


def purge_expired_cache(
    db_path: Optional[str | Path] = None,
) -> None:
    """
    Delete all cache entries where expires_at <= now.
    """
    now = utc_now_iso()
    with get_connection(db_path) as conn:
        conn.execute(
            """
            DELETE FROM cache
            WHERE expires_at <= ?
            """,
            (now,),
        )
