# main.py
"""
JCM Pipeline Runner — Sprint 1

Two execution modes:
    1. Full pipeline  — Node 1 (PropStream ingestion) + Nodes 2-9.
    2. Adapter pipeline — run_all_adapters() (top-of-funnel) + Nodes 2-9
       for each adapter run that produces "sourced" leads.

Nodes read and write from SQLite via pipeline_stage and data_sources.
Each node is idempotent — safe to re-run after a partial failure.
"""

import argparse
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

from core.database import init_db, insert_run, update_run_status, get_leads_by_stage
from core.settings import get_settings

# Node entry points — names must match exactly
from nodes.node_01_ingestion import process_ingestion
from nodes.node_02_normalization import process_normalization
from nodes.node_03_distress import run_distress_scoring
from nodes.node_04_enrichment import run_enrichment
from nodes.node_05_arv import run_arv_estimation
from nodes.node_06_images import run_image_retrieval
from nodes.node_07_vision import run_vision_analysis
from nodes.node_08_underwriting import run_underwriting
from nodes.node_09_publisher import run_publisher

# Adapter registry
from adapters import run_all_adapters

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("jcm_pipeline")


# ---------------------------------------------------------------------------
# Nodes 2-9 runner (shared between full pipeline and adapter pipeline)
# ---------------------------------------------------------------------------

def _run_nodes_2_through_9(
    run_id: str,
    db_path: Path,
    all_metrics: dict[str, dict],
) -> None:
    """
    Execute Nodes 2-9 sequentially for the given run_id.

    Expects "sourced" leads to already exist in the database under run_id.
    Mutates all_metrics in-place with per-node results.
    """
    # --- Node 2: Normalization ---
    # Note: process_normalization() only accepts run_id (no db_path param)
    logger.info("--- Node 2: Normalization ---")
    m2 = process_normalization(run_id)
    all_metrics["node_02"] = m2
    logger.info("Node 2 complete: %s", m2)

    # --- Node 3: Distress Scoring ---
    logger.info("--- Node 3: Distress Scoring ---")
    m3 = run_distress_scoring(run_id, db_path)
    all_metrics["node_03"] = m3
    logger.info("Node 3 complete: %s", m3)

    # --- Node 4: Property Enrichment ---
    logger.info("--- Node 4: Property Enrichment ---")
    m4 = run_enrichment(run_id, db_path)
    all_metrics["node_04"] = m4
    logger.info("Node 4 complete: %s", m4)

    # --- Node 5: ARV Estimation ---
    logger.info("--- Node 5: ARV Estimation ---")
    m5 = run_arv_estimation(run_id, db_path)
    all_metrics["node_05"] = m5
    logger.info("Node 5 complete: %s", m5)

    # --- Node 6: Image Collection ---
    logger.info("--- Node 6: Image Collection ---")
    m6 = run_image_retrieval(run_id, db_path)
    all_metrics["node_06"] = m6
    logger.info("Node 6 complete: %s", m6)

    # --- Node 7: Vision Analysis ---
    logger.info("--- Node 7: Vision Analysis ---")
    m7 = run_vision_analysis(run_id, db_path)
    all_metrics["node_07"] = m7
    logger.info("Node 7 complete: %s", m7)

    # --- Node 8: Underwriting ---
    logger.info("--- Node 8: Underwriting ---")
    m8 = run_underwriting(run_id, db_path)
    all_metrics["node_08"] = m8
    logger.info("Node 8 complete: %s", m8)

    # --- Node 9: CRM Publisher ---
    logger.info("--- Node 9: CRM Publisher ---")
    m9 = run_publisher(run_id, db_path)
    all_metrics["node_09"] = m9
    logger.info("Node 9 complete: %s", m9)


# ---------------------------------------------------------------------------
# Full pipeline (Node 1 PropStream ingestion + Nodes 2-9)
# ---------------------------------------------------------------------------

def run_pipeline(run_id: str | None = None) -> dict:
    """
    Execute the full 9-node JCM pipeline for one run.

    Args:
        run_id: Optional explicit run ID. Defaults to timestamp-based ID.

    Returns:
        Dict of per-node metrics.
    """
    settings = get_settings()
    db_path: Path = settings.DATABASE_PATH

    if run_id is None:
        run_id = f"run_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"

    logger.info("Starting JCM Pipeline | run_id=%s", run_id)
    start_time = time.time()

    # --- Initialize database ---
    init_db(db_path)
    logger.info("Database initialized at %s", db_path)

    # Register run in runs table — required by FK constraint on leads
    insert_run(run_id=run_id, status="started", notes="Full pipeline run", db_path=db_path)
    logger.info("Run registered: %s", run_id)

    all_metrics: dict[str, dict] = {}

    try:
        # --- Node 1: Ingestion ---
        logger.info("--- Node 1: Ingestion ---")
        m1 = process_ingestion(run_id, db_path)
        all_metrics["node_01"] = m1
        logger.info("Node 1 complete: %s", m1)

        if m1.get("leads_ingested", 0) == 0:
            logger.warning(
                "Node 1 ingested 0 leads — skipping remaining nodes. "
                "Check API credentials and sourcing filters."
            )
            update_run_status(
                run_id=run_id,
                status="completed_empty",
                completed_at=datetime.now(UTC).isoformat(),
                notes="0 leads ingested — pipeline halted early",
                db_path=db_path,
            )
            _print_summary(run_id, all_metrics, time.time() - start_time)
            return all_metrics

        # --- Nodes 2-9 ---
        _run_nodes_2_through_9(run_id, db_path, all_metrics)

        # --- Mark run complete ---
        m9 = all_metrics.get("node_09", {})
        update_run_status(
            run_id=run_id,
            status="completed",
            completed_at=datetime.now(UTC).isoformat(),
            notes=(
                f"Published: {m9.get('published', 0)}, "
                f"Errors across all nodes: "
                f"{sum(m.get('errors', 0) for m in all_metrics.values())}"
            ),
            db_path=db_path,
        )

    except Exception as exc:
        logger.exception("Pipeline failed with unhandled exception: %s", exc)
        update_run_status(
            run_id=run_id,
            status="failed",
            completed_at=datetime.now(UTC).isoformat(),
            notes=f"Unhandled exception: {exc}",
            db_path=db_path,
        )
        raise

    elapsed = time.time() - start_time
    _print_summary(run_id, all_metrics, elapsed)
    return all_metrics


# ---------------------------------------------------------------------------
# Adapter pipeline (top-of-funnel adapters + Nodes 2-9)
# ---------------------------------------------------------------------------

def run_adapter_pipeline(
    dry_run: bool = False,
    max_items: int | None = None,
    only: list[str] | None = None,
    skip: list[str] | None = None,
) -> dict[str, dict]:
    """
    Run all registered adapters, then process their leads through Nodes 2-9.

    Adapters write 'sourced' leads under their own run_ids. This function
    discovers those run_ids from the adapter metrics and feeds each one
    through the downstream pipeline nodes.

    Returns:
        Combined metrics: adapter metrics + per-run node metrics.
    """
    settings = get_settings()
    db_path: Path = settings.DATABASE_PATH

    logger.info("Starting Adapter Pipeline")
    start_time = time.time()

    init_db(db_path)
    logger.info("Database initialized at %s", db_path)

    combined_metrics: dict[str, dict] = {}

    # --- Phase 1: Run adapters ---
    logger.info("=" * 50)
    logger.info("Phase 1: Running top-of-funnel adapters...")
    logger.info("=" * 50)

    adapter_metrics = run_all_adapters(
        dry_run=dry_run,
        max_items=max_items,
        only=only,
        skip=skip,
    )
    combined_metrics["adapters"] = adapter_metrics
    logger.info("Adapter phase complete: %s", adapter_metrics)

    if dry_run:
        logger.info("Dry run mode — skipping Nodes 2-9.")
        _print_adapter_summary(combined_metrics, time.time() - start_time)
        return combined_metrics

    # --- Phase 2: Discover adapter run_ids and process through Nodes 2-9 ---
    # Adapter run_ids follow the pattern "adapter_cl_*", "adapter_fb_*", etc.
    # We find them by scanning the runs table for recent adapter runs that
    # have sourced leads.
    logger.info("=" * 50)
    logger.info("Phase 2: Processing adapter leads through Nodes 2-9...")
    logger.info("=" * 50)

    # Collect run_ids from the database that have "sourced" leads
    # from adapter runs (started in the last few minutes)
    from core.database import get_connection
    adapter_run_ids: list[str] = []
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT r.run_id
            FROM runs r
            JOIN leads l ON r.run_id = l.run_id
            WHERE r.run_id LIKE 'adapter_%'
              AND r.status IN ('started', 'adapter_complete')
              AND l.pipeline_stage = 'sourced'
            ORDER BY r.started_at DESC
            """
        ).fetchall()
        adapter_run_ids = [row["run_id"] for row in rows]

    if not adapter_run_ids:
        logger.warning(
            "No adapter runs with sourced leads found. "
            "Adapters may have failed or produced no leads."
        )
        _print_adapter_summary(combined_metrics, time.time() - start_time)
        return combined_metrics

    logger.info("Found %d adapter run(s) with sourced leads: %s",
                len(adapter_run_ids), adapter_run_ids)

    for adapter_run_id in adapter_run_ids:
        sourced = get_leads_by_stage(adapter_run_id, "sourced", db_path)
        logger.info(
            "Processing adapter run %s (%d sourced leads)",
            adapter_run_id, len(sourced),
        )

        if not sourced:
            continue

        run_metrics: dict[str, dict] = {}
        try:
            _run_nodes_2_through_9(adapter_run_id, db_path, run_metrics)

            # Mark the adapter run as fully processed
            m9 = run_metrics.get("node_09", {})
            update_run_status(
                run_id=adapter_run_id,
                status="completed",
                completed_at=datetime.now(UTC).isoformat(),
                notes=(
                    f"Adapter leads processed through Nodes 2-9. "
                    f"Published: {m9.get('published', 0)}, "
                    f"Errors: {sum(m.get('errors', 0) for m in run_metrics.values())}"
                ),
                db_path=db_path,
            )
        except Exception as exc:
            logger.exception(
                "Pipeline failed for adapter run %s: %s", adapter_run_id, exc
            )
            update_run_status(
                run_id=adapter_run_id,
                status="failed",
                completed_at=datetime.now(UTC).isoformat(),
                notes=f"Unhandled exception in Nodes 2-9: {exc}",
                db_path=db_path,
            )

        combined_metrics[adapter_run_id] = run_metrics

    elapsed = time.time() - start_time
    _print_adapter_summary(combined_metrics, elapsed)
    return combined_metrics


# ---------------------------------------------------------------------------
# Summary printers
# ---------------------------------------------------------------------------

def _print_summary(
    run_id: str,
    metrics: dict[str, dict],
    elapsed: float,
) -> None:
    """Print a structured run summary to stdout."""
    labels = {
        "node_01": "Ingestion      ",
        "node_02": "Normalization  ",
        "node_03": "Distress       ",
        "node_04": "Enrichment     ",
        "node_05": "ARV Estimation ",
        "node_06": "Images         ",
        "node_07": "Vision         ",
        "node_08": "Underwriting   ",
        "node_09": "CRM Publisher  ",
    }

    print("\n" + "=" * 60)
    print(f"  JCM PIPELINE COMPLETE  |  run_id: {run_id}")
    print(f"  Elapsed: {elapsed:.2f}s")
    print("=" * 60)
    for key, label in labels.items():
        m = metrics.get(key, {})
        if m:
            print(f"  Node {key[-2:]} {label}: {m}")
    print("=" * 60 + "\n")


def _print_adapter_summary(
    metrics: dict[str, dict],
    elapsed: float,
) -> None:
    """Print a summary for the adapter pipeline run."""
    print("\n" + "=" * 60)
    print("  JCM ADAPTER PIPELINE COMPLETE")
    print(f"  Elapsed: {elapsed:.2f}s")
    print("=" * 60)

    # Adapter phase
    adapter_m = metrics.get("adapters", {})
    if adapter_m:
        print("  --- Adapter Results ---")
        for name, m in adapter_m.items():
            print(f"    {name}: {m}")

    # Node processing per adapter run
    node_labels = {
        "node_02": "Normalization  ",
        "node_03": "Distress       ",
        "node_04": "Enrichment     ",
        "node_05": "ARV Estimation ",
        "node_06": "Images         ",
        "node_07": "Vision         ",
        "node_08": "Underwriting   ",
        "node_09": "CRM Publisher  ",
    }

    for key, val in metrics.items():
        if key.startswith("adapter_") and key != "adapters":
            print(f"  --- Pipeline for run: {key} ---")
            for node_key, label in node_labels.items():
                m = val.get(node_key, {})
                if m:
                    print(f"    Node {node_key[-2:]} {label}: {m}")

    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="JCM Pipeline Runner — Sprint 1"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "adapters"],
        default="full",
        help=(
            "'full' runs Node 1 (PropStream) + Nodes 2-9. "
            "'adapters' runs top-of-funnel adapters + Nodes 2-9. "
            "Default: full."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="(adapters mode only) Parse but skip DB writes.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="(adapters mode only) Process at most N items per adapter.",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        default=None,
        help="(adapters mode only) Run only these adapter names.",
    )
    parser.add_argument(
        "--skip",
        nargs="+",
        default=None,
        help="(adapters mode only) Skip these adapter names.",
    )

    args = parser.parse_args()

    if args.mode == "adapters":
        run_adapter_pipeline(
            dry_run=args.dry_run,
            max_items=args.max_items,
            only=args.only,
            skip=args.skip,
        )
    else:
        run_pipeline()