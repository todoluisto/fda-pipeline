"""Pipeline orchestrator — ties together extract, transform, load, and scheduling.

This is the main entry point. It can be invoked:
  - From the CLI:  python -m fda_pipeline.pipeline [--full-refresh] [--run-now]
  - From Docker:   the container CMD calls this module
  - From a cloud handler (Cloud Run / Lambda): import and call ``run()``
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import uuid
from datetime import datetime, timezone

from fda_pipeline import config
from fda_pipeline.extractor import extract
from fda_pipeline.transformer import transform
from fda_pipeline.loader import get_storage_backend, load
from fda_pipeline.scheduler.local import LocalScheduler
from fda_pipeline.scheduler.cloud import CloudScheduler
from fda_pipeline.storage.base import StorageBackend

logger = logging.getLogger("fda_pipeline")


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _configure_logging() -> None:
    """Set up root logger with console (and optionally file) handlers."""
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler — stdout is captured by Cloud Run / Docker logs
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(config.LOG_LEVEL)
    root.addHandler(console_handler)

    # File handler — local mode only (Cloud Run logs go to stdout)
    if config.STORAGE_BACKEND != "cloud":
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(config.LOG_FILE)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)


# ---------------------------------------------------------------------------
# State file helpers — all routed through the storage backend
# ---------------------------------------------------------------------------

def _read_last_run_date(storage: StorageBackend) -> str | None:
    """Read the last successful run date from last_run.json.

    Returns:
        YYYYMMDD string, or None if no previous run exists.
    """
    data = storage.read_json("last_run.json")
    if not data:
        return None
    return data.get("last_run_date")


def _write_last_run_date(date_str: str, storage: StorageBackend) -> None:
    """Persist the last successful run date."""
    storage.write_json({
        "last_run_date": date_str,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }, "last_run.json")
    logger.info("Updated last_run_date to %s", date_str)


def _read_run_history(storage: StorageBackend | None = None) -> list[dict]:
    """Read the run history.

    Args:
        storage: Optional backend. If None, one is created automatically.
            Pass None when calling from monitor.py (no storage context yet).
    """
    if storage is None:
        storage = get_storage_backend()
    data = storage.read_json("run_history.json")
    if data is None:
        return []
    return data if isinstance(data, list) else []


def _write_run_history(history: list[dict], storage: StorageBackend) -> None:
    """Write run history, keeping the last 100 entries."""
    storage.write_json(history[-100:], "run_history.json")


def _start_run_entry(full_refresh: bool, storage: StorageBackend) -> str:
    """Create a 'running' entry in run history. Returns the run ID."""
    run_id = str(uuid.uuid4())[:8]
    history = _read_run_history(storage)
    history.append({
        "id": run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "status": "running",
        "full_refresh": full_refresh,
        "records_extracted": None,
        "rows_loaded": None,
        "error": None,
    })
    _write_run_history(history, storage)
    return run_id


def _finish_run_entry(
    run_id: str,
    storage: StorageBackend,
    *,
    status: str,
    duration: float,
    records_extracted: int = 0,
    rows_loaded: int = 0,
    error: str | None = None,
) -> None:
    """Update a run entry with final results."""
    history = _read_run_history(storage)
    for entry in history:
        if entry["id"] == run_id:
            entry["finished_at"] = datetime.now(timezone.utc).isoformat()
            entry["duration_seconds"] = round(duration, 1)
            entry["status"] = status
            entry["records_extracted"] = records_extracted
            entry["rows_loaded"] = rows_loaded
            entry["error"] = error
            break
    _write_run_history(history, storage)


# ---------------------------------------------------------------------------
# Data quality validation
# ---------------------------------------------------------------------------

def _validate(rows: list[dict], full_refresh: bool) -> None:
    """Run data quality checks on transformed rows.

    Logs warnings for issues but does not block the pipeline — partial
    data today is better than no data for the dashboard.
    """
    if not rows:
        return

    total = len(rows)
    logger.info("Running data quality checks on %d rows", total)

    null_app_type = sum(1 for r in rows if not r.get("application_type"))
    if null_app_type > 0:
        logger.warning(
            "DATA QUALITY: %d / %d rows have empty application_type (%.1f%%)",
            null_app_type, total, 100 * null_app_type / total,
        )

    null_strength = sum(1 for r in rows if not r.get("strength"))
    if null_strength > total * 0.5:
        logger.warning(
            "DATA QUALITY: %d / %d rows have empty strength (%.1f%%)",
            null_strength, total, 100 * null_strength / total,
        )

    if full_refresh:
        unique_apps = len({r.get("application_number", "") for r in rows})
        if unique_apps < 25000:
            logger.warning(
                "DATA QUALITY: Full refresh yielded only %d unique applications. "
                "Expected ~28,000+. Some partitions may have failed.",
                unique_apps,
            )

    logger.info("Data quality checks complete")


# ---------------------------------------------------------------------------
# Core run logic
# ---------------------------------------------------------------------------

def run(full_refresh: bool = False) -> None:
    """Execute one pipeline run.

    Args:
        full_refresh: If True, ignore the last run date and fetch all records.

    CLOUD MIGRATION NOTE: This function is the handler entry point for
    Cloud Run. Call it directly from your cloud handler or via /api/run.
    """
    start_time = time.monotonic()
    today = datetime.now().strftime("%Y%m%d")
    today_pretty = datetime.now().strftime("%Y-%m-%d")

    # Initialise storage first — all state files go through this backend
    storage = get_storage_backend()
    run_id = _start_run_entry(full_refresh, storage)
    records_extracted = 0
    rows_loaded = 0

    logger.info("=" * 60)
    logger.info("Pipeline run started — full_refresh=%s  run_id=%s", full_refresh, run_id)
    logger.info("=" * 60)

    try:
        # Determine date range
        since_date: str | None = None
        if not full_refresh:
            since_date = _read_last_run_date(storage)
            if since_date:
                logger.info("Incremental run — fetching records since %s", since_date)
            else:
                logger.info("No previous run found — performing full historical pull")

        # Extract
        raw_records = extract(since_date=since_date)
        records_extracted = len(raw_records)
        if not raw_records:
            logger.info("No new records returned — nothing to process")
            elapsed = time.monotonic() - start_time
            logger.info("Pipeline finished in %.1f seconds (0 records)", elapsed)
            _finish_run_entry(run_id, storage, status="success", duration=elapsed)
            return

        # Transform
        flat_rows = transform(raw_records)

        # Validate
        _validate(flat_rows, full_refresh)

        # Load
        incremental_filename = f"{config.INCREMENTAL_OUTPUT_PREFIX}{today_pretty}.csv"
        rows_loaded = load(
            rows=flat_rows,
            storage=storage,
            full_filename=config.FULL_OUTPUT_FILENAME,
            incremental_filename=incremental_filename,
        )

        # Update last-run bookmark
        _write_last_run_date(today, storage)

        elapsed = time.monotonic() - start_time
        logger.info(
            "Pipeline finished successfully in %.1f seconds — %d incremental rows loaded",
            elapsed, rows_loaded,
        )
        _finish_run_entry(
            run_id, storage, status="success", duration=elapsed,
            records_extracted=records_extracted, rows_loaded=rows_loaded,
        )

    except Exception as exc:
        elapsed = time.monotonic() - start_time
        logger.exception("Pipeline run FAILED after %.1f seconds", elapsed)
        _finish_run_entry(
            run_id, storage, status="failed", duration=elapsed,
            records_extracted=records_extracted, rows_loaded=rows_loaded,
            error=str(exc),
        )


# ---------------------------------------------------------------------------
# CLI & scheduling
# ---------------------------------------------------------------------------

def _get_scheduler():
    """Return the configured scheduler backend.

    CLOUD MIGRATION NOTE: Set SCHEDULER_BACKEND=cloud to use the no-op
    CloudScheduler. Cloud Scheduler (GCP) calls POST /api/run on the
    monitor service instead of running an in-process loop.
    """
    if config.SCHEDULER_BACKEND == "cloud":
        return CloudScheduler()
    return LocalScheduler()


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="FDA Drug Approval Data Pipeline",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Force a complete re-pull of all records (ignore last_run.json)",
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Run the pipeline immediately instead of waiting for the schedule",
    )
    args = parser.parse_args()

    _configure_logging()
    logger.info("Pipeline starting — config: backend=%s, scheduler=%s",
                config.STORAGE_BACKEND, config.SCHEDULER_BACKEND)

    scheduler = _get_scheduler()

    if args.run_now or args.full_refresh:
        scheduler.run_once(lambda: run(full_refresh=args.full_refresh))
    else:
        logger.info("Entering scheduled mode — will run daily at %s", config.SCHEDULE_TIME)
        scheduler.start(lambda: run(full_refresh=False), config.SCHEDULE_TIME)


if __name__ == "__main__":
    main()
