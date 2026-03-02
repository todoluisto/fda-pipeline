"""Monitoring dashboard for the FDA data pipeline.

A minimal Flask app that shows pipeline run history, allows triggering
manual runs, and runs the daily scheduler in the background. Run with:

    python -m fda_pipeline.monitor

Then visit http://localhost:5050
"""

from __future__ import annotations

import base64
import json
import logging
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import wraps

import schedule
from flask import Flask, Response, jsonify, render_template, request

from fda_pipeline import config
from fda_pipeline.loader import get_storage_backend
from fda_pipeline.pipeline import _configure_logging, _read_run_history, _write_run_history, run

app = Flask(__name__)
logger = logging.getLogger(__name__)

# Lock to prevent concurrent pipeline runs
_pipeline_lock = threading.Lock()

# In-memory schedule time (mutable at runtime via API)
_schedule_time: str = config.SCHEDULE_TIME


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def _scheduled_job() -> None:
    """Job executed by the scheduler — runs an incremental pipeline."""
    if _is_running():
        logger.info("Scheduler: skipping — pipeline already running")
        return
    logger.info("Scheduler: starting scheduled pipeline run")

    def _run_in_background():
        with _pipeline_lock:
            run(full_refresh=False)

    threading.Thread(target=_run_in_background, daemon=True).start()


def _reschedule(run_time: str) -> None:
    """Clear all scheduled jobs and register a new daily job at run_time."""
    global _schedule_time
    schedule.clear()
    schedule.every().day.at(run_time).do(_scheduled_job)
    _schedule_time = run_time
    logger.info("Scheduler: daily run set for %s", run_time)


def _scheduler_loop() -> None:
    """Background loop that checks for pending scheduled jobs."""
    while True:
        schedule.run_pending()
        time.sleep(30)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _next_scheduled_run() -> str:
    """Compute the next scheduled run time based on current schedule."""
    hour, minute = map(int, _schedule_time.split(":"))
    now = datetime.now()
    scheduled = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if scheduled <= now:
        scheduled += timedelta(days=1)
    return scheduled.isoformat()


def _is_running() -> bool:
    """Check if a pipeline run is currently in progress."""
    history = _read_run_history()
    return any(entry.get("status") == "running" for entry in history)


def _cleanup_stale_runs() -> None:
    """On startup, mark any lingering 'running' entries as failed.

    If the container was OOM-killed or crashed mid-run, the run entry is
    left in 'running' state in GCS forever. This runs once at startup (after
    a restart) to close those out so the dashboard doesn't stay stuck.
    """
    try:
        storage = get_storage_backend()
        history = _read_run_history(storage)
        stale = [e for e in history if e.get("status") == "running"]
        if not stale:
            return
        for entry in stale:
            entry["status"] = "failed"
            entry["error"] = "Process restarted unexpectedly (possible OOM or crash)"
            entry["finished_at"] = datetime.now(timezone.utc).isoformat()
            logger.warning("Marked stale run %s as failed (process had restarted)", entry["id"])
        _write_run_history(history, storage)
    except Exception:
        logger.exception("Failed to clean up stale run entries on startup")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def requires_auth(f):
    """HTTP Basic Auth decorator. Skipped entirely when DASHBOARD_PASSWORD is unset."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not config.DASHBOARD_PASSWORD:
            return f(*args, **kwargs)
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Basic "):
            try:
                _, password = base64.b64decode(auth[6:]).decode().split(":", 1)
                if password == config.DASHBOARD_PASSWORD:
                    return f(*args, **kwargs)
            except Exception:
                pass
        return Response(
            "Authentication required",
            401,
            {"WWW-Authenticate": 'Basic realm="FDA Pipeline Monitor"'},
        )
    return decorated


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
@requires_auth
def index():
    return render_template("dashboard.html")


@app.route("/api/status")
@requires_auth
def api_status():
    history = _read_run_history()
    return jsonify({
        "next_run": _next_scheduled_run(),
        "schedule_time": _schedule_time,
        "pipeline_running": any(e.get("status") == "running" for e in history),
        "runs": list(reversed(history)),  # newest first
    })


@app.route("/api/run", methods=["POST"])
def api_run():
    if _is_running():
        return jsonify({"error": "Pipeline is already running"}), 409

    full_refresh = request.args.get("full_refresh", "false").lower() == "true"

    def _run_in_background():
        with _pipeline_lock:
            run(full_refresh=full_refresh)

    thread = threading.Thread(target=_run_in_background, daemon=True)
    thread.start()

    return jsonify({"status": "started", "full_refresh": full_refresh})


@app.route("/api/schedule", methods=["POST"])
@requires_auth
def api_schedule():
    data = request.get_json(silent=True) or {}
    new_time = data.get("time", "").strip()

    if not re.match(r"^\d{2}:\d{2}$", new_time):
        return jsonify({"error": "Invalid time format. Use HH:MM (24h)."}), 400

    hour, minute = map(int, new_time.split(":"))
    if hour > 23 or minute > 59:
        return jsonify({"error": "Invalid time value."}), 400

    _reschedule(new_time)
    return jsonify({"schedule_time": _schedule_time, "next_run": _next_scheduled_run()})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    _configure_logging()

    # Clean up any runs left in 'running' state from a previous crash/OOM
    _cleanup_stale_runs()

    # Start the scheduler in a background thread
    _reschedule(_schedule_time)
    scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True)
    scheduler_thread.start()

    port = int(config.__dict__.get("MONITOR_PORT", 5050))
    logger.info("Starting monitoring dashboard on http://localhost:%d", port)
    logger.info("Scheduler active — pipeline will run daily at %s", _schedule_time)
    app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    main()
