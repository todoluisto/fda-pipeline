"""Centralized configuration loaded from environment variables.

All values that change between environments (local, staging, production)
are read from a .env file via python-dotenv. Never hardcode these values.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


# --- API Configuration ---

# OpenFDA Drugs@FDA endpoint
API_BASE_URL: str = os.getenv(
    "FDA_API_BASE_URL", "https://api.fda.gov/drug/drugsfda.json"
)

# Optional API key for higher rate limits (register at https://open.fda.gov/apis/authentication/)
API_KEY: str | None = os.getenv("FDA_API_KEY")

# Maximum records per request (OpenFDA hard limit is 1000)
API_PAGE_SIZE: int = int(os.getenv("FDA_API_PAGE_SIZE", "1000"))

# Retry settings for failed API calls
API_MAX_RETRIES: int = int(os.getenv("FDA_API_MAX_RETRIES", "3"))
API_RETRY_BACKOFF_BASE: float = float(os.getenv("FDA_API_RETRY_BACKOFF_BASE", "2.0"))

# Application-number prefix partitions for full extraction.
# Each partition must return fewer than 25,000 records so that
# skip/limit pagination works within it. The union of all
# partitions covers the entire dataset.
API_PARTITIONS: list[str] = [
    "application_number:ANDA0*",
    "application_number:ANDA2*",
    "application_number:NDA0*",
    "application_number:NDA2*",
    "application_number:BLA*",
]


# --- Storage Configuration ---

# CLOUD MIGRATION NOTE: Change STORAGE_BACKEND to "cloud" and set the
# appropriate CLOUD_STORAGE_BUCKET / CLOUD_STORAGE_PREFIX when deploying
# to AWS S3 or GCP GCS.
STORAGE_BACKEND: str = os.getenv("STORAGE_BACKEND", "local")

# Local storage settings
DATA_DIR: Path = Path(os.getenv("DATA_DIR", str(Path(__file__).resolve().parent.parent / "fda_data")))

# Cloud storage settings (used only when STORAGE_BACKEND=cloud)
CLOUD_STORAGE_PROVIDER: str = os.getenv("CLOUD_STORAGE_PROVIDER", "s3")  # "s3" or "gcs"
CLOUD_STORAGE_BUCKET: str = os.getenv("CLOUD_STORAGE_BUCKET", "")
CLOUD_STORAGE_PREFIX: str = os.getenv("CLOUD_STORAGE_PREFIX", "fda_data/")


# --- Scheduler Configuration ---

# CLOUD MIGRATION NOTE: Change SCHEDULER_BACKEND to "cloud" when deploying.
# In cloud mode, the scheduler does nothing — the cloud platform (EventBridge,
# Cloud Scheduler) triggers the pipeline entry point directly.
SCHEDULER_BACKEND: str = os.getenv("SCHEDULER_BACKEND", "local")

# Time to run the nightly job (HH:MM in local time, 24h format)
SCHEDULE_TIME: str = os.getenv("SCHEDULE_TIME", "02:00")


# --- Pipeline State ---

# Path to the JSON file that tracks the last successful run date
LAST_RUN_FILE: Path = Path(os.getenv(
    "LAST_RUN_FILE",
    str(DATA_DIR / "last_run.json"),
))

# Structured run history for the monitoring dashboard
RUN_HISTORY_FILE: Path = Path(os.getenv(
    "RUN_HISTORY_FILE",
    str(DATA_DIR / "run_history.json"),
))


# --- Logging ---

# CLOUD MIGRATION NOTE: In cloud environments, set LOG_LEVEL via env var
# and configure the logging handler to ship to CloudWatch / GCP Logging.
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE: Path = Path(os.getenv("LOG_FILE", str(DATA_DIR / "pipeline.log")))


# --- Output File Names ---

FULL_OUTPUT_FILENAME: str = "fda_drugs_full.csv"
INCREMENTAL_OUTPUT_PREFIX: str = "fda_drugs_incremental_"
