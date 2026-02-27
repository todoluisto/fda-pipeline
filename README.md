# FDA Drug Approval Data Pipeline

A modular, cloud-ready data pipeline that pulls drug approval data from the
[OpenFDA Drugs@FDA API](https://open.fda.gov/apis/drug/drugsfda/) and saves it
to CSV files. Designed as a local proof-of-concept with a clean migration path
to AWS or GCP.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      pipeline.py                        │
│              (orchestrator + CLI + logging)              │
├──────────┬──────────────┬──────────────┬────────────────┤
│          │              │              │                │
│  extractor.py   transformer.py   loader.py    scheduler/ │
│  (API calls +   (flatten JSON   (DataFrame   ┌─────────┐│
│   pagination     to rows)        → storage)  │ local.py ││
│   + retry)                           │       │ cloud.py ││
│                                      │       └─────────┘│
│                                      ▼                  │
│                                 storage/                │
│                              ┌───────────┐              │
│                              │  base.py  │ (ABC)        │
│                              ├───────────┤              │
│                              │ local.py  │ ← active     │
│                              │ cloud.py  │ ← stub       │
│                              └───────────┘              │
└─────────────────────────────────────────────────────────┘
         │                                     │
         ▼                                     ▼
   OpenFDA API                          fda_data/
   (source)                       (CSV output + logs)
```

**Key design decisions:**
- Abstract base classes for storage and scheduling make cloud migration a
  config change, not a code rewrite.
- All environment-dependent values live in `.env` — nothing is hardcoded.
- Python `logging` module is used throughout so logs can be redirected to any
  cloud log sink.

## Project Structure

```
fda-pipeline/
├── fda_pipeline/
│   ├── __init__.py
│   ├── config.py              # All configuration from env vars
│   ├── extractor.py           # API calls + pagination + retry
│   ├── transformer.py         # Flatten nested JSON → flat rows
│   ├── loader.py              # Write to storage backend
│   ├── pipeline.py            # Orchestrator + CLI entry point
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── base.py            # Abstract StorageBackend
│   │   ├── local.py           # Writes CSV to local filesystem
│   │   └── cloud.py           # Stub for S3 / GCS
│   └── scheduler/
│       ├── __init__.py
│       ├── base.py            # Abstract SchedulerBackend
│       ├── local.py           # schedule library (cron-like)
│       └── cloud.py           # No-op stub for cloud triggers
├── fda_data/                  # Output directory (gitignored)
├── .env                       # Environment config (gitignored)
├── .env.example               # Template with all variables documented
├── .gitignore
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Local Setup

### Prerequisites

- Python 3.11+
- pip

### Install

```bash
# Clone the repo
git clone <repo-url> && cd fda-pipeline

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment variables
cp .env.example .env
# Edit .env as needed (defaults work for local runs)
```

## Running the Pipeline

### Manual run (recommended for first use)

```bash
# Run once immediately — first run does a full historical pull
python -m fda_pipeline.pipeline --run-now

# Force a full re-pull (ignores last_run.json)
python -m fda_pipeline.pipeline --full-refresh
```

### Scheduled mode

```bash
# Starts a long-running process that triggers at SCHEDULE_TIME (default 02:00)
python -m fda_pipeline.pipeline
```

### With Docker

```bash
# Build and run once
docker compose up fda-pipeline

# Run in scheduled mode (stays alive)
docker compose --profile scheduled up fda-pipeline-scheduled

# Rebuild after code changes
docker compose build && docker compose up fda-pipeline
```

## Output

Each run produces:

| File | Description |
|------|-------------|
| `fda_data/fda_drugs_full.csv` | Cumulative dataset (all records, deduplicated) |
| `fda_data/fda_drugs_incremental_YYYY-MM-DD.csv` | Records new/changed in this run |
| `fda_data/pipeline.log` | Timestamped log of all runs |
| `fda_data/last_run.json` | Bookmark for incremental runs |

### CSV Fields

| Field | Source |
|-------|--------|
| `application_number` | Top-level |
| `sponsor_name` | Top-level |
| `application_type` | Top-level (NDA, BLA, ANDA) |
| `brand_name` | `openfda.brand_name` |
| `generic_name` | `openfda.generic_name` |
| `manufacturer_name` | `openfda.manufacturer_name` |
| `product_ndc` | `openfda.product_ndc` |
| `submission_type` | Latest submission |
| `submission_status` | Latest submission |
| `submission_status_date` | Latest submission |
| `submission_class_code_description` | Latest submission |
| `marketing_status` | Products array |
| `dosage_form` | Products array |
| `route` | Products array |
| `strength` | Products array |

## Configuration via .env

See `.env.example` for all available variables with descriptions. Key settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_BACKEND` | `local` | `local` or `cloud` |
| `SCHEDULER_BACKEND` | `local` | `local` or `cloud` |
| `SCHEDULE_TIME` | `02:00` | Daily run time (HH:MM, 24h) |
| `FDA_API_KEY` | *(empty)* | Optional API key for higher rate limits |
| `LOG_LEVEL` | `INFO` | Python log level |

## Cloud Migration Guide

The pipeline is designed so migrating to the cloud requires **config changes
and implementing two stub classes** — no changes to the core pipeline logic.

### AWS: S3 + Lambda + EventBridge

#### 1. Storage — switch to S3

Update `.env`:
```
STORAGE_BACKEND=cloud
CLOUD_STORAGE_PROVIDER=s3
CLOUD_STORAGE_BUCKET=my-fda-data-bucket
CLOUD_STORAGE_PREFIX=fda_data/
```

Implement the methods in `fda_pipeline/storage/cloud.py` using `boto3`:
```python
import boto3
self._client = boto3.client("s3")
# See the inline CLOUD MIGRATION NOTE comments in cloud.py for examples
```

Add `boto3` to `requirements.txt`.

#### 2. Scheduler — EventBridge triggers Lambda

Update `.env`:
```
SCHEDULER_BACKEND=cloud
```

Create a Lambda handler (`handler.py`):
```python
from fda_pipeline.pipeline import _configure_logging, run

def lambda_handler(event, context):
    _configure_logging()
    full_refresh = event.get("full_refresh", False)
    run(full_refresh=full_refresh)
    return {"statusCode": 200}
```

Create an EventBridge rule with schedule expression `cron(0 2 * * ? *)`
targeting this Lambda function.

#### 3. Logging

Lambda automatically ships stdout/stderr to CloudWatch. The pipeline's
console handler already writes to stdout, so logs appear in CloudWatch
with no code changes.

#### 4. Docker deployment (ECS alternative)

Push the existing Dockerfile to ECR and create an ECS Scheduled Task
using the same EventBridge cron expression.

---

### GCP: GCS + Cloud Run + Cloud Scheduler

#### 1. Storage — switch to GCS

Update `.env`:
```
STORAGE_BACKEND=cloud
CLOUD_STORAGE_PROVIDER=gcs
CLOUD_STORAGE_BUCKET=my-fda-data-bucket
CLOUD_STORAGE_PREFIX=fda_data/
```

Implement the methods in `fda_pipeline/storage/cloud.py` using
`google-cloud-storage`. Add it to `requirements.txt`.

#### 2. Scheduler — Cloud Scheduler triggers Cloud Run

Create a minimal Flask app (`main.py`):
```python
from flask import Flask
from fda_pipeline.pipeline import _configure_logging, run

app = Flask(__name__)

@app.route("/run", methods=["POST"])
def trigger():
    _configure_logging()
    run(full_refresh=False)
    return "OK", 200
```

Deploy the Dockerfile to Cloud Run. Create a Cloud Scheduler job with
cron `0 2 * * *` targeting the Cloud Run `/run` endpoint.

#### 3. Logging

Cloud Run automatically captures stdout to GCP Logging. No code changes
needed beyond what's already in place.

---

### Migration Checklist

- [ ] Implement `CloudStorage.write_csv()`, `read_csv()`, `file_exists()`
- [ ] Add cloud SDK to `requirements.txt` (`boto3` or `google-cloud-storage`)
- [ ] Set `STORAGE_BACKEND=cloud` and configure bucket variables in `.env`
- [ ] Set `SCHEDULER_BACKEND=cloud` in `.env`
- [ ] Create cloud handler (Lambda handler or Flask app for Cloud Run)
- [ ] Configure cloud scheduler (EventBridge rule or Cloud Scheduler job)
- [ ] Push Docker image to cloud registry (ECR / Artifact Registry)
- [ ] Move `last_run.json` to cloud storage (or use the same bucket)
- [ ] Configure IAM roles for the compute service to access the bucket
- [ ] Test with `--run-now` before enabling the scheduled trigger
