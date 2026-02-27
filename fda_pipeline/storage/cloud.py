"""Google Cloud Storage backend.

Stores all pipeline output (CSVs and state JSON files) in a GCS bucket.
Authentication uses Application Default Credentials — no key file required
when running on Cloud Run.

To activate:
  1. Set STORAGE_BACKEND=cloud in your .env / Cloud Run env vars.
  2. Set CLOUD_STORAGE_PROVIDER=gcs
  3. Set CLOUD_STORAGE_BUCKET=<your-bucket-name>
  4. Set CLOUD_STORAGE_PREFIX=fda_data/  (or any key prefix you prefer)
  5. Ensure the service account has the roles/storage.objectAdmin role.

Local development:
  Run `gcloud auth application-default login` once, then set the env vars above.
"""

from __future__ import annotations

import io
import json
import logging

import pandas as pd
from google.cloud import storage  # type: ignore[import]
from google.cloud.exceptions import NotFound  # type: ignore[import]

from fda_pipeline.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class CloudStorage(StorageBackend):
    """Reads and writes pipeline output to Google Cloud Storage."""

    def __init__(self, bucket: str, prefix: str) -> None:
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""
        self._client = storage.Client()
        self._bucket = self._client.bucket(bucket)
        logger.info(
            "CloudStorage (GCS) initialized — bucket=%s prefix=%s",
            bucket, self._prefix,
        )

    def _blob(self, filename: str) -> storage.Blob:
        return self._bucket.blob(f"{self._prefix}{filename}")

    # ------------------------------------------------------------------
    # CSV
    # ------------------------------------------------------------------

    def write_csv(self, df: pd.DataFrame, filename: str) -> str:
        blob = self._blob(filename)
        blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
        uri = f"gs://{self._bucket.name}/{blob.name}"
        logger.info("Wrote %d rows to %s", len(df), uri)
        return uri

    def read_csv(self, filename: str) -> pd.DataFrame | None:
        blob = self._blob(filename)
        try:
            data = blob.download_as_bytes()
            return pd.read_csv(io.BytesIO(data))
        except NotFound:
            return None

    def file_exists(self, filename: str) -> bool:
        return self._blob(filename).exists()

    # ------------------------------------------------------------------
    # JSON (state files: last_run.json, run_history.json)
    # ------------------------------------------------------------------

    def read_json(self, filename: str) -> dict | None:
        blob = self._blob(filename)
        try:
            return json.loads(blob.download_as_text())
        except NotFound:
            return None
        except json.JSONDecodeError as exc:
            logger.warning(
                "Could not parse gs://%s/%s: %s",
                self._bucket.name, blob.name, exc,
            )
            return None

    def write_json(self, data: dict, filename: str) -> None:
        blob = self._blob(filename)
        blob.upload_from_string(
            json.dumps(data, indent=2),
            content_type="application/json",
        )
