"""Loader — writes transformed data to the configured storage backend.

Uses the abstract StorageBackend interface so the destination can be
swapped by changing a single config variable.
"""

import logging

import pandas as pd

from fda_pipeline import config
from fda_pipeline.storage.base import StorageBackend
from fda_pipeline.storage.local import LocalStorage

logger = logging.getLogger(__name__)


def get_storage_backend() -> StorageBackend:
    """Factory that returns the configured storage backend.

    CloudStorage is imported lazily so that google-cloud-storage is only
    required when STORAGE_BACKEND=cloud (not on local installs).

    CLOUD MIGRATION NOTE: Set STORAGE_BACKEND=cloud and configure
    CLOUD_STORAGE_BUCKET / CLOUD_STORAGE_PREFIX to switch to GCS.
    """
    if config.STORAGE_BACKEND == "cloud":
        from fda_pipeline.storage.cloud import CloudStorage  # lazy import
        return CloudStorage(
            bucket=config.CLOUD_STORAGE_BUCKET,
            prefix=config.CLOUD_STORAGE_PREFIX,
        )
    return LocalStorage(data_dir=config.DATA_DIR)


def load(
    rows: list[dict],
    storage: StorageBackend,
    full_filename: str,
    incremental_filename: str,
) -> int:
    """Load transformed rows into storage.

    Writes two files:
      1. An incremental file containing only this run's new/changed rows.
      2. A full cumulative file that merges the new rows with any existing
         full dataset (deduplicating on the natural product grain).

    Args:
        rows: Flat dicts from the transformer.
        storage: The storage backend to write to.
        full_filename: Filename for the cumulative dataset.
        incremental_filename: Filename for this run's incremental data.

    Returns:
        The number of rows in the incremental file.
    """
    if not rows:
        logger.info("No rows to load — skipping write")
        return 0

    new_df = pd.DataFrame(rows)

    # Write incremental file
    inc_path = storage.write_csv(new_df, incremental_filename)
    logger.info("Incremental file: %s (%d rows)", inc_path, len(new_df))

    # Merge with existing full dataset
    existing_df = storage.read_csv(full_filename)
    if existing_df is not None and not existing_df.empty:
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        # Deduplicate — keep the latest row for each unique combination
        dedup_cols = [
            "application_number",
            "dosage_form",
            "strength",
            "marketing_status",
            "route",
        ]
        combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
    else:
        combined = new_df

    full_path = storage.write_csv(combined, full_filename)
    logger.info("Full file: %s (%d rows)", full_path, len(combined))

    return len(new_df)
