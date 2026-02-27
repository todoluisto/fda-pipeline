"""Local filesystem storage backend.

Writes CSV and JSON files to a local directory (configured via DATA_DIR).
This is the default backend for local development and proof-of-concept runs.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from fda_pipeline.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class LocalStorage(StorageBackend):
    """Writes pipeline output to the local filesystem."""

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir
        self._data_dir.mkdir(parents=True, exist_ok=True)
        logger.info("LocalStorage initialized — data_dir=%s", self._data_dir)

    def write_csv(self, df: pd.DataFrame, filename: str) -> str:
        path = self._data_dir / filename
        df.to_csv(path, index=False)
        logger.info("Wrote %d rows to %s", len(df), path)
        return str(path)

    def read_csv(self, filename: str) -> pd.DataFrame | None:
        path = self._data_dir / filename
        if not path.exists():
            return None
        return pd.read_csv(path)

    def file_exists(self, filename: str) -> bool:
        return (self._data_dir / filename).exists()

    def read_json(self, filename: str) -> dict | None:
        path = self._data_dir / filename
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read %s: %s", path, exc)
            return None

    def write_json(self, data: dict, filename: str) -> None:
        path = self._data_dir / filename
        path.write_text(json.dumps(data, indent=2))
