"""Abstract base class for storage backends.

All storage implementations must conform to this interface. The pipeline
references only this abstraction, so swapping from local to cloud storage
requires changing only the STORAGE_BACKEND config variable.

CLOUD MIGRATION NOTE: When adding a new cloud provider, create a new
subclass of StorageBackend and register it in the factory function in
loader.py.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class StorageBackend(ABC):
    """Interface for writing pipeline output to a destination."""

    @abstractmethod
    def write_csv(self, df: pd.DataFrame, filename: str) -> str:
        """Write a DataFrame as a CSV file to the storage destination.

        Args:
            df: The DataFrame to write.
            filename: The target filename (e.g. "fda_drugs_full.csv").

        Returns:
            A string describing where the file was written (path or URI).
        """

    @abstractmethod
    def read_csv(self, filename: str) -> pd.DataFrame | None:
        """Read a CSV file from the storage destination.

        Args:
            filename: The filename to read.

        Returns:
            A DataFrame, or None if the file does not exist.
        """

    @abstractmethod
    def file_exists(self, filename: str) -> bool:
        """Check whether a file exists in the storage destination."""

    @abstractmethod
    def read_json(self, filename: str) -> dict | None:
        """Read a JSON file from the storage destination.

        Args:
            filename: The filename to read (e.g. "last_run.json").

        Returns:
            Parsed dict, or None if the file does not exist.
        """

    @abstractmethod
    def write_json(self, data: dict, filename: str) -> None:
        """Write a dict as a JSON file to the storage destination.

        Args:
            data: The dict to serialise.
            filename: The target filename (e.g. "run_history.json").
        """
