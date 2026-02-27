"""Storage layer with pluggable backends.

Provides an abstract interface and concrete implementations for
local filesystem and cloud storage destinations.

CloudStorage is imported lazily inside loader.py so that the
google-cloud-storage SDK is only required when STORAGE_BACKEND=cloud.
"""

from fda_pipeline.storage.base import StorageBackend
from fda_pipeline.storage.local import LocalStorage

__all__ = ["StorageBackend", "LocalStorage"]
