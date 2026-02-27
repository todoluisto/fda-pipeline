"""Scheduler layer with pluggable backends.

Provides an abstract interface and concrete implementations for
local cron-like scheduling and cloud-based scheduling stubs.
"""

from fda_pipeline.scheduler.base import SchedulerBackend
from fda_pipeline.scheduler.local import LocalScheduler
from fda_pipeline.scheduler.cloud import CloudScheduler

__all__ = ["SchedulerBackend", "LocalScheduler", "CloudScheduler"]
