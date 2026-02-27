"""Abstract base class for scheduler backends.

All scheduler implementations must conform to this interface.

CLOUD MIGRATION NOTE: In cloud deployments the scheduler is external
(EventBridge, Cloud Scheduler). The CloudScheduler stub documents how
the pipeline entry point maps to a cloud trigger.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable


class SchedulerBackend(ABC):
    """Interface for scheduling pipeline runs."""

    @abstractmethod
    def start(self, job: Callable[[], None], run_time: str) -> None:
        """Register and start the scheduled job.

        Args:
            job: A zero-argument callable that executes one pipeline run.
            run_time: The time to run the job daily (HH:MM, 24h format).
        """

    @abstractmethod
    def run_once(self, job: Callable[[], None]) -> None:
        """Execute the job immediately (for manual / CLI invocations)."""
