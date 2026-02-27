"""Local scheduler using the ``schedule`` library.

Runs the pipeline on a cron-like daily schedule inside the current process.
Suitable for local development and Docker-based deployments.
"""

import logging
import time
from collections.abc import Callable

import schedule

from fda_pipeline.scheduler.base import SchedulerBackend

logger = logging.getLogger(__name__)


class LocalScheduler(SchedulerBackend):
    """Runs the pipeline daily at a configured time using ``schedule``."""

    def start(self, job: Callable[[], None], run_time: str) -> None:
        schedule.every().day.at(run_time).do(job)
        logger.info("Scheduled daily run at %s (local time)", run_time)

        # Block and loop until the process is killed
        while True:
            schedule.run_pending()
            time.sleep(30)

    def run_once(self, job: Callable[[], None]) -> None:
        logger.info("Running pipeline immediately (manual invocation)")
        job()
