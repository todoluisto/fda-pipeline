"""Cloud scheduler stub.

CLOUD MIGRATION NOTE: In a cloud deployment the scheduling is handled by
an external service, not by this Python process:

  AWS:  EventBridge rule → triggers Lambda function or ECS task
  GCP:  Cloud Scheduler → triggers Cloud Run endpoint via HTTP

The pipeline's ``run()`` function becomes the handler/entry point:

  AWS Lambda example (handler.py):
      from fda_pipeline.pipeline import run
      def lambda_handler(event, context):
          run(full_refresh=False)

  Cloud Run example (main.py):
      from flask import Flask
      from fda_pipeline.pipeline import run
      app = Flask(__name__)
      @app.route("/run", methods=["POST"])
      def trigger():
          run(full_refresh=False)
          return "OK", 200

This stub class exists so the codebase remains symmetric and the
SCHEDULER_BACKEND config variable works consistently.
"""

import logging
from collections.abc import Callable

from fda_pipeline.scheduler.base import SchedulerBackend

logger = logging.getLogger(__name__)


class CloudScheduler(SchedulerBackend):
    """No-op scheduler for cloud deployments.

    The cloud platform handles scheduling externally. This class
    only supports ``run_once`` for local testing of the cloud path.
    """

    def start(self, job: Callable[[], None], run_time: str) -> None:
        logger.info(
            "CloudScheduler.start() is a no-op. "
            "Configure your cloud platform's scheduler (EventBridge / Cloud Scheduler) "
            "to invoke the pipeline entry point at %s UTC.",
            run_time,
        )

    def run_once(self, job: Callable[[], None]) -> None:
        logger.info("CloudScheduler: running pipeline immediately")
        job()
