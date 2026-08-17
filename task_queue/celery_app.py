"""Celery Application Initialization and Topology.

Configures RabbitMQ message broker, Redis result backend, exchange bindings,
and Dead Letter Queue (DLQ) topology.
"""
from __future__ import annotations

import logging
from celery import Celery
from celery.signals import after_setup_logger, worker_init
from kombu import Exchange, Queue

from config.logging_config import LOGGING_CONFIG, setup_logging
from config.settings import settings
from utils.json_utils import json_dumps, json_loads
import kombu.serialization

setup_logging()
logger = logging.getLogger(__name__)

# Register robust custom json serializer for Kombu/Celery supporting Decimal, datetime, etc.
kombu.serialization.register(
    "json",
    json_dumps,
    json_loads,
    content_type="application/json",
    content_encoding="utf-8",
)

logger.info("Initializing Celery app with Broker: %s", settings.safe_rabbitmq_url)
logger.info("Setting up Celery Result Backend: %s", settings.safe_redis_url)

celery_app = Celery(
    "prtg_tasks",
    broker=settings.rabbitmq_url,
    backend=settings.redis_url,
    include=["task_queue.tasks"],
)

default_exchange = Exchange("prtg_events", type="direct")
dlx_exchange = Exchange("prtg_dlx", type="direct")

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    result_expires=3600,
    # --- Logging ---
    worker_hijack_root_logger=False,
    # --- Delivery / Reliability ---
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_acks_on_failure_or_timeout=False,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=None,
    # --- Worker Behavior ---
    worker_prefetch_multiplier=1,
    task_time_limit=settings.celery_task_time_limit,
    task_soft_time_limit=settings.celery_task_soft_time_limit,
    # --- Queues / DLQ Topology ---
    task_queues=(
        Queue(
            "prtg_webhook_queue",
            default_exchange,
            routing_key="prtg.webhook",
            queue_arguments={
                "x-dead-letter-exchange": "prtg_dlx",
                "x-dead-letter-routing-key": "prtg.webhook.dlq",
            },
        ),
        Queue("prtg_webhook_dlq", dlx_exchange, routing_key="prtg.webhook.dlq"),
    ),
    task_default_queue="prtg_webhook_queue",
    task_default_exchange="prtg_events",
    task_default_routing_key="prtg.webhook",
)

logger.info("Celery queues and exchanges configured successfully.")


# ---------------------------------------------------------------------------
# Celery Signals for Logging Integration
# ---------------------------------------------------------------------------

@after_setup_logger.connect
def setup_celery_logging(logger, format, loglevel, plaintext, **kwargs):
    """Re-apply logging configuration after Celery's own logger setup runs."""
    import logging.config
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("Custom dictConfig successfully applied to Celery loggers.")


@worker_init.connect
def on_worker_init(**kwargs):
    """Log worker process initialization details."""
    logger.info("Celery worker process initializing...")