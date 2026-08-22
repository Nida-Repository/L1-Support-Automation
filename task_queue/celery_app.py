"""Celery Application Initialization and Topology.

Configures RabbitMQ message broker, Redis result backend, exchange bindings,
and Dead Letter Queue (DLQ) topology.
"""
from __future__ import annotations

import logging
from celery import Celery
from celery.signals import after_setup_logger, worker_init
from celery.schedules import crontab
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
        Queue(
            "incoming_email_queue",
            default_exchange,
            routing_key="email.incoming",
            queue_arguments={
                "x-dead-letter-exchange": "prtg_dlx",
                "x-dead-letter-routing-key": "prtg.email.dlq",
            },
        ),
        Queue(
            "isp_monitor_queue",
            default_exchange,
            routing_key="isp.monitor",
            queue_arguments={
                "x-dead-letter-exchange": "prtg_dlx",
                "x-dead-letter-routing-key": "prtg.monitor.dlq",
            },
        ),
        Queue("prtg_webhook_dlq", dlx_exchange, routing_key="prtg.webhook.dlq"),
        Queue("prtg_email_dlq", dlx_exchange, routing_key="prtg.email.dlq"),
        Queue("prtg_monitor_dlq", dlx_exchange, routing_key="prtg.monitor.dlq"),
    ),
    task_routes={
        "process_prtg_webhook": {"queue": "prtg_webhook_queue", "routing_key": "prtg.webhook"},
        "process_incoming_email": {"queue": "incoming_email_queue", "routing_key": "email.incoming"},
        "scan_isp_reply_monitors": {"queue": "isp_monitor_queue", "routing_key": "isp.monitor"},
    },
    task_default_queue="prtg_webhook_queue",
    task_default_exchange="prtg_events",
    task_default_routing_key="prtg.webhook",
    # --- Celery Beat Periodic Tasks ---
    beat_schedule={
        "scan-isp-reply-monitors": {
            "task": "scan_isp_reply_monitors",
            "schedule": crontab(minute=f"*/{settings.isp_monitor_beat_interval_minutes}"),
            "options": {"queue": "isp_monitor_queue"},
        },
    },
)

logger.info("Celery queues and exchanges configured successfully.")


# ---------------------------------------------------------------------------
# Celery Signals for Logging Integration
# ---------------------------------------------------------------------------

@after_setup_logger.connect
def setup_celery_logging(logger=None, format=None, loglevel=None, plaintext=None, **kwargs):
    """Re-apply logging configuration after Celery's own logger setup runs."""
    import logging.config
    logging.config.dictConfig(LOGGING_CONFIG)
    if logger:
        logger.info("Custom dictConfig successfully applied to Celery loggers.")


@worker_init.connect
def on_worker_init(**kwargs):
    """Log worker process initialization details."""
    logger.info("Celery worker process initializing...")