import logging
import os
from urllib.parse import urlparse

from celery import Celery
from celery.signals import after_setup_logger, worker_init
from dotenv import load_dotenv
from kombu import Exchange, Queue

from config.logging_config import setup_logging, LOGGING_CONFIG
setup_logging()
# 1. Instantiate module-level logger
logger = logging.getLogger(__name__)

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
REDIS_URL = os.getenv("REDIS_URL")


def _safe_url(url: str) -> str:
    """Helper to scrub passwords from connection URLs before logging."""
    try:
        parsed = urlparse(url)
        if parsed.password:
            return url.replace(parsed.password, "******")
        return url
    except Exception:
        return "[masked_url]"


if not RABBITMQ_URL:
    logger.critical("RABBITMQ_URL environment variable is missing!")
    raise RuntimeError("RABBITMQ_URL environment variable is not set")

if not REDIS_URL:
    logger.critical("REDIS_URL environment variable is missing!")
    raise RuntimeError("REDIS_URL environment variable is not set")

logger.info("Initializing Celery app with Broker: %s", _safe_url(RABBITMQ_URL))
logger.info("Setting up Celery Result Backend: %s", _safe_url(REDIS_URL))

celery_app = Celery(
    "prtg_tasks",
    broker=RABBITMQ_URL,
    backend=REDIS_URL,
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
    worker_hijack_root_logger=False,   # <-- stop Celery from wiping your handlers


    # --- Delivery / reliability ---
    task_acks_late=True,                     # ack only after task completes
    task_reject_on_worker_lost=True,         # requeue if worker dies mid-task
    task_acks_on_failure_or_timeout=False,   # do NOT ack on raised exception
    broker_connection_retry_on_startup=True, 
    broker_connection_max_retries=None,      

    # --- Worker behavior ---
    worker_prefetch_multiplier=1,            # don't hoard messages ahead of DLQ retries
    task_time_limit=120,                     # hard kill runaway tasks (seconds)
    task_soft_time_limit=90,

    # --- Queues / DLQ topology ---
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
    """
    Re-apply dictConfig after Celery's own logger setup runs, as a safety net.
    With worker_hijack_root_logger=False.
    """
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("Custom dictConfig successfully applied to Celery loggers.")


@worker_init.connect
def on_worker_init(**kwargs):
    """Log worker process initialization details."""
    logger.info("Celery worker process initializing...")