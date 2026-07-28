import os
from celery import Celery
from kombu import Exchange, Queue

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
REDIS_URL = os.getenv("REDIS_URL")

if not RABBITMQ_URL:
    raise RuntimeError("RABBITMQ_URL environment variable is not set")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL environment variable is not set")

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

    # --- Delivery / reliability ---
    task_acks_late=True,                     # ack only after task completes
    task_reject_on_worker_lost=True,         # requeue if worker dies mid-task
    task_acks_on_failure_or_timeout=False,   # do NOT ack on a raised exception —
                                              # required so a failed task doesn't
                                              # silently vanish from the queue;
                                              # tasks.py explicitly routes to the
                                              # DLQ on final failure instead.
    broker_connection_retry_on_startup=True, # required default changed in Celery 6-line behavior
    broker_connection_max_retries=None,      # retry forever on startup

    # --- Worker behavior ---
    worker_prefetch_multiplier=1,            # don't hoard messages ahead of DLQ-worthy retries
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