import os
from celery import Celery
from kombu import Exchange, Queue

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
REDIS_URL = os.getenv("REDIS_URL")

celery_app = Celery(
    "prtg_tasks",
    broker=RABBITMQ_URL,
    backend=REDIS_URL,  # Redis handles task execution results & status
    include=["task_queue.tasks"],
)

# Configure Exchanges and Dead Letter Queues (DLQ)
default_exchange = Exchange("prtg_events", type="direct")
dlx_exchange = Exchange("prtg_dlx", type="direct")

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    result_expires=3600,  # Store task results in Redis for 1 hour
    task_acks_late=True,  # Redeliver if a worker crashes mid-task
    task_reject_on_worker_lost=True,
    # Configure main queue and dead-letter queue bindings
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