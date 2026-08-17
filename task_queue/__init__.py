"""Task Queue Package."""
from task_queue.celery_app import celery_app
from task_queue.tasks import process_prtg_webhook_task

__all__ = [
    "celery_app",
    "process_prtg_webhook_task",
]
