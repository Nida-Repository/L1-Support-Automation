import logging
from task_queue.celery_app import celery_app
from models.prtg_alert import PRTGWebhookPayload, SensorStatus
from cache.redis_cache import IncidentStateTracker
from processors import (
    down_processor,
    unusual_processor,
    up_processor,
    warning_processor,
    paused_processor,
)

logger = logging.getLogger(__name__)

STATUS_PROCESSORS = {
    SensorStatus.DOWN: down_processor,
    SensorStatus.WARNING: warning_processor,
    SensorStatus.PAUSED: paused_processor,
    SensorStatus.UNUSUAL: unusual_processor,
    SensorStatus.UP: up_processor,
}


@celery_app.task(
    bind=True,
    name="process_prtg_webhook",
    max_retries=5,
    default_retry_delay=10,  # Exponential backoff: 10s, 20s, 40s...
    backoff=True,
)
def process_prtg_webhook_task(self, raw_payload: dict):
    """
    Consumes validated payloads from RabbitMQ, checks Redis for duplicates,
    and dispatches to the corresponding processor.
    """
    try:
        payload = PRTGWebhookPayload.model_validate(raw_payload)

        # 1. Deduplication via Redis
        if IncidentStateTracker.is_duplicate_alert(payload.sensor_id, payload.status.value):
            logger.info(
                f"Duplicate alert ignored for Sensor ID {payload.sensor_id} "
                f"(Status remains: {payload.status.value})"
            )
            return {"status": "ignored", "reason": "duplicate_state"}

        # 2. Update active status in Redis
        IncidentStateTracker.set_sensor_state(payload.sensor_id, payload.status.value)

        logger.info(
            f"Processing PRTG Alert for Sensor ID {payload.sensor_id} with status: {payload.status}"
        )

        # 3. Dispatch to processor
        processor = STATUS_PROCESSORS.get(payload.status)
        if processor:
            processor.process(payload)
        else:
            logger.warning(f"No processor registered for status: {payload.status}")

        return {"status": "processed", "sensor_id": payload.sensor_id}

    except Exception as exc:
        sensor_id = raw_payload.get("sensor_id") or raw_payload.get("sensorid", "Unknown")
        logger.error(f"Task failed for Sensor ID {sensor_id}: {exc}. Retrying...")
        
        # Auto-retries up to max_retries, then routes to DLQ
        raise self.retry(exc=exc)