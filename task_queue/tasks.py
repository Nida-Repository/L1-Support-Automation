import logging

from kombu import Producer
from pydantic import ValidationError

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


def _send_to_dlq(raw_payload: dict, reason: str) -> None:
    """Manually publish a permanently-failed payload to the DLQ.
    Needed because Celery acks/nacks don't map cleanly onto RabbitMQ's
    dead-letter-on-reject semantics — see celery_app.py comment."""
    try:
        with celery_app.connection_for_write() as conn:
            producer = Producer(conn)
            producer.publish(
                {"payload": raw_payload, "failure_reason": reason},
                exchange="prtg_dlx",
                routing_key="prtg.webhook.dlq",
                declare=[celery_app.conf.task_queues[1]],  # prtg_webhook_dlq
                retry=True,
            )
        logger.critical(f"Payload routed to DLQ: {reason}")
    except Exception as dlq_exc:
        # If we can't even reach the DLQ, this is the last line of defense —
        # log loudly so it's caught by log-based alerting.
        logger.critical(
            f"FAILED TO PUBLISH TO DLQ (payload may be lost): {dlq_exc} | payload={raw_payload}"
        )


@celery_app.task(
    bind=True,
    name="process_prtg_webhook",
    max_retries=5,
    default_retry_delay=10,
    retry_backoff=True,        
    retry_backoff_max=300,     
    retry_jitter=True,         
)
def process_prtg_webhook_task(self, raw_payload: dict):
    """
    Consumes validated payloads from RabbitMQ, checks Redis for duplicates,
    and dispatches to the corresponding processor.
    """
    sensor_id = raw_payload.get("sensor_id", "Unknown")

    try:
        payload = PRTGWebhookPayload.model_validate(raw_payload)
    except ValidationError as exc:
        logger.error(f"Permanent validation failure for sensor {sensor_id}: {exc}")
        _send_to_dlq(raw_payload, f"validation_error: {exc}")
        return {"status": "failed", "reason": "validation_error", "sensor_id": sensor_id}

    try:
        # --- 2. Deduplication via Redis ---
        if IncidentStateTracker.is_duplicate_alert(payload.sensor_id, payload.status.value):
            logger.info(
                f"Duplicate alert ignored for Sensor ID {payload.sensor_id} "
                f"(status remains: {payload.status.value})"
            )
            return {"status": "ignored", "reason": "duplicate_state"}

        # --- 3. Update active status in Redis ---
        IncidentStateTracker.set_sensor_state(payload.sensor_id, payload.status.value)

        logger.info(
            f"Processing PRTG alert for sensor {payload.sensor_id} status={payload.status}"
        )

        # --- 4. Dispatch to processor ---
        processor = STATUS_PROCESSORS.get(payload.status)
        if processor is None:
            logger.warning(f"No processor registered for status: {payload.status}")
            return {"status": "skipped", "reason": "no_processor", "sensor_id": payload.sensor_id}

        processor.process(payload)
        return {"status": "processed", "sensor_id": payload.sensor_id}

    except Exception as exc:
        logger.error(
            f"Transient failure for sensor {sensor_id} "
            f"(attempt {self.request.retries + 1}/{self.max_retries}): {exc}"
        )
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            logger.critical(f"Max retries exceeded for sensor {sensor_id}: {exc}")
            _send_to_dlq(raw_payload, f"max_retries_exceeded: {exc}")
            return {"status": "failed", "reason": "max_retries_exceeded", "sensor_id": sensor_id}