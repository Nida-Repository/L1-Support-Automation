"""Celery Task Definitions for PRTG Alert Webhook Processing.

Consumes validated payloads from RabbitMQ, validates schema, checks Redis
for duplicate events, and dispatches to appropriate status processors.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from celery.utils.log import get_task_logger
from kombu import Producer
from pydantic import ValidationError

from cache.redis_cache import IncidentStateTracker
from models.prtg_alert import PRTGWebhookPayload, SensorStatus
from processors import (
    down_processor,
    paused_processor,
    unusual_processor,
    up_processor,
    warning_processor,
)
from task_queue.celery_app import celery_app

logger = get_task_logger(__name__)

STATUS_PROCESSORS = {
    SensorStatus.DOWN: down_processor,
    SensorStatus.WARNING: warning_processor,
    SensorStatus.PAUSED: paused_processor,
    SensorStatus.UNUSUAL: unusual_processor,
    SensorStatus.UP: up_processor,
}


def _send_to_dlq(raw_payload: dict[str, Any], reason: str) -> None:
    """Manually route permanently-failed payloads to the Dead Letter Queue."""
    sensor_id = raw_payload.get("sensor_id") or raw_payload.get("sensorid", "Unknown")
    logger.warning("Attempting to publish message to DLQ for sensor_id: %s | Reason: %s", sensor_id, reason)

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
        logger.critical("Payload successfully routed to DLQ for sensor_id: %s | Reason: %s", sensor_id, reason)
    except Exception as dlq_exc:
        logger.critical(
            "FAILED TO PUBLISH TO DLQ for sensor_id: %s | Exception: %s",
            sensor_id,
            dlq_exc,
            exc_info=True,
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
def process_prtg_webhook_task(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Consumes webhook events, performs deduplication, and invokes domain processors."""
    sensor_id = raw_payload.get("sensor_id") or raw_payload.get("sensorid", "Unknown")
    logger.info(
        "Starting processing task execution for sensor_id: %s [Attempt %d/%d]",
        sensor_id,
        self.request.retries + 1,
        self.max_retries + 1,
    )

    # 1. Validation
    try:
        payload = PRTGWebhookPayload.model_validate(raw_payload)
        logger.debug("Successfully validated payload schema for sensor_id: %s", sensor_id)
    except ValidationError as exc:
        logger.error("Permanent schema validation failure for sensor_id %s: %s", sensor_id, exc)
        _send_to_dlq(raw_payload, f"validation_error: {exc}")
        return {"status": "failed", "reason": "validation_error", "sensor_id": sensor_id}

    try:
        # 2. Deduplication via Redis
        if IncidentStateTracker.is_duplicate_alert(payload.sensor_id, payload.status.value):
            logger.info(
                "Duplicate alert ignored for sensor_id %s (current status remains: %s)",
                payload.sensor_id,
                payload.status.value,
            )
            return {"status": "ignored", "reason": "duplicate_state", "sensor_id": payload.sensor_id}

        # 3. Dispatch to processor
        processor = STATUS_PROCESSORS.get(payload.status)
        if processor is None:
            logger.warning("No processor registered for status '%s' on sensor_id: %s", payload.status, payload.sensor_id)
            return {"status": "skipped", "reason": "no_processor", "sensor_id": payload.sensor_id}

        logger.info("Dispatching sensor_id %s (status: %s) to %s", payload.sensor_id, payload.status.value, processor.__name__)
        processor.process(payload)

        # 4. Update active status in Redis after successful processing
        IncidentStateTracker.set_sensor_state(payload.sensor_id, payload.status.value)
        logger.debug("Updated Redis active state for sensor_id %s to status: %s", payload.sensor_id, payload.status.value)

        logger.info("Successfully processed PRTG alert for sensor_id: %s", payload.sensor_id)
        return {"status": "processed", "sensor_id": payload.sensor_id}

    except Exception as exc:
        current_attempt = self.request.retries + 1
        logger.error(
            "Transient processing error for sensor_id %s (Attempt %d/%d): %s",
            sensor_id,
            current_attempt,
            self.max_retries,
            exc,
            exc_info=True,
        )
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            logger.critical("Max retries (%d) exceeded for sensor_id %s. Routing to DLQ...", self.max_retries, sensor_id)
            _send_to_dlq(raw_payload, f"max_retries_exceeded: {exc}")
            return {"status": "failed", "reason": "max_retries_exceeded", "sensor_id": sensor_id}