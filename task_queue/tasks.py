"""Celery Task Definitions for PRTG Alert Webhook Processing.

Consumes validated payloads from RabbitMQ, validates schema, checks Redis
for duplicate events, and dispatches to appropriate status processors.
"""
from __future__ import annotations

import base64
import datetime
import logging
from typing import Any, Dict, Optional

from celery.utils.log import get_task_logger
from kombu import Producer
from pydantic import ValidationError

from app.crud import AttachmentRepository, IspEmailThreadRepository, session_scope
from app.crud import EscalationRecordRepository, NotFoundError
from app.database import SessionLocal
from app.models import EmailClassificationType, EmailDirectionType
from cache.redis_cache import EmailThreadCache, IncidentStateTracker
from clients.email_utils import clean_message_id
from models.email_thread_model import IncomingEmailPayload
from models.prtg_alert import PRTGWebhookPayload, SensorStatus
from processors import (
    down_processor,
    paused_processor,
    unusual_processor,
    up_processor,
    warning_processor,
)
from services.minio_service import minio_service
from task_queue.celery_app import celery_app

logger = get_task_logger(__name__)

STATUS_PROCESSORS = {
    SensorStatus.DOWN: down_processor,
    SensorStatus.WARNING: warning_processor,
    SensorStatus.PAUSED: paused_processor,
    SensorStatus.UNUSUAL: unusual_processor,
    SensorStatus.UP: up_processor,
}


def _send_email_to_dlq(raw_payload: dict[str, Any], reason: str) -> None:
    """Manually route permanently-failed inbound email payloads to the Dead Letter Queue."""
    msg_id = raw_payload.get("message_id", "Unknown")
    logger.warning("Routing failed email message to DLQ [Message-ID: %s | Reason: %s]", msg_id, reason)
    try:
        with celery_app.connection_for_write() as conn:
            producer = Producer(conn)
            producer.publish(
                {"payload": raw_payload, "failure_reason": reason},
                exchange="prtg_dlx",
                routing_key="prtg.email.dlq",
                declare=[celery_app.conf.task_queues[3]],  # prtg_email_dlq
                retry=True,
            )
        logger.critical("Inbound email payload routed to DLQ [Message-ID: %s]", msg_id)
    except Exception as dlq_exc:
        logger.critical("FAILED TO PUBLISH EMAIL TO DLQ [Message-ID: %s]: %s", msg_id, dlq_exc, exc_info=True)


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


@celery_app.task(
    bind=True,
    name="process_incoming_email",
    max_retries=5,
    default_retry_delay=10,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
def process_incoming_email_task(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Processes matched inbound email messages: records thread entry and uploads attachments to MinIO.

    Responsibilities:
    1. Validate inbound email task payload schema.
    2. Store incoming email in ISP_EMAIL_THREADS with direction=INCOMING.
    3. Update Redis Message-ID cache.
    4. Upload attachments to MinIO with structured object keys.
    5. Save attachment metadata in ATTACHMENTS table associated with alert_id and thread_id.
    6. Does NOT perform automatic classification (leaves as UNKNOWN).
    """
    msg_id = raw_payload.get("message_id", "Unknown")
    alert_id = raw_payload.get("alert_id")
    logger.info(
        "Starting inbound email processing task [Message-ID: %s | Alert ID: %s | Attempt %d/%d]",
        msg_id,
        alert_id,
        self.request.retries + 1,
        self.max_retries + 1,
    )

    # 1. Validation
    try:
        payload = IncomingEmailPayload.model_validate(raw_payload)
        logger.debug("Successfully validated inbound email payload [Message-ID: %s]", payload.message_id)
    except ValidationError as exc:
        logger.error("Permanent schema validation failure for inbound email %s: %s", msg_id, exc)
        _send_email_to_dlq(raw_payload, f"validation_error: {exc}")
        return {"status": "failed", "reason": "validation_error", "message_id": msg_id}

    clean_id = clean_message_id(payload.message_id)
    clean_in_reply_to = clean_message_id(payload.in_reply_to) if payload.in_reply_to else None

    # Parse received_at datetime
    received_at_dt = datetime.datetime.now(datetime.timezone.utc)
    if payload.received_at:
        try:
            from dateutil import parser as date_parser
            parsed_dt = date_parser.parse(payload.received_at)
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=datetime.timezone.utc)
            received_at_dt = parsed_dt
        except Exception:
            received_at_dt = datetime.datetime.now(datetime.timezone.utc)

    thread_id = None
    uploaded_attachments_count = 0

    try:
        with session_scope(SessionLocal) as session:
            thread_repo = IspEmailThreadRepository(session)
            attachment_repo = AttachmentRepository(session)

            # Check if thread entry already exists (idempotency)
            existing_thread = thread_repo.get_by_message_id(clean_id)
            if existing_thread:
                thread_id = existing_thread.thread_id
                logger.info("Found existing thread record for Message-ID: %s (thread_id=%d)", clean_id, thread_id)
            else:
                thread = thread_repo.create(
                    alert_id=payload.alert_id,
                    message_id=clean_id,
                    in_reply_to=clean_in_reply_to,
                    email_references=payload.references if payload.references else None,
                    subject=payload.subject,
                    sender=payload.sender,
                    receiver=payload.receiver,
                    cc=payload.cc if payload.cc else None,
                    direction=EmailDirectionType.INCOMING,
                    sent_received_at=received_at_dt,
                    body=payload.body,
                    classification_type=EmailClassificationType.UNKNOWN,
                )
                thread_id = thread.thread_id
                logger.info(
                    "Created new INCOMING email thread record (thread_id=%d, alert_id=%d, msg_id=%s)",
                    thread_id,
                    payload.alert_id,
                    clean_id,
                )

            # Mark escalation response received if escalation_id is known
            if payload.escalation_id:
                try:
                    escalation_repo = EscalationRecordRepository(session)
                    escalation_repo.mark_response_received(
                        payload.escalation_id,
                        notes=f"ISP replied via email (Message-ID: {clean_id})",
                    )
                    logger.info(
                        "Marked response_received=True for escalation_id=%d [Alert: %d | Message-ID: %s]",
                        payload.escalation_id,
                        payload.alert_id,
                        clean_id,
                    )
                except NotFoundError:
                    logger.warning(
                        "escalation_id=%d not found while marking response received [Alert: %d]",
                        payload.escalation_id,
                        payload.alert_id,
                    )
                except Exception as esc_exc:
                    logger.error(
                        "Failed to mark response received for escalation_id=%d: %s",
                        payload.escalation_id,
                        esc_exc,
                    )

            # Process attachments
            for att_meta in payload.attachment_metadata:
                file_name = att_meta.get("file_name", "unnamed_file")
                content_type = att_meta.get("content_type", "application/octet-stream")
                payload_b64 = att_meta.get("payload_base64")

                if not payload_b64:
                    logger.warning("Skipping attachment '%s' due to missing payload data", file_name)
                    continue

                try:
                    file_bytes = base64.b64decode(payload_b64)
                except Exception as b64_exc:
                    logger.error("Failed to base64-decode attachment '%s': %s", file_name, b64_exc)
                    continue

                # Upload to MinIO
                upload_res = minio_service.upload_attachment(
                    alert_id=payload.alert_id,
                    thread_id=thread_id,
                    file_name=file_name,
                    file_data=file_bytes,
                    content_type=content_type,
                )

                # Check if attachment record already exists
                existing_att = attachment_repo.get_by_object_key(upload_res["object_key"])
                if not existing_att:
                    attachment_repo.create(
                        alert_id=payload.alert_id,
                        thread_id=thread_id,
                        file_name=upload_res["file_name"],
                        file_type=upload_res["file_type"],
                        file_size=upload_res["file_size"],
                        bucket_name=upload_res["bucket_name"],
                        object_key=upload_res["object_key"],
                        etag=upload_res.get("etag"),
                        uploaded_by="SYSTEM",
                        uploaded_at=datetime.datetime.now(datetime.timezone.utc),
                    )
                    uploaded_attachments_count += 1
                    logger.info("Saved attachment metadata for '%s' (object_key: %s)", file_name, upload_res["object_key"])

        # Update Redis cache with newly stored incoming Message-ID
        if clean_id and thread_id:
            EmailThreadCache.set_message_id_mapping(
                clean_id,
                {
                    "alert_id": payload.alert_id,
                    "thread_id": thread_id,
                },
            )

        logger.info(
            "Successfully processed inbound email [Message-ID: %s | Alert: %d | Thread: %s | Attachments: %d]",
            clean_id,
            payload.alert_id,
            thread_id,
            uploaded_attachments_count,
        )
        return {
            "status": "processed",
            "message_id": clean_id,
            "alert_id": payload.alert_id,
            "thread_id": thread_id,
            "attachments_uploaded": uploaded_attachments_count,
        }

    except Exception as exc:
        current_attempt = self.request.retries + 1
        logger.error(
            "Transient error processing inbound email %s (Attempt %d/%d): %s",
            msg_id,
            current_attempt,
            self.max_retries,
            exc,
            exc_info=True,
        )
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            logger.critical("Max retries exceeded for inbound email %s. Routing to DLQ...", msg_id)
            _send_email_to_dlq(raw_payload, f"max_retries_exceeded: {exc}")
            return {"status": "failed", "reason": "max_retries_exceeded", "message_id": msg_id}