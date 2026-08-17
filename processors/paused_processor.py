"""Paused Workflow Processor.

Triggered when a sensor transitions into the 'Paused' state.
Sends a notification to the internal support team and logs the event without
holding database connections during external SMTP dispatch.
"""
from __future__ import annotations

import asyncio
import datetime
import logging
from typing import Any

from app.crud import (
    AlertHistoryRepository,
    AlertStateRepository,
    ConstraintViolationError,
    DuplicateError,
    EscalationRecordRepository,
    NotFoundError,
    RepositoryError,
    SensorLogRepository,
    SensorRepository,
    SiteIspAssignmentRepository,
    SiteRepository,
    session_scope,
)
from app.database import SessionLocal
from app.models import LogLevelType, LogStatusType
from clients.smtp_client import send_paused_notification
from config.settings import settings
from processors.base import (
    extract_field,
    extract_sensor_id,
    json_safe_dict,
    sanitize_status,
    serialize_payload_for_json,
)

logger = logging.getLogger(__name__)

PAUSED_STATE_NAME = "Paused"
DEFAULT_ALERT_MESSAGE = "Sensor condition has been PAUSED"


class PausedWorkflow:
    """Workflow handler for PAUSED sensor events."""

    async def execute(self, payload: Any) -> None:
        logger.info("Executing PausedWorkflow...")

        sensor_id = extract_sensor_id(payload)
        if not sensor_id:
            logger.error("Payload missing or invalid 'sensor_id': %s", payload)
            return

        logger.info("Processing PausedWorkflow for sensor_id: %s", sensor_id)

        raw_status = extract_field(payload, "status", "Paused")
        status_str = sanitize_status(raw_status, default="Paused")
        message = extract_field(payload, "message", DEFAULT_ALERT_MESSAGE)
        timestamp = datetime.datetime.now(datetime.timezone.utc)

        # ------------------------------------------------------------------
        # Phase 1: Fetch topology and create ALERT_HISTORY row in DB
        # ------------------------------------------------------------------
        site_id = None
        site_name = None
        sensor_name = None
        alert_id = None

        with session_scope(SessionLocal) as session:
            sensor_repo = SensorRepository(session)
            assignment_repo = SiteIspAssignmentRepository(session)
            site_repo = SiteRepository(session)
            alert_state_repo = AlertStateRepository(session)
            alert_repo = AlertHistoryRepository(session)

            sensor = sensor_repo.get(sensor_id)
            if not sensor:
                logger.error("Sensor with ID %s not found in database.", sensor_id)
                return

            assignment = assignment_repo.get(sensor.site_isp_assignment_id)
            if not assignment:
                logger.error("Assignment ID %s for sensor %s not found.", sensor.site_isp_assignment_id, sensor_id)
                return

            site = site_repo.get(assignment.site_id)
            if not site:
                logger.error("Site ID %s for assignment %s not found.", assignment.site_id, assignment.assignment_id)
                return

            site_id = site.site_id
            site_name = getattr(site, "site_name", str(site_id))
            sensor_name = getattr(sensor, "sensor_name", str(sensor_id))

            logger.info("Fetched target for sensor %s: Site ID %s (%s)", sensor_id, site_id, site_name)

            try:
                paused_state = alert_state_repo.get_by_name(PAUSED_STATE_NAME)
                if paused_state is None:
                    logger.error(
                        "ALERT_STATES row '%s' not found; cannot create alert_history for sensor %s.",
                        PAUSED_STATE_NAME,
                        sensor_id,
                    )
                else:
                    alert = alert_repo.create(
                        sensor_id=sensor_id,
                        state_id=paused_state.state_id,
                        alert_message=message,
                        escalation_status="Pending",
                    )
                    alert_id = alert.alert_id
                    logger.info("Created alert_history %s for sensor %s", alert_id, sensor_id)
            except RepositoryError:
                logger.exception("Failed to create alert_history for sensor %s", sensor_id)
            except Exception:
                logger.exception("Unexpected error creating alert_history for sensor %s", sensor_id)

        # ------------------------------------------------------------------
        # Phase 2: Send notification to support team (outside DB transaction)
        # ------------------------------------------------------------------
        notification_payload = {
            "site_name": site_name,
            "sensor_name": sensor_name,
            "status": status_str,
            "message": message,
            "timestamp": timestamp.isoformat(),
        }

        email_sent = False
        try:
            email_sent = await asyncio.to_thread(send_paused_notification, notification_payload)
        except Exception:
            logger.exception("Unexpected error dispatching paused notification for sensor %s", sensor_id)
            email_sent = False

        # ------------------------------------------------------------------
        # Phase 3: Record escalation audit trail & SENSOR_LOGS entry in DB
        # ------------------------------------------------------------------
        escalation_status = "Escalated to SUPPORT TEAM" if email_sent else "Support notification failed"
        support_email = settings.support_team_email or "support@example.com"
        raw_payload_data = serialize_payload_for_json(payload)

        with session_scope(SessionLocal) as session:
            alert_repo = AlertHistoryRepository(session)
            escalation_repo = EscalationRecordRepository(session)
            log_repo = SensorLogRepository(session)

            if alert_id is not None:
                try:
                    escalation_repo.create(
                        alert_id=alert_id,
                        escalated_to="SUPPORT TEAM",
                        recipient_email=support_email,
                        cc_emails=None,
                        email_subject="Sensor Paused Alert",
                        email_body=message,
                        response_received=False,
                        response_notes=None if email_sent else "Email dispatch failed",
                    )
                    alert_repo.update(alert_id, escalation_status=escalation_status)
                    logger.info("Recorded SUPPORT TEAM escalation for alert_id=%s (sent=%s)", alert_id, email_sent)
                except (NotFoundError, DuplicateError, ConstraintViolationError, RepositoryError) as exc:
                    logger.error("Error recording escalation for alert_id=%s: %s", alert_id, exc)
                except Exception:
                    logger.exception("Unexpected error recording escalation for alert_id=%s", alert_id)

            try:
                log_entry = log_repo.create(
                    sensor_id=sensor_id,
                    log_timestamp=timestamp,
                    log_level=LogLevelType.INFO,
                    log_status=LogStatusType.OPENED,
                    log_message=f"Sensor registered PAUSED state. Support notification {'sent' if email_sent else 'failed'}.",
                    log_details=json_safe_dict({
                        "site_id": site_id,
                        "alert_id": alert_id,
                        "status": status_str,
                        "message": message,
                        "email_sent": email_sent,
                        "escalation_status": escalation_status,
                        "raw_payload": raw_payload_data,
                    }),
                )
                logger.info(
                    "Successfully created sensor_log entry (ID: %s) for sensor %s",
                    getattr(log_entry, "log_id", "N/A"),
                    sensor_id,
                )
            except RepositoryError:
                logger.exception("Failed to create sensor_log for sensor %s (alert_id=%s)", sensor_id, alert_id)

        logger.info(
            "Paused workflow completed for sensor %s (alert_id=%s, email_sent=%s)",
            sensor_id,
            alert_id,
            email_sent,
        )


def process(payload: Any) -> None:
    """Synchronous entry point for Celery task worker."""
    logger.info("Received request to process Paused workflow payload.")
    workflow = PausedWorkflow()
    asyncio.run(workflow.execute(payload))