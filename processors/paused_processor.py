import asyncio
import datetime
import logging
import os

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
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# ALERT_STATES row that represents a sensor-paused condition as paused.
PAUSED_STATE_NAME = "Paused"

DEFAULT_ALERT_MESSAGE = "Sensor condition has been PAUSED"

load_dotenv()
SUPPORT_EMAIL = os.getenv("SUPPORT_TEAM_EMAIL")


def _extract_sensor_id(payload):
    if hasattr(payload, "sensor_id"):
        return payload.sensor_id
    if isinstance(payload, dict):
        return payload.get("sensor_id")
    return getattr(payload, "sensor_id", None)


def _extract_field(payload, field, default=None):
    """Pull an optional field off either a pydantic-style payload or a dict."""
    if isinstance(payload, dict):
        return payload.get(field, default)
    return getattr(payload, field, default)


class PausedWorkflow:

    async def execute(self, payload):
        logger.info("Executing PausedWorkflow...")

        sensor_id = _extract_sensor_id(payload)
        if not sensor_id:
            logger.error("Payload missing 'sensor_id': %s", payload)
            return

        logger.info("Processing PausedWorkflow for sensor_id: %s", sensor_id)

        # ------------------------------------------------------------------
        #  Fetch sensor / assignment / site details
        # ------------------------------------------------------------------
        with session_scope(SessionLocal) as session:
            sensor_repo = SensorRepository(session)
            assignment_repo = SiteIspAssignmentRepository(session)
            site_repo = SiteRepository(session)
            log_repo = SensorLogRepository(session)
            alert_state_repo = AlertStateRepository(session)
            alert_repo = AlertHistoryRepository(session)
            escalation_repo = EscalationRecordRepository(session)

            sensor = sensor_repo.get(sensor_id)
            if not sensor:
                logger.error("Sensor with ID %s not found.", sensor_id)
                return

            assignment = assignment_repo.get(sensor.site_isp_assignment_id)
            if not assignment:
                logger.error(
                    "Assignment ID %s for sensor %s not found.",
                    sensor.site_isp_assignment_id,
                    sensor_id,
                )
                return

            site = site_repo.get(assignment.site_id)
            if not site:
                logger.error(
                    "Site ID %s for assignment %s not found.",
                    assignment.site_id,
                    assignment.assignment_id,
                )
                return

            site_id = site.site_id
            site_name = getattr(site, "site_name", str(site_id))
            sensor_name = getattr(sensor, "sensor_name", str(sensor_id))

            logger.info(
                "Fetched target for sensor %s: Site ID %s (%s)",
                sensor_id,
                site_id,
                site_name,
            )

            raw_status = _extract_field(payload, "status", "Paused")
            if hasattr(raw_status, "value"):
                status_str = str(raw_status.value)
            else:
                status_str = str(raw_status).replace("SensorStatus.", "")

            message = _extract_field(payload, "message", DEFAULT_ALERT_MESSAGE)
            timestamp = datetime.datetime.now(datetime.timezone.utc)

            # ----------------------------------------------------------------
            #  Create ALERT_HISTORY record with the Paused state
            # ----------------------------------------------------------------
            alert_id = None
            try:
                paused_state = alert_state_repo.get_by_name(PAUSED_STATE_NAME)
                if paused_state is None:
                    logger.error(
                        "ALERT_STATES row '%s' not found; cannot create alert_history "
                        "for sensor %s. Continuing without an alert_id.",
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
                    logger.info(
                        "Created alert_history %s for sensor %s", alert_id, sensor_id
                    )
            except RepositoryError:
                logger.exception(
                    "Failed to create alert_history for sensor %s", sensor_id
                )
            except Exception:
                logger.exception(
                    "Unexpected error creating alert_history for sensor %s", sensor_id
                )

            # ----------------------------------------------------------------
            #  Send the paused email to the support team
            # ----------------------------------------------------------------
            notification_payload = {
                "site_name": site_name,
                "sensor_name": sensor_name,
                "status": status_str,
                "message": message,
                "timestamp": timestamp.isoformat(),
            }

            email_sent = False
            try:
                email_sent = await asyncio.to_thread(
                    send_paused_notification, notification_payload
                )
            except Exception:
                logger.exception(
                    "Unexpected error while sending paused notification for sensor %s",
                    sensor_id,
                )
                email_sent = False

            # ----------------------------------------------------------------
            #  Record escalation outcome + update ALERT_HISTORY
            # ----------------------------------------------------------------
            escalation_status = None
            if alert_id is not None:
                try:
                    if email_sent:
                        escalation_repo.create(
                            alert_id=alert_id,
                            escalated_to="SUPPORT TEAM",
                            recipient_email=SUPPORT_EMAIL,
                            cc_emails=None,
                            email_subject="Sensor Paused Alert",
                            email_body=message,
                            response_received=False,
                            response_notes=None,
                        )
                        escalation_status = "Escalated to SUPPORT TEAM"
                        logger.info(
                            "Recorded SUPPORT TEAM escalation for alert_id=%s", alert_id
                        )
                    else:
                        escalation_repo.create(
                            alert_id=alert_id,
                            escalated_to="SUPPORT TEAM",
                            recipient_email=SUPPORT_EMAIL,
                            cc_emails=None,
                            email_subject="Sensor Paused Alert",
                            email_body=message,
                            response_received=False,
                            response_notes="Email dispatch failed",
                        )
                        escalation_status = "Support notification failed"
                        logger.warning(
                            "Recorded failed SUPPORT TEAM escalation for alert_id=%s",
                            alert_id,
                        )

                    alert_repo.update(alert_id, escalation_status=escalation_status)

                except NotFoundError:
                    logger.error(
                        "alert_id=%s not found while recording escalation", alert_id
                    )
                except DuplicateError as exc:
                    logger.error(
                        "Duplicate escalation record for alert_id=%s: %s", alert_id, exc
                    )
                except ConstraintViolationError as exc:
                    logger.error(
                        "Constraint violation recording escalation for alert_id=%s: %s",
                        alert_id,
                        exc,
                    )
                except RepositoryError:
                    logger.exception(
                        "Repository error recording escalation for alert_id=%s", alert_id
                    )
                except Exception:
                    logger.exception(
                        "Unexpected error recording escalation for alert_id=%s", alert_id
                    )
            else:
                logger.warning(
                    "No alert_id available for sensor %s; skipping escalation record.",
                    sensor_id,
                )

            # ----------------------------------------------------------------
            #  Create SENSOR_LOGS entry documenting the paused state + outcome
            # ----------------------------------------------------------------
            try:
                log_entry = log_repo.create(
                    sensor_id=sensor_id,
                    log_timestamp=timestamp,
                    log_level=LogLevelType.INFO,  # Adjust log level as needed (e.g., LogLevelType.WARNING)
                    log_status=LogStatusType.OPENED,
                    log_message=(
                        f"Sensor registered PAUSED state. Support notification "
                        f"{'sent' if email_sent else 'failed'}."
                    ),
                    log_details={
                        "site_id": site_id,
                        "alert_id": alert_id,
                        "status": status_str,
                        "message": message,
                        "email_sent": email_sent,
                        "escalation_status": escalation_status,
                    },
                )
                logger.info(
                    "Successfully created sensor_log entry (ID: %s) for sensor %s",
                    getattr(log_entry, "log_id", "N/A"),
                    sensor_id,
                )
            except RepositoryError:
                logger.exception(
                    "Failed to create sensor_log for sensor %s (alert_id=%s)",
                    sensor_id,
                    alert_id,
                )

        logger.info(
            "Paused workflow completed for sensor %s (alert_id=%s, email_sent=%s)",
            sensor_id,
            alert_id,
            email_sent,
        )


# Entry point for Celery
def process(payload):
    logger.info("Received request to process Paused workflow payload.")
    workflow = PausedWorkflow()
    asyncio.run(workflow.execute(payload))