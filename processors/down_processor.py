import asyncio
import datetime
import json
import logging

from app.crud import (
    AlertHistoryRepository,
    AlertStateRepository,
    RepositoryError,
    SensorLogRepository,
    SensorRepository,
    SiteIspAssignmentRepository,
    SiteRepository,
    session_scope,
)
from app.database import SessionLocal
from app.models import LogLevelType, LogStatusType
from services.ping_service import PingIp

logger = logging.getLogger(__name__)

# Name of the ALERT_STATES row that represents a sensor-down condition.
DOWN_STATE_NAME = "Down"


def _serialize_payload_for_json(payload) -> dict:
    """Safely convert payload objects (Pydantic models, dicts, etc.) into JSON-serializable dicts."""
    # Pydantic v2
    if hasattr(payload, "model_dump"):
        return payload.model_dump(mode="json")
    # Pydantic v1
    if hasattr(payload, "dict"):
        return json.loads(payload.json())
    # Standard dict or fallback
    if isinstance(payload, dict):
        return json.loads(json.dumps(payload, default=str))
    return {"raw": str(payload)}


class DownWorkflow:

    async def execute(self, payload):
        logger.info("Executing DownWorkflow...")

        # Extract sensor_id safely
        if hasattr(payload, "sensor_id"):
            sensor_id = payload.sensor_id
        elif isinstance(payload, dict):
            sensor_id = payload.get("sensor_id")
        else:
            sensor_id = getattr(payload, "sensor_id", None)

        if not sensor_id:
            logger.error("Payload missing 'sensor_id': %s", payload)
            return

        logger.info("Processing DownWorkflow for sensor_id: %s", sensor_id)

        with session_scope(SessionLocal) as session:
            sensor_repo = SensorRepository(session)
            assignment_repo = SiteIspAssignmentRepository(session)
            site_repo = SiteRepository(session)
            log_repo = SensorLogRepository(session)
            alert_state_repo = AlertStateRepository(session)
            alert_repo = AlertHistoryRepository(session)

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
            primary_ip = str(site.primary_ip)

            logger.info(
                "Fetched target for sensor %s: Site ID %s, IP %s",
                sensor_id,
                site_id,
                primary_ip,
            )

            # -----------------------------------------------------------
            # Create alert_history row BEFORE running the ping diagnostic,
            # so the resulting alert_id can be attached to the ping result
            # and to the sensor_log entry below.
            # -----------------------------------------------------------
            alert_id = None
            try:
                down_state = alert_state_repo.get_by_name(DOWN_STATE_NAME)
                if down_state is None:
                    logger.error(
                        "ALERT_STATES row '%s' not found; cannot create alert_history "
                        "for sensor %s. Continuing without an alert_id.",
                        DOWN_STATE_NAME,
                        sensor_id,
                    )
                else:
                    alert = alert_repo.create(
                        sensor_id=sensor_id,
                        state_id=down_state.state_id,
                        alert_message="Sensor went DOWN",
                        escalation_status="Pending",
                    )
                    alert_id = alert.alert_id
                    logger.info(
                        "Created alert_history %s for sensor %s", alert_id, sensor_id
                    )
            except RepositoryError:
                # Don't let alert-creation failures block the down workflow --
                # log it and continue without an alert_id.
                logger.exception(
                    "Failed to create alert_history for sensor %s", sensor_id
                )
            except Exception:
                logger.exception(
                    "Unexpected error creating alert_history for sensor %s", sensor_id
                )

            # Execute ping service
            ping_service = PingIp()
            ping_payload = {
                "site_id": site_id,
                "target_ip": primary_ip,
                "sensor_id": sensor_id,
                "alert_id": alert_id,  # may be None if alert creation failed above
            }

            logger.info(
                "Triggering PingIp diagnostic service for target IP %s (sensor_id=%s, alert_id=%s)",
                primary_ip,
                sensor_id,
                alert_id,
            )

            try:
                ping_results = await ping_service.execute(ping_payload)
                logger.info(
                    "Ping service execution completed for sensor %s. Results: %s",
                    sensor_id,
                    ping_results,
                )
            except Exception:
                logger.exception(
                    "Ping service execution failed for sensor %s (alert_id=%s)",
                    sensor_id,
                    alert_id,
                )
                ping_results = None

            # Safely convert payload into JSON-serializable primitive dict
            raw_payload_data = _serialize_payload_for_json(payload)

            # Create sensor_log entry
            try:
                log_entry = log_repo.create(
                    sensor_id=sensor_id,
                    log_timestamp=datetime.datetime.now(datetime.timezone.utc),
                    log_level=LogLevelType.CRITICAL,
                    log_status=LogStatusType.OPENED,
                    log_message=f"Sensor registered DOWN. Executed ping diagnostic against {primary_ip}.",
                    log_details={
                        "site_id": site_id,
                        "target_ip": primary_ip,
                        "alert_id": alert_id,
                        "ping_results": ping_results,
                        "raw_payload": raw_payload_data,
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
            "Down workflow completed successfully for sensor %s (alert_id=%s)",
            sensor_id,
            alert_id,
        )


def process(payload):
    logger.info("Received request to process Down workflow payload.")
    workflow = DownWorkflow()
    asyncio.run(workflow.execute(payload))