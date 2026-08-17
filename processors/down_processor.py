"""Down Workflow Processor.

Triggered when a sensor transitions into the 'Down' state.
Executes diagnostic ping batches against the site's primary IP, records
alert history, and logs diagnostic results.
"""
from __future__ import annotations

import asyncio
import datetime
import logging
from typing import Any

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
from processors.base import extract_sensor_id, json_safe_dict, serialize_payload_for_json
from services.ping_service import PingIp

logger = logging.getLogger(__name__)

DOWN_STATE_NAME = "Down"


class DownWorkflow:
    """Workflow handler for DOWN sensor events."""

    async def execute(self, payload: Any) -> None:
        logger.info("Executing DownWorkflow...")

        sensor_id = extract_sensor_id(payload)
        if not sensor_id:
            logger.error("Payload missing or invalid 'sensor_id': %s", payload)
            return

        logger.info("Processing DownWorkflow for sensor_id: %s", sensor_id)

        # -----------------------------------------------------------
        # Transaction 1: Fetch topology and create ALERT_HISTORY row
        # -----------------------------------------------------------
        site_id = None
        primary_ip = None
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

            logger.info("Fetched target for sensor %s: Site ID %s, IP %s", sensor_id, site_id, primary_ip)

            try:
                down_state = alert_state_repo.get_by_name(DOWN_STATE_NAME)
                if down_state is None:
                    logger.error(
                        "ALERT_STATES row '%s' not found; cannot create alert_history for sensor %s.",
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
                    logger.info("Created alert_history %s for sensor %s", alert_id, sensor_id)
            except RepositoryError:
                logger.exception("Failed to create alert_history for sensor %s", sensor_id)
            except Exception:
                logger.exception("Unexpected error creating alert_history for sensor %s", sensor_id)

        # -----------------------------------------------------------
        # Non-DB Phase: Ping Diagnostics
        # -----------------------------------------------------------
        ping_service = PingIp()
        ping_payload = {
            "site_id": site_id,
            "target_ip": primary_ip,
            "sensor_id": sensor_id,
            "alert_id": alert_id,
        }

        logger.info(
            "Triggering PingIp diagnostic service for target IP %s (sensor_id=%s, alert_id=%s)",
            primary_ip,
            sensor_id,
            alert_id,
        )

        ping_results = None
        try:
            ping_results = await ping_service.execute(ping_payload)
            logger.info("Ping service execution completed for sensor %s", sensor_id)
        except Exception:
            logger.exception(
                "Ping service execution failed for sensor %s (alert_id=%s)",
                sensor_id,
                alert_id,
            )

        raw_payload_data = serialize_payload_for_json(payload)

        # -----------------------------------------------------------
        # Transaction 2: Persist SENSOR_LOGS entry
        # -----------------------------------------------------------
        with session_scope(SessionLocal) as session:
            log_repo = SensorLogRepository(session)
            try:
                log_entry = log_repo.create(
                    sensor_id=sensor_id,
                    log_timestamp=datetime.datetime.now(datetime.timezone.utc),
                    log_level=LogLevelType.CRITICAL,
                    log_status=LogStatusType.OPENED,
                    log_message=f"Sensor registered DOWN. Executed ping diagnostic against {primary_ip}.",
                    log_details=json_safe_dict({
                        "site_id": site_id,
                        "target_ip": primary_ip,
                        "alert_id": alert_id,
                        "ping_results": ping_results,
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

        logger.info("Down workflow completed successfully for sensor %s (alert_id=%s)", sensor_id, alert_id)


def process(payload: Any) -> None:
    """Synchronous entry point for Celery task worker."""
    logger.info("Received request to process Down workflow payload.")
    workflow = DownWorkflow()
    asyncio.run(workflow.execute(payload))