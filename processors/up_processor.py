"""Up Workflow Processor.

Triggered when a sensor recovers and transitions back into the 'Up' state.
Closes open alerts and logs a resolution entry marking the issue as CLOSED.
"""
from __future__ import annotations

import asyncio
import datetime
import logging
from typing import Any

from app.crud import (
    AlertHistoryRepository,
    RepositoryError,
    SensorLogRepository,
    SensorRepository,
    session_scope,
)
from app.database import SessionLocal
from app.models import LogLevelType, LogStatusType
from processors.base import extract_sensor_id, json_safe_dict, serialize_payload_for_json

logger = logging.getLogger(__name__)


class UpWorkflow:
    """Workflow handler for UP (recovery) sensor events."""

    async def execute(self, payload: Any) -> None:
        logger.info("Executing UpWorkflow...")

        sensor_id = extract_sensor_id(payload)
        if not sensor_id:
            logger.error("Payload missing or invalid 'sensor_id': %s", payload)
            return

        logger.info("Processing UpWorkflow for sensor_id: %s", sensor_id)
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        raw_payload_data = serialize_payload_for_json(payload)

        with session_scope(SessionLocal) as session:
            sensor_repo = SensorRepository(session)
            log_repo = SensorLogRepository(session)
            alert_repo = AlertHistoryRepository(session)

            sensor = sensor_repo.get(sensor_id)
            if not sensor:
                logger.error("Sensor with ID %s not found in database.", sensor_id)
                return

            # Close open sensor log entries
            closed_log_count = log_repo.close_open_logs(sensor_id)
            logger.info("Closed %d open sensor_log entries for sensor %s", closed_log_count, sensor_id)

            # Resolve any open alerts for this sensor
            try:
                open_alerts = alert_repo.list_for_sensor(sensor_id, limit=20)
                for alert in open_alerts:
                    if alert.resolved_at is None:
                        alert_repo.resolve(alert.alert_id, resolved_at=timestamp)
                        logger.info("Resolved open alert_id=%s for sensor %s", alert.alert_id, sensor_id)
            except Exception:
                logger.exception("Error resolving open alerts for sensor %s", sensor_id)

            # Create a resolution log entry marking the state as CLOSED
            try:
                log_entry = log_repo.create(
                    sensor_id=sensor_id,
                    log_timestamp=timestamp,
                    log_level=LogLevelType.INFO,
                    log_status=LogStatusType.CLOSED,
                    log_message="Sensor recovered and registered UP. Issue marked as CLOSED.",
                    log_details=json_safe_dict({
                        "raw_payload": raw_payload_data,
                    }),
                )
                logger.info(
                    "Successfully created resolution sensor_log entry (ID: %s) for sensor %s",
                    getattr(log_entry, "log_id", "N/A"),
                    sensor_id,
                )
            except RepositoryError:
                logger.exception("Failed to create resolution sensor_log for sensor %s", sensor_id)

        logger.info("Up workflow completed successfully for sensor %s", sensor_id)


def process(payload: Any) -> None:
    """Synchronous entry point for Celery task worker."""
    logger.info("Received request to process Up workflow payload.")
    workflow = UpWorkflow()
    asyncio.run(workflow.execute(payload))