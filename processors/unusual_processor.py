"""Unusual Workflow Processor.

Triggered when a sensor transitions into the 'Unusual' state in PRTG.
Logs the unusual event into the database and records telemetry for diagnostics.
"""
from __future__ import annotations

import asyncio
import datetime
import logging
from typing import Any

from app.crud import (
    RepositoryError,
    SensorLogRepository,
    SensorRepository,
    session_scope,
)
from app.database import SessionLocal
from app.models import LogLevelType, LogStatusType
from processors.base import (
    extract_field,
    extract_sensor_id,
    json_safe_dict,
    sanitize_status,
    serialize_payload_for_json,
)

logger = logging.getLogger(__name__)

DEFAULT_ALERT_MESSAGE = "Sensor reported an UNUSUAL condition"


class UnusualWorkflow:
    """Workflow handler for UNUSUAL sensor events."""

    async def execute(self, payload: Any) -> None:
        logger.info("Executing UnusualWorkflow...")

        sensor_id = extract_sensor_id(payload)
        if not sensor_id:
            logger.error("Payload missing or invalid 'sensor_id': %s", payload)
            return

        logger.info("Processing UnusualWorkflow for sensor_id: %s", sensor_id)

        raw_status = extract_field(payload, "status", "Unusual")
        status_str = sanitize_status(raw_status, default="Unusual")
        message = extract_field(payload, "message", DEFAULT_ALERT_MESSAGE)
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        raw_payload_data = serialize_payload_for_json(payload)

        with session_scope(SessionLocal) as session:
            sensor_repo = SensorRepository(session)
            log_repo = SensorLogRepository(session)

            sensor = sensor_repo.get(sensor_id)
            if not sensor:
                logger.error("Sensor with ID %s not found in database.", sensor_id)
                return

            try:
                log_entry = log_repo.create(
                    sensor_id=sensor_id,
                    log_timestamp=timestamp,
                    log_level=LogLevelType.INFO,
                    log_status=LogStatusType.OPENED,
                    log_message=f"Sensor registered UNUSUAL condition: {message}",
                    log_details=json_safe_dict({
                        "status": status_str,
                        "message": message,
                        "raw_payload": raw_payload_data,
                    }),
                )
                logger.info(
                    "Successfully created sensor_log entry (ID: %s) for sensor %s",
                    getattr(log_entry, "log_id", "N/A"),
                    sensor_id,
                )
            except RepositoryError:
                logger.exception("Failed to create sensor_log for unusual sensor %s", sensor_id)

        logger.info("Unusual workflow completed successfully for sensor %s", sensor_id)


def process(payload: Any) -> None:
    """Synchronous entry point for Celery task worker."""
    logger.info("Received request to process Unusual workflow payload.")
    workflow = UnusualWorkflow()
    asyncio.run(workflow.execute(payload))