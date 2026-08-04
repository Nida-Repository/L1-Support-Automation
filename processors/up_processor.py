import asyncio
import datetime
import json
import logging

from app.crud import (
    RepositoryError,
    SensorLogRepository,
    SensorRepository,
    session_scope,
)
from app.database import SessionLocal
from app.models import LogLevelType, LogStatusType

logger = logging.getLogger(__name__)


def _serialize_payload_for_json(payload) -> dict:
    """Safely convert payload objects into JSON-serializable dicts."""
    if hasattr(payload, "model_dump"):
        return payload.model_dump(mode="json")
    if hasattr(payload, "dict"):
        return json.loads(payload.json())
    if isinstance(payload, dict):
        return json.loads(json.dumps(payload, default=str))
    return {"raw": str(payload)}


class UpWorkflow:

    async def execute(self, payload):
        logger.info("Executing UpWorkflow...")

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

        logger.info("Processing UpWorkflow for sensor_id: %s", sensor_id)

        with session_scope(SessionLocal) as session:
            sensor_repo = SensorRepository(session)
            log_repo = SensorLogRepository(session)

            sensor = sensor_repo.get(sensor_id)
            if not sensor:
                logger.error("Sensor with ID %s not found.", sensor_id)
                return


            # Create a resolution log entry marking the state as CLOSED
            raw_payload_data = _serialize_payload_for_json(payload)
            try:
                log_entry = log_repo.create(
                    sensor_id=sensor_id,
                    log_timestamp=datetime.datetime.now(datetime.timezone.utc),
                    log_level=LogLevelType.INFO,
                    log_status=LogStatusType.CLOSED,
                    log_message="Sensor recovered and registered UP. Issue marked as CLOSED.",
                    log_details={
                        "raw_payload": raw_payload_data,
                    },
                )
                logger.info(
                    "Successfully created resolution sensor_log entry (ID: %s) for sensor %s",
                    getattr(log_entry, "log_id", "N/A"),
                    sensor_id,
                )
            except RepositoryError:
                logger.exception(
                    "Failed to create resolution sensor_log for sensor %s", sensor_id
                )

        logger.info(
            "Up workflow completed successfully for sensor %s (Log status set to CLOSED)",
            sensor_id,
        )


def process(payload):
    logger.info("Received request to process Up workflow payload.")
    workflow = UpWorkflow()
    asyncio.run(workflow.execute(payload))