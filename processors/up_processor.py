import asyncio
import datetime
import json
import logging

from app.crud import (
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

        with session_scope(SessionLocal) as session:
            sensor_repo = SensorRepository(session)
            log_repo = SensorLogRepository(session)

            sensor = sensor_repo.get(sensor_id)
            if not sensor:
                logger.error("Sensor with ID %s not found.", sensor_id)
                return

            # Option A: If your log_repo has an update function (e.g. update_status_by_sensor)
            # log_repo.close_open_logs_for_sensor(sensor_id)

            # Option B: Create a resolution log entry marking the state as CLOSED
            raw_payload_data = _serialize_payload_for_json(payload)
            log_repo.create(
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
            "Up workflow completed successfully for sensor %s (Log status set to CLOSED)",
            sensor_id,
        )


def process(payload):
    workflow = UpWorkflow()
    asyncio.run(workflow.execute(payload))