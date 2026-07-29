import asyncio
import datetime
import json
import logging

from app.crud import (
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
            assignment_repo = SiteIspAssignmentRepository(session)
            site_repo = SiteRepository(session)
            log_repo = SensorLogRepository(session)

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

            # Execute ping service
            ping_service = PingIp()
            ping_payload = {
                "site_id": site_id,
                "target_ip": primary_ip,
                "sensor_id": sensor_id,
            }
            ping_results = await ping_service.execute(ping_payload)

            # Safely convert payload into JSON-serializable primitive dict
            raw_payload_data = _serialize_payload_for_json(payload)

            # Create sensor_log entry (will now commit successfully)
            log_repo.create(
                sensor_id=sensor_id,
                log_timestamp=datetime.datetime.now(datetime.timezone.utc),
                log_level=LogLevelType.CRITICAL,
                log_status=LogStatusType.OPENED,
                log_message=f"Sensor registered DOWN. Executed ping diagnostic against {primary_ip}.",
                log_details={
                    "site_id": site_id,
                    "target_ip": primary_ip,
                    "ping_results": ping_results,
                    "raw_payload": raw_payload_data,
                },
            )

        logger.info(
            "Down workflow completed successfully for sensor %s", sensor_id
        )


def process(payload):
    workflow = DownWorkflow()
    asyncio.run(workflow.execute(payload))