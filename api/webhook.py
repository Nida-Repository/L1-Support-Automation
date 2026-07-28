import logging
import os
import secrets

from fastapi import FastAPI, Header, HTTPException, status, Depends
from kombu.exceptions import OperationalError as KombuOperationalError

from models.prtg_alert import PRTGWebhookPayload
from task_queue.tasks import process_prtg_webhook_task
from cache.redis_cache import CacheService, IncidentStateTracker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="PRTG Webhook Gateway", version="1.0.0")

WEBHOOK_SECRET = os.getenv("PRTG_WEBHOOK_SECRET")

if not WEBHOOK_SECRET:
    WEBHOOK_SECRET = secrets.token_urlsafe(32)
    logger.warning(
        "PRTG_WEBHOOK_SECRET is not set in environment! "
        f"Generated temporary secret for this process only: {WEBHOOK_SECRET}"
    )
    logger.warning("Add PRTG_WEBHOOK_SECRET=<token> to your .env file for production.")


def authenticate_prtg(x_prtg_token: str = Header(None, alias="X-PRTG-Token")):
    if not x_prtg_token or not secrets.compare_digest(x_prtg_token, WEBHOOK_SECRET):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing PRTG authentication token",
        )


@app.post("/webhook/prtg", status_code=status.HTTP_200_OK)
async def receive_prtg_webhook(
    payload: PRTGWebhookPayload,
    authenticated: None = Depends(authenticate_prtg),
):
    payload_dict = payload.model_dump(mode="json")

    # --- Cache lookup is best-effort — never block queuing on it ---
    try:
        site_context = CacheService.get_sensor_site_info(payload.sensor_id)
        if site_context:
            payload_dict["site_context"] = site_context
    except Exception as exc:
        logger.warning(f"Site-context lookup failed for sensor {payload.sensor_id}: {exc}")

    # --- Publish to RabbitMQ — this IS critical, must fail loudly ---
    try:
        process_prtg_webhook_task.delay(payload_dict)
    except KombuOperationalError as exc:
        logger.error(f"RabbitMQ unreachable while queuing sensor {payload.sensor_id}: {exc}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Message broker unavailable — PRTG should retry this webhook",
        )
    except Exception as exc:
        logger.error(f"Unexpected error publishing task for sensor {payload.sensor_id}: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to publish event to queue",
        )

    return {
        "status": "success",
        "message": "Webhook accepted and queued for processing",
        "sensor_id": payload.sensor_id,
    }


@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    redis_ok = IncidentStateTracker.ping()

    broker_ok = True
    try:
        with process_prtg_webhook_task.app.connection_for_write() as conn:
            conn.ensure_connection(max_retries=1, timeout=2)
    except Exception as exc:
        broker_ok = False
        logger.warning(f"Health check: broker unreachable: {exc}")

    healthy = redis_ok and broker_ok
    return {
        "status": "healthy" if healthy else "degraded",
        "redis": "up" if redis_ok else "down",
        "broker": "up" if broker_ok else "down",
    }