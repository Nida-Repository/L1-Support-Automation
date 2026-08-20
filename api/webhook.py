"""FastAPI Inbound Webhook Gateway for PRTG Alerts.

Provides authenticated webhook ingestion endpoints for PRTG Network Monitor,
best-effort site context enrichment via Redis, and queuing to RabbitMQ.
"""
from __future__ import annotations

import logging
import secrets
from typing import Any, Dict

from fastapi import Depends, FastAPI, Header, HTTPException, status
from kombu.exceptions import OperationalError as KombuOperationalError

from api.alerts import router as alerts_router
from api.attachments import router as attachments_router
from api.auth import router as auth_router
from api.closure import router as closure_router
from api.email_threads import router as email_threads_router
from api.notifications import router as notifications_router
from api.root_cause import router as root_cause_router
from cache.redis_cache import CacheService, IncidentStateTracker
from config.logging_config import setup_logging
from config.settings import settings
from models.prtg_alert import PRTGWebhookPayload
from task_queue.tasks import process_prtg_webhook_task

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="L1 Support Automation Gateway",
    description="Automated L1 Incident Management, Diagnostics, and Root Cause Analysis Gateway",
    version="1.0.0",
)

# Public & Authenticated Routers
app.include_router(auth_router)
app.include_router(alerts_router)
app.include_router(root_cause_router)
app.include_router(attachments_router)
app.include_router(closure_router)
app.include_router(notifications_router)
app.include_router(email_threads_router)


_WEBHOOK_SECRET = settings.prtg_webhook_secret

if not _WEBHOOK_SECRET:
    _WEBHOOK_SECRET = secrets.token_urlsafe(32)
    logger.warning("PRTG_WEBHOOK_SECRET is not configured in environment. Generated ephemeral secret for this process.")
    logger.warning("To persist authorization, set PRTG_WEBHOOK_SECRET in your environment or .env file.")


def authenticate_prtg(x_prtg_token: str = Header(None, alias="X-PRTG-Token")) -> None:
    """Validate incoming PRTG authentication token using constant-time comparison."""
    if not x_prtg_token or not secrets.compare_digest(x_prtg_token, _WEBHOOK_SECRET):
        logger.warning("Authentication failed: Invalid or missing X-PRTG-Token header")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing PRTG authentication token",
        )


@app.post("/webhook/prtg", status_code=status.HTTP_200_OK)
async def receive_prtg_webhook(
    payload: PRTGWebhookPayload,
    authenticated: None = Depends(authenticate_prtg),
) -> Dict[str, Any]:
    """Ingest, enrich, and queue an incoming PRTG alert payload."""
    logger.info("Received PRTG webhook for sensor_id: %s (status: %s)", payload.sensor_id, payload.status.value)
    payload_dict = payload.model_dump(mode="json")

    # Best-effort cache enrichment
    try:
        site_context = CacheService.get_sensor_site_info(payload.sensor_id)
        if site_context:
            payload_dict["site_context"] = site_context
            logger.info("Enriched payload with site context for sensor %s", payload.sensor_id)
    except Exception as exc:
        logger.warning("Site-context lookup failed for sensor %s: %s", payload.sensor_id, exc)

    # Publish to RabbitMQ
    try:
        process_prtg_webhook_task.delay(payload_dict)
        logger.info("Successfully queued PRTG task for sensor_id: %s", payload.sensor_id)
    except KombuOperationalError as exc:
        logger.error("RabbitMQ broker unreachable while queuing sensor %s: %s", payload.sensor_id, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Message broker unavailable — webhook should be retried",
        )
    except Exception as exc:
        logger.error("Unexpected error publishing task for sensor %s: %s", payload.sensor_id, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to publish event to processing queue",
        )

    return {
        "status": "success",
        "message": "Webhook accepted and queued for processing",
        "sensor_id": payload.sensor_id,
    }


@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check() -> Dict[str, Any]:
    """Comprehensive readiness and liveness health check."""
    logger.debug("Executing system health check...")
    redis_ok = IncidentStateTracker.ping()

    broker_ok = True
    try:
        with process_prtg_webhook_task.app.connection_for_write() as conn:
            conn.ensure_connection(max_retries=1, timeout=2)
    except Exception as exc:
        broker_ok = False
        logger.warning("Health check: broker unreachable: %s", exc)

    healthy = redis_ok and broker_ok
    status_str = "healthy" if healthy else "degraded"

    if not healthy:
        logger.warning("Health check degraded (Redis: %s, Broker: %s)", redis_ok, broker_ok)
    else:
        logger.debug("Health check status: healthy")

    return {
        "status": status_str,
        "redis": "up" if redis_ok else "down",
        "broker": "up" if broker_ok else "down",
    }