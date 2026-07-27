import os
import secrets
from fastapi import FastAPI, Header, HTTPException, status, Depends

from models.prtg_alert import PRTGWebhookPayload
from task_queue.tasks import process_prtg_webhook_task
from cache.redis_cache import CacheService

app = FastAPI(title="PRTG Webhook Gateway", version="1.0.0")

WEBHOOK_SECRET = os.getenv("PRTG_WEBHOOK_SECRET")

if not WEBHOOK_SECRET:
    WEBHOOK_SECRET = secrets.token_urlsafe(32)
    print("\n" + "=" * 60)
    print("WARNING: 'PRTG_WEBHOOK_SECRET' is not set in environment!")
    print(f"Generated temporary secret for PRTG header:\n\n  X-PRTG-Token: {WEBHOOK_SECRET}\n")
    print("Add PRTG_WEBHOOK_SECRET=<token> to your .env file for production.")
    print("=" * 60 + "\n")


def authenticate_prtg(x_prtg_token: str = Header(None, alias="X-PRTG-Token")):
    if not x_prtg_token or not secrets.compare_digest(x_prtg_token, WEBHOOK_SECRET):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing PRTG authentication token",
        )


@app.post("/webhook/prtg", status_code=status.HTTP_200_OK)
async def receive_prtg_webhook(
    payload: PRTGWebhookPayload,
    authenticated: None = Depends(authenticate_prtg)
):
    try:
        # Pull cached site information from Redis
        site_context = CacheService.get_sensor_site_info(payload.sensor_id)

        payload_dict = payload.model_dump(mode="json")
        if site_context:
            payload_dict["site_context"] = site_context

        process_prtg_webhook_task.delay(payload_dict)

        return {
            "status": "success",
            "message": "Webhook accepted and queued for processing",
            "sensor_id": payload.sensor_id
        }

    except Exception as err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish event to queue: {str(err)}"
        )


@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    return {"status": "healthy"}