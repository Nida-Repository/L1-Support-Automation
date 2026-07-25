from fastapi import FastAPI
from models.prtg_alert import PRTGWebhookPayload

app = FastAPI()

@app.post("/webhooks/prtg")
async def receive_prtg_webhook(payload: PRTGWebhookPayload):
    # handle payload here (e.g., push to Celery task, since you have celery installed)
    return {"received": True, "sensor": payload.sensor_name, "status": payload.status}