from fastapi import FastAPI
from models.prtg_alert import PRTGWebhookPayload

app = FastAPI()
 #To test prtg_alert.py
@app.post("/webhooks/prtg")
async def receive_prtg_webhook(payload: PRTGWebhookPayload):
   
    return {"received": True, "sensor": payload.sensor_name, "status": payload.status}