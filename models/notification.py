#Represents an email, SMS, Teams, or Slack notification.
from datetime import datetime
from pydantic import BaseModel


class Notification(BaseModel):
    recipient: str
    subject: str
    message: str
    channel: str
    sent_at: datetime
    success: bool = False