#Represents an incident that system creates after processing an alert.
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Incident(BaseModel):
    incident_id: str
    sensor_id: int
    status: str
    severity: str
    opened_at: datetime
    closed_at: Optional[datetime] = None
    root_cause: Optional[str] = None