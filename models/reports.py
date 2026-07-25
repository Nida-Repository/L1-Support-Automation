#Represents a generated report.
from datetime import datetime
from pydantic import BaseModel


class Report(BaseModel):
    report_name: str
    generated_at: datetime
    total_incidents: int
    resolved_incidents: int
    unresolved_incidents: int