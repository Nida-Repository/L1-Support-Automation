#Represents an alert received from the PRTG webhook
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)


class SensorStatus(str, Enum):
    """PRTG's %laststatus / %status placeholder values."""
    UP = "Up"
    DOWN = "Down"
    WARNING = "Warning"
    UNUSUAL = "Unusual"
    PAUSED = "Paused"
    # UNKNOWN = "Unknown"
    # DOWN_ACKNOWLEDGED = "Down (Acknowledged)"


class PRTGWebhookPayload(BaseModel):

    model_config = ConfigDict(
        populate_by_name=True,   # allows instantiation via field name OR alias
        str_strip_whitespace=True,
        extra="ignore",          # don't 422 on extra PRTG placeholders you didn't map
        json_schema_extra={
            "example": {
                "sensorid": "12345",
                "sensorname": "Ping",
                "device": "core-switch-01",
                "host": "10.10.1.1",
                "laststatus": "Down",
                "message": "Request timed out.",
                "lastcheck": "2026-07-25 09:12:03",
                "lastup": "2026-07-25 08:57:03",
                "lastdown": "2026-07-25 09:12:03",
                "comments": "Primary uplink switch",
                "commentsdevice": "Rack 3, Datacenter A",
                "history": "09:12:03 Down; 08:57:03 Up",
            }
        },
    )

    # --- Core sensor details ---
    # MANDATORY FIELDS:
    #                  SENSOR ID, SENSOR NAME, STATUS
    sensor_id: int = Field(..., alias="sensorid")
    sensor_name: str = Field(..., alias="sensorname")
    device_name: Optional[str] = Field(default=None, alias="lastup")
    host_ip: Optional[str] = Field(default=None, alias="lastup")
    status: SensorStatus = Field(..., alias="laststatus")
    message: Optional[str] = Field(default=None, alias="message")

    # --- Timestamps ---
    # Server-side receipt time - NOT sent by PRTG, always current UTC time.
    received_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    # PRTG timestamp placeholders come in PRTG's own locale-dependent
    # date format, not ISO 8601 - keep as raw strings, don't force datetime.
    last_check: Optional[str] = Field(default=None, alias="lastcheck")
    last_up: Optional[str] = Field(default=None, alias="lastup")
    last_down: Optional[str] = Field(default=None, alias="lastdown")

    # --- Comments & history ---
    comments_sensor: Optional[str] = Field(default=None, alias="comments")
    comments_device: Optional[str] = Field(default=None, alias="commentsdevice")
    history: Optional[str] = Field(default=None, alias="history")

    # --- Validators ---
    @field_validator("sensor_id", mode="before")
    @classmethod
    def coerce_sensor_id(cls, v):
        """PRTG sends %sensorid as a string; coerce cleanly and error clearly if not numeric."""
        if isinstance(v, str):
            v = v.strip()
            if not v.isdigit():
                raise ValueError(f"sensorid must be numeric, got {v!r}")
            return int(v)
        return v

    @field_validator("status", mode="before")
    @classmethod
    def normalize_status(cls, v):
        """Guard against case/whitespace drift in %laststatus."""
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator(
        "message",
        "comments_sensor",
        "comments_device",
        "history",
        mode="before",
    )
    @classmethod
    def empty_string_to_none(cls, v):
        """PRTG sends empty string '' rather than omitting the key when no value exists."""
        if isinstance(v, str) and v.strip() == "":
            return None
        return v