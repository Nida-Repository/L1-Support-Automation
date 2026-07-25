
from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Annotated, Any, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

# --------------------------------------------------------------------------- #
# Enums mirroring the Postgres ENUM types
# --------------------------------------------------------------------------- #


class LogLevel(str, enum.Enum):
    GOOD = "GOOD"
    INFO = "INFO"
    WARNING = "WARNING"
    SEVERE = "SEVERE"
    CRITICAL = "CRITICAL"


class LogStatus(str, enum.Enum):
    OPENED = "opened"
    CLOSED = "closed"


# --------------------------------------------------------------------------- #
# Shared field constraints
# --------------------------------------------------------------------------- #

# SENSORS.sensor_id is constrained to a 4-digit range in the DB; mirroring
# that here catches bad FK values before they ever reach the database.
SensorId = Annotated[int, Field(ge=1000, le=9999, description="FK -> SENSORS.sensor_id")]

# BIGINT identity column, sequence starts at 100.
LogId = Annotated[int, Field(ge=100, description="PK -> SENSOR_LOGS.log_id")]


# --------------------------------------------------------------------------- #
# Base
# --------------------------------------------------------------------------- #


class SensorLogBase(BaseModel):
    """Fields common to create/update/read representations."""

    model_config = ConfigDict(
        from_attributes=True,
        use_enum_values=False,
        str_strip_whitespace=True,
        extra="forbid",
        validate_assignment=True,
    )

    sensor_id: SensorId
    log_level: LogLevel = LogLevel.SEVERE
    log_status: LogStatus = LogStatus.OPENED
    log_message: Optional[str] = Field(default=None, max_length=10_000)
    log_details: Optional[dict[str, Any]] = None

    @field_validator("log_message")
    @classmethod
    def _empty_message_to_none(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v.strip() == "":
            return None
        return v

    @field_validator("log_details")
    @classmethod
    def _details_must_be_json_object(cls, v: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:

        if v is not None and not isinstance(v, dict):
            raise ValueError("log_details must be a JSON object (dict)")
        return v

class SensorLogCreate(SensorLogBase):
    """Payload for inserting a new sensor log row.

    log_id and log_timestamp are DB-generated (IDENTITY / DEFAULT
    CURRENT_TIMESTAMP)
    """

    pass


# --------------------------------------------------------------------------- #
# Update (inbound - e.g. PATCH /sensor-logs/{log_id})
# --------------------------------------------------------------------------- #


class SensorLogUpdate(BaseModel):
    """Partial update. Only log_status/log_message/log_details are realistically
    mutable post-insert (e.g. closing out a log entry with a note)."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    log_status: Optional[LogStatus] = None
    log_message: Optional[str] = Field(default=None, max_length=10_000)
    log_details: Optional[dict[str, Any]] = None

    @model_validator(mode="after")
    def _at_least_one_field(self) -> "SensorLogUpdate":
        if self.log_status is None and self.log_message is None and self.log_details is None:
            raise ValueError("at least one field must be provided for an update")
        return self


# --------------------------------------------------------------------------- #
# Read (outbound - e.g. GET /sensor-logs/{log_id}); also the ORM-facing model
# --------------------------------------------------------------------------- #


class SensorLogRead(SensorLogBase):
    """Response model. Built from a SQLAlchemy ORM row with:

        SensorLogRead.model_validate(orm_instance)

    which works because `from_attributes=True` is set on the model config
    (inherited from SensorLogBase).
    """

    log_id: LogId
    log_timestamp: datetime

    @field_validator("log_timestamp")
    @classmethod
    def _must_be_timezone_aware(cls, v: datetime) -> datetime:
        # Column is TIMESTAMPTZ; guard against naive datetimes slipping in
        # from a misconfigured session/driver.
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

if __name__ == "__main__":
    # Quick smoke test
    payload = {
        "sensor_id": 1042,
        "log_level": "CRITICAL",
        "log_status": "opened",
        "log_message": "Packet loss exceeded threshold",
        "log_details": {"packet_loss_percent": 42.5, "threshold": 20.0},
    }
    created = SensorLogCreate.model_validate(payload)
    print(created.model_dump_json(indent=2))

    read = SensorLogRead.model_validate(
        {
            "log_id": 101,
            "sensor_id": 1042,
            "log_timestamp": datetime.now(timezone.utc),
            "log_level": "CRITICAL",
            "log_status": "opened",
            "log_message": "Packet loss exceeded threshold",
            "log_details": {"packet_loss_percent": 42.5},
        }
    )
    print(read.model_dump_json(indent=2))