
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

# ---------------------------------------------------------------------------
# Shared constraints
# ---------------------------------------------------------------------------
# SENSORS.sensor_id and ALERT_STATES.state_id are both constrained to a
# 4-digit range at the DB layer (CHECK ... BETWEEN 1000 AND 9999).

FourDigitId = Field(ge=1000, le=9999)

MAX_ESCALATION_STATUS_LEN = 50


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------
class AlertHistoryBase(BaseModel):
    """Fields common to writing and reading an ALERT_HISTORY row."""

    model_config = ConfigDict(
        from_attributes=True,   # allows Model.model_validate(sqlalchemy_instance)
        str_strip_whitespace=True,
        extra="forbid",
    )

    sensor_id: int = FourDigitId
    state_id: int = FourDigitId
    alert_message: Optional[str] = Field(
        default=None,
        description="Free-form alert text (maps to TEXT column, no DB length cap).",
    )
    escalation_status: Optional[str] = Field(
        default=None,
        max_length=MAX_ESCALATION_STATUS_LEN,
        description="Human-readable escalation status, e.g. 'Pending', 'Escalated to ISP'.",
    )

    @field_validator("alert_message", "escalation_status", mode="before")
    @classmethod
    def _blank_to_none(cls, v: Optional[str]) -> Optional[str]:
        """Treat empty/whitespace-only strings as NULL, matching typical DB semantics."""
        if isinstance(v, str) and v.strip() == "":
            return None
        return v


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------
class AlertHistoryCreate(AlertHistoryBase):
    """Payload for inserting a new alert.
    """

    pass


class AlertHistoryImport(AlertHistoryCreate):

    triggered_at: datetime


# ---------------------------------------------------------------------------
# Update (PATCH-style, all fields optional)
# ---------------------------------------------------------------------------
class AlertHistoryUpdate(BaseModel):
    """Partial update payload.
    """

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    state_id: Optional[int] = Field(default=None, ge=1000, le=9999)
    resolved_at: Optional[datetime] = None
    alert_message: Optional[str] = None
    escalation_status: Optional[str] = Field(default=None, max_length=MAX_ESCALATION_STATUS_LEN)

    @field_validator("alert_message", "escalation_status", mode="before")
    @classmethod
    def _blank_to_none(cls, v: Optional[str]) -> Optional[str]:
        if isinstance(v, str) and v.strip() == "":
            return None
        return v


# ---------------------------------------------------------------------------
# Read / Response
# ---------------------------------------------------------------------------
class AlertHistoryRead(AlertHistoryBase):
    """Full row shape returned to API clients, and the shape you get back
    from `AlertHistoryRead.model_validate(orm_obj)` when orm_obj is a
    SQLAlchemy ALERT_HISTORY instance (thanks to from_attributes=True).
    """

    alert_id: int = Field(ge=100, description="IDENTITY PK, starts at 100.")
    triggered_at: datetime
    resolved_at: Optional[datetime] = None

    @model_validator(mode="after")
    def _resolved_not_before_triggered(self) -> "AlertHistoryRead":
        if self.resolved_at is not None and self.resolved_at < self.triggered_at:
            raise ValueError("resolved_at cannot be earlier than triggered_at")
        return self

    @property
    def is_open(self) -> bool:
        """Convenience flag: True while the alert has not been resolved."""
        return self.resolved_at is None


# ---------------------------------------------------------------------------
# Optional: lightweight nested reference, useful if you ever expand
# AlertHistoryRead to embed sensor/state info without pulling every column.
# ---------------------------------------------------------------------------
class AlertStateRef(BaseModel):
    """Minimal ALERT_STATES reference, e.g. for embedding in AlertHistoryRead
    as `state: AlertStateRef` if you join the lookup table in your query.
    """

    model_config = ConfigDict(from_attributes=True)

    state_id: int = Field(ge=1000, le=9999)
    state_name: str


__all__ = [
    "AlertHistoryBase",
    "AlertHistoryCreate",
    "AlertHistoryImport",
    "AlertHistoryUpdate",
    "AlertHistoryRead",
    "AlertStateRef",
]