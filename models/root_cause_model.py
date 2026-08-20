"""Pydantic v2 Schemas for Root Cause Analysis (RCA).

Provides strict validation and serialization for incident root cause submissions,
updates, and read responses.
"""
from __future__ import annotations

import datetime
from typing import Any, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class RootCauseBase(BaseModel):
    """Base fields for Root Cause Analysis."""

    model_config = ConfigDict(
        from_attributes=True,
        str_strip_whitespace=True,
        extra="forbid",
    )

    root_cause_name: str = Field(
        ...,
        min_length=3,
        max_length=100,
        description="Descriptive title of the root cause (e.g. 'Fiber Cut on Metro Route')",
    )
    category: str = Field(
        ...,
        min_length=2,
        max_length=100,
        description="Category (e.g. 'Hardware', 'Fiber Cut', 'Power Outage', 'Configuration', 'ISP Maintenance')",
    )
    description: str = Field(
        ...,
        min_length=10,
        description="Detailed description including actual root cause, reason for downtime, and responsible party.",
    )
    identified_by: str = Field(
        ...,
        min_length=2,
        max_length=100,
        description="The engineer/entity who identified the root cause (e.g. 'ISP Engineer', 'Field Engineer', 'Vendor NOC').",
    )
    customer_confirmed: bool = Field(
        default=False,
        description="Whether the customer confirmed the resolution and root cause (True/False).",
    )

    @field_validator("root_cause_name", "category", "identified_by", mode="before")
    @classmethod
    def _strip_and_validate_non_empty(cls, v: Any) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only")
        return v.strip()

    @field_validator("description", mode="before")
    @classmethod
    def _validate_description_content(cls, v: Any) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("Root cause description is mandatory and cannot be empty")
        cleaned = v.strip()
        if len(cleaned) < 10:
            raise ValueError("Description must be detailed (at least 10 characters)")
        return cleaned


class RootCauseCreate(RootCauseBase):
    """Payload for creating a Root Cause entry."""

    total_downtime_seconds: Optional[int] = Field(
        default=None,
        ge=0,
        description="Optional override for total downtime in seconds. If omitted, calculated as resolved_at - triggered_at.",
    )


class RootCauseUpdate(BaseModel):
    """Payload for updating a Root Cause entry."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    root_cause_name: Optional[str] = Field(default=None, min_length=3, max_length=100)
    category: Optional[str] = Field(default=None, min_length=2, max_length=100)
    description: Optional[str] = Field(default=None, min_length=10)
    identified_by: Optional[str] = Field(default=None, min_length=2, max_length=100)
    customer_confirmed: Optional[bool] = None
    total_downtime_seconds: Optional[int] = Field(default=None, ge=0)


class RootCauseRead(RootCauseBase):
    """Full Root Cause record returned to API clients."""

    model_config = ConfigDict(from_attributes=True, str_strip_whitespace=True, extra="ignore")

    root_cause_id: int = Field(..., ge=100, description="PK -> ROOT_CAUSE.root_cause_id")
    alert_id: int = Field(..., ge=100, description="FK -> ALERT_HISTORY.alert_id")
    identified_at: datetime.datetime
    total_downtime: Optional[datetime.timedelta] = None
    total_downtime_seconds: Optional[int] = None
    total_downtime_human: Optional[str] = None

    @model_validator(mode="after")
    def _compute_downtime_helpers(self) -> "RootCauseRead":
        if self.total_downtime is not None and self.total_downtime_seconds is None:
            object.__setattr__(self, "total_downtime_seconds", int(self.total_downtime.total_seconds()))
        if self.total_downtime_seconds is not None and self.total_downtime_human is None:
            secs = self.total_downtime_seconds
            hours, remainder = divmod(secs, 3600)
            minutes, seconds = divmod(remainder, 60)
            parts = []
            if hours > 0:
                parts.append(f"{hours}h")
            if minutes > 0 or hours > 0:
                parts.append(f"{minutes}m")
            parts.append(f"{seconds}s")
            object.__setattr__(self, "total_downtime_human", " ".join(parts))
        return self