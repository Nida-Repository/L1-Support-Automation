"""Pydantic v2 Schemas for Centralized Incident Closure.

Defines payloads and response formats for finalizing incident closure for both
automatic recovery and manual management override flows.
"""
from __future__ import annotations

import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from models.root_cause_model import RootCauseRead


class ClosureRcaPayload(BaseModel):
    """Mandatory Root Cause Analysis details required for closure."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    root_cause_name: str = Field(
        ...,
        min_length=3,
        max_length=100,
        description="Name of the root cause",
    )
    category: str = Field(
        ...,
        min_length=2,
        max_length=100,
        description="Category (e.g. Hardware, Fiber Cut, ISP Maintenance, Power Issue)",
    )
    description: str = Field(
        ...,
        min_length=10,
        description="Detailed description including actual root cause, reason for downtime, and responsible party (isp, customer, prtg error, field engineer)",
    )
    identified_by: str = Field(
        ...,
        min_length=2,
        max_length=100,
        description="Person/organization who identified the issue (e.g. ISP Engineer, Field Engineer, Vendor Engineer, Support Engineer)",
    )
    customer_confirmed: bool = Field(
        ...,
        description="Customer confirmation status (True/False)",
    )
    total_downtime_seconds: Optional[int] = Field(
        default=None,
        ge=0,
        description="Optional downtime override in seconds. Defaults to resolved_at - triggered_at.",
    )


class CompleteClosureRequest(BaseModel):
    """Payload for completing incident closure after automatic recovery."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    root_cause: ClosureRcaPayload = Field(..., description="Mandatory Root Cause Analysis payload")
    closure_reason: Optional[str] = Field(
        default=None,
        description="Optional notes regarding the resolution/closure",
    )
    is_manual: bool = Field(default=False, description="Manual closure flag")



class ManualClosureRequest(BaseModel):
    """Payload for executing a manual management closure of an incident."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    closure_reason: str = Field(
        ...,
        min_length=5,
        description="Mandatory management reason for manual closure",
    )
    root_cause: ClosureRcaPayload = Field(..., description="Mandatory Root Cause Analysis payload")


class ClosureResponse(BaseModel):
    """Structured response returned after atomic closure transaction completes."""

    model_config = ConfigDict(from_attributes=True)

    status: str = Field(default="success", description="Status code string")
    message: str = Field(..., description="Human-readable outcome description")
    alert_id: int = Field(..., description="ID of the closed alert")
    sensor_id: int = Field(..., description="Sensor associated with the alert")
    resolved_at: datetime.datetime = Field(..., description="Resolution timestamp")
    total_downtime_human: Optional[str] = Field(default=None, description="Formatted total downtime")
    root_cause: RootCauseRead = Field(..., description="Saved Root Cause record")
    attachments_count: int = Field(..., description="Total attachments associated with this incident")
    closed_at: datetime.datetime = Field(..., description="Closure transaction timestamp")