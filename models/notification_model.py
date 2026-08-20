"""Pydantic v2 Schemas for Pending Closure Notifications.

Provides schemas for surfacing recovered incidents that are pending support engineer
Root Cause Analysis and final closure.
"""
from __future__ import annotations

import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class PendingClosureNotification(BaseModel):
    """Notification item for a sensor that has recovered but is pending closure info."""

    model_config = ConfigDict(from_attributes=True, str_strip_whitespace=True)

    alert_id: int = Field(..., description="Alert ID")
    sensor_id: int = Field(..., description="Sensor ID")
    sensor_name: str = Field(..., description="Sensor Name")
    site_name: Optional[str] = Field(default=None, description="Site location name")
    isp_name: Optional[str] = Field(default=None, description="ISP provider name")
    circuit_id: Optional[str] = Field(default=None, description="Circuit Identifier")
    triggered_at: datetime.datetime = Field(..., description="When outage was detected")
    recovered_at: datetime.datetime = Field(..., description="When sensor recovered (resolved_at)")
    downtime_seconds: Optional[int] = Field(default=None, description="Calculated outage duration in seconds")
    downtime_human: Optional[str] = Field(default=None, description="Human readable outage duration")
    notification_message: str = Field(
        ...,
        description="Formatted message: Sensor '<sensor_name>' recovered successfully. Incident closure information is pending.",
    )


class PendingClosuresResponse(BaseModel):
    """Collection envelope for pending closure notifications."""

    count: int = Field(..., description="Total count of pending closures")
    items: List[PendingClosureNotification] = Field(default_factory=list, description="List of notification items")