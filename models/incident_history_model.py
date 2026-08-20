"""Pydantic v2 Schemas for Complete Incident Lifecycle History.

Structures the full chronological audit trail and operational timeline for an incident.
"""
from __future__ import annotations

import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from models.attachment_model import AttachmentRead
from models.root_cause_model import RootCauseRead


class AlertSummary(BaseModel):
    """Contextual summary of the alert, sensor, site, and circuit topology."""

    model_config = ConfigDict(from_attributes=True, str_strip_whitespace=True)

    alert_id: int = Field(..., description="Alert ID")
    sensor_id: int = Field(..., description="Sensor ID")
    sensor_name: str = Field(..., description="Sensor Name")
    sensor_type: Optional[str] = Field(default=None, description="Sensor Type")
    site_id: Optional[int] = Field(default=None, description="Site ID")
    site_name: Optional[str] = Field(default=None, description="Site Name")
    primary_ip: Optional[str] = Field(default=None, description="Target host / IP")
    isp_id: Optional[int] = Field(default=None, description="ISP ID")
    isp_name: Optional[str] = Field(default=None, description="ISP Name")
    circuit_id: Optional[str] = Field(default=None, description="Circuit Identifier")
    state_id: int = Field(..., description="Alert State ID")
    state_name: str = Field(..., description="Alert State Name")
    current_status: str = Field(..., description="Overall incident status (e.g. Open, Recovered - Pending Closure, Closed)")
    alert_message: Optional[str] = Field(default=None, description="Original alert description")
    escalation_status: Optional[str] = Field(default=None, description="Current escalation stage")
    triggered_at: datetime.datetime = Field(..., description="When outage was triggered")
    resolved_at: Optional[datetime.datetime] = Field(default=None, description="When outage was resolved")
    total_downtime_human: Optional[str] = Field(default=None, description="Formatted outage duration")


class AlertListItemRead(BaseModel):
    """Item representation for open / active alert lists."""

    model_config = ConfigDict(from_attributes=True, str_strip_whitespace=True)

    alert_id: int
    sensor_id: int
    sensor_name: str
    site_name: Optional[str] = None
    isp_name: Optional[str] = None
    state_name: str
    triggered_at: datetime.datetime
    resolved_at: Optional[datetime.datetime] = None
    is_recovered_pending_closure: bool = False
    escalation_status: Optional[str] = None
    alert_message: Optional[str] = None


class IncidentLifecycleHistoryRead(BaseModel):
    """Complete chronological lifecycle representation of an incident."""

    model_config = ConfigDict(from_attributes=True)

    # 1. Core Summary
    alert_information: AlertSummary = Field(..., description="Complete alert and topology summary")
    triggered_time: datetime.datetime = Field(..., description="Exact incident trigger timestamp")
    resolved_time: Optional[datetime.datetime] = Field(default=None, description="Exact incident recovery timestamp")
    total_downtime: Optional[str] = Field(default=None, description="Human-readable total downtime")
    current_status: str = Field(..., description="Current status: OPEN, PENDING_CLOSURE, or CLOSED")

    # 2. RCA & Supporting Evidence
    root_cause_analysis: Optional[RootCauseRead] = Field(default=None, description="Root Cause Analysis if submitted")
    attachments: List[AttachmentRead] = Field(default_factory=list, description="All uploaded files and documents")

    # 3. Chronological Operational Event Streams
    sensor_logs: List[dict[str, Any]] = Field(default_factory=list, description="Chronological sensor state logs")
    ping_diagnostics: List[dict[str, Any]] = Field(default_factory=list, description="Chronological ping tests and loss telemetry")
    isp_email_threads: List[dict[str, Any]] = Field(default_factory=list, description="Chronological ISP email communications")
    reminder_history: List[dict[str, Any]] = Field(default_factory=list, description="Chronological ISP follow-up reminders")
    escalation_history: List[dict[str, Any]] = Field(default_factory=list, description="Chronological internal and ISP escalations")