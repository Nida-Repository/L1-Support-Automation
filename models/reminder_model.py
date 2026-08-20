"""Pydantic v2 Schemas for Reminder History.

Provides response schemas for email reminder logs and follow-ups.
"""
from __future__ import annotations

import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from app.models import ReminderStatusType


class ReminderHistoryRead(BaseModel):
    """Schema for reading a reminder history entry."""

    model_config = ConfigDict(from_attributes=True, extra="ignore")

    reminder_id: int = Field(..., description="PK -> REMINDER_HISTORY.reminder_id")
    alert_id: int = Field(..., description="FK -> ALERT_HISTORY.alert_id")
    reminder_number: int = Field(..., description="Sequential reminder number")
    sent_at: datetime.datetime = Field(..., description="When reminder email was sent")
    email_id: Optional[int] = Field(default=None, description="FK -> ISP_CONTACT_EMAILS.email_id")
    response_received: bool = Field(default=False, description="Whether ISP responded to this reminder")
    response_received_at: Optional[datetime.datetime] = Field(default=None, description="When response was received")
    status: ReminderStatusType = Field(default=ReminderStatusType.SENT, description="Delivery status")


class ReminderHistoryPage(BaseModel):
    """Paginated envelope for reminder history items."""

    items: List[ReminderHistoryRead]
    total: int
    limit: int
    offset: int
    has_more: bool