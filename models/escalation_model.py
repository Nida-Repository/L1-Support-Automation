
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    field_validator,
)


class EscalatedTo(str, Enum):
    """Mirrors chk_escalated_to CHECK constraint."""
    ISP = "ISP"
    SUPPORT_TEAM = "SUPPORT TEAM"


# ---------------------------------------------------------------------------
# Shared base
# ---------------------------------------------------------------------------

class EscalationRecordBase(BaseModel):
    """Fields common to create/update/read variants."""

    model_config = ConfigDict(
        from_attributes=True,     # allows ORM -> model (SQLAlchemy row objects)
        str_strip_whitespace=True,
        extra="forbid",
    )

    alert_id: int = Field(..., gt=0, description="FK -> ALERT_HISTORY.alert_id")
    escalated_to: EscalatedTo
    recipient_email: EmailStr
    cc_emails: Optional[list[EmailStr]] = Field(
        default=None,
        description="List of CC'd email addresses; each entry is validated as a proper email.",
    )
    email_subject: Optional[str] = None
    email_body: Optional[str] = None
    response_received: bool = False
    response_notes: Optional[str] = None

    @field_validator("cc_emails")
    @classmethod
    def dedupe_cc_emails(cls, v: Optional[list[EmailStr]]) -> Optional[list[EmailStr]]:
        """Remove duplicates while preserving order; empty list -> None."""
        if v is None:
            return v
        seen: set[str] = set()
        deduped: list[EmailStr] = []
        for email in v:
            key = email.lower()
            if key not in seen:
                seen.add(key)
                deduped.append(email)
        return deduped or None


# ---------------------------------------------------------------------------
# Create (inbound, e.g. POST /escalations)
# ---------------------------------------------------------------------------

class EscalationRecordCreate(EscalationRecordBase):
    """Payload for creating a new escalation record.
    """
    pass


# ---------------------------------------------------------------------------
# Update (inbound, e.g. PATCH /escalations/{id})
# ---------------------------------------------------------------------------

class EscalationRecordUpdate(BaseModel):
    """Partial update — typically used to record a response to an escalation."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    response_received: Optional[bool] = None
    response_notes: Optional[str] = None
    cc_emails: Optional[list[EmailStr]] = None

    @field_validator("cc_emails")
    @classmethod
    def dedupe_cc_emails(cls, v: Optional[list[EmailStr]]) -> Optional[list[EmailStr]]:
        if v is None:
            return v
        seen: set[str] = set()
        deduped: list[EmailStr] = []
        for email in v:
            key = email.lower()
            if key not in seen:
                seen.add(key)
                deduped.append(email)
        return deduped or None


# ---------------------------------------------------------------------------
# Read (outbound, e.g. GET /escalations/{id}) — ORM-backed
# ---------------------------------------------------------------------------

class EscalationRecordRead(EscalationRecordBase):
    """Full record as read from the DB, including server-generated fields."""

    escalation_id: int
    sent_at: datetime