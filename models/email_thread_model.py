"""Pydantic v2 Schemas for Email Threading and Attachment Processing.

Provides request/response validation, serialization, and classification mapping
for FastAPI endpoints and asynchronous task queues.
"""
from __future__ import annotations

import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.models import EmailClassificationType, EmailDirectionType

# Canonical human-readable to enum mapping
CLASSIFICATION_ALIAS_MAP: dict[str, EmailClassificationType] = {
    "link up and stable": EmailClassificationType.LINK_STABLE,
    "link stable": EmailClassificationType.LINK_STABLE,
    "planned maintenance": EmailClassificationType.MAINTENANCE,
    "maintenance": EmailClassificationType.MAINTENANCE,
    "request for additional information": EmailClassificationType.NEED_PING,
    "need ping": EmailClassificationType.NEED_PING,
    "need traceroute": EmailClassificationType.NEED_TRACEROUTE,
    "technical issue identified": EmailClassificationType.TECHNICAL_ISSUE,
    "technical issue": EmailClassificationType.TECHNICAL_ISSUE,
    "power issue detected by isp": EmailClassificationType.POWER_ISSUE,
    "power issue": EmailClassificationType.POWER_ISSUE,
    "no issue found on isp side": EmailClassificationType.NO_ISP_ISSUE,
    "no isp issue": EmailClassificationType.NO_ISP_ISSUE,
    "unknown / manual investigation": EmailClassificationType.UNKNOWN,
    "unknown": EmailClassificationType.UNKNOWN,
}


class AttachmentRead(BaseModel):
    """Schema for serializing email attachment metadata."""

    model_config = ConfigDict(from_attributes=True, extra="ignore")

    attachment_id: int
    alert_id: int
    thread_id: Optional[int] = None
    file_name: str
    file_type: str
    file_size: int
    bucket_name: str
    object_key: str
    etag: Optional[str] = None
    uploaded_by: str = "SYSTEM"
    uploaded_at: datetime.datetime


class EmailClassificationUpdate(BaseModel):
    """Request schema for Support Engineers manually updating email classification."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, use_enum_values=False)

    classification: EmailClassificationType = Field(
        ...,
        description=(
            "Classification label (e.g. 'Link Up and Stable', 'Planned Maintenance', "
            "'Request for Additional Information', 'Technical Issue Identified', "
            "'Power Issue Detected by ISP', 'No Issue Found on ISP Side', 'Unknown / Manual Investigation')"
        ),
    )

    @field_validator("classification", mode="before")
    @classmethod
    def validate_and_normalize_classification(cls, v: Any) -> EmailClassificationType:
        if isinstance(v, EmailClassificationType):
            return v
        if isinstance(v, str):
            key = v.strip().lower()
            if key in CLASSIFICATION_ALIAS_MAP:
                return CLASSIFICATION_ALIAS_MAP[key]
            # Check direct enum value match
            for enum_item in EmailClassificationType:
                if enum_item.value.lower() == key:
                    return enum_item
        raise ValueError(
            f"Invalid classification '{v}'. Allowed values: {list(CLASSIFICATION_ALIAS_MAP.keys())}"
        )


class EmailThreadRead(BaseModel):
    """Schema for reading complete email thread details."""

    model_config = ConfigDict(from_attributes=True, extra="ignore")

    thread_id: int
    alert_id: int
    message_id: str
    in_reply_to: Optional[str] = None
    email_references: Optional[List[str]] = None
    subject: Optional[str] = None
    sender: str
    receiver: str
    cc: Optional[List[str]] = None
    direction: EmailDirectionType
    sent_received_at: datetime.datetime
    body: Optional[str] = None
    classification_type: EmailClassificationType
    created_at: datetime.datetime
    attachments: List[AttachmentRead] = Field(default_factory=list)


class EmailThreadPage(BaseModel):
    """Paginated envelope for email thread collections."""

    items: List[EmailThreadRead]
    total: int
    limit: int
    offset: int
    has_more: bool


class IncomingEmailPayload(BaseModel):
    """Schema for validating raw inbound email event tasks published to RabbitMQ."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    alert_id: int
    thread_id: Optional[int] = None
    message_id: str
    in_reply_to: Optional[str] = None
    references: List[str] = Field(default_factory=list)
    sender: str
    receiver: str
    cc: List[str] = Field(default_factory=list)
    subject: Optional[str] = None
    body: Optional[str] = None
    received_at: Optional[str] = None
    attachment_metadata: List[dict[str, Any]] = Field(default_factory=list)
