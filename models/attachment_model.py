"""Pydantic v2 Schemas for File Attachment Metadata.

Handles serialization and validation for incident and RCA supporting document metadata.
"""
from __future__ import annotations

import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class AttachmentBase(BaseModel):
    """Base fields for attachment metadata."""

    model_config = ConfigDict(
        from_attributes=True,
        str_strip_whitespace=True,
        extra="ignore",
    )

    file_name: str = Field(..., min_length=1, max_length=100, description="Original or sanitized filename")
    file_type: str = Field(..., max_length=100, description="MIME type of the attachment")
    file_size: int = Field(..., gt=0, description="File size in bytes")
    bucket_name: str = Field(..., max_length=100, description="MinIO bucket name")
    object_key: str = Field(..., max_length=500, description="Unique storage path/key inside MinIO bucket")
    etag: Optional[str] = Field(default=None, max_length=100, description="MinIO S3 ETag hash")


class AttachmentCreate(AttachmentBase):
    """Internal model for saving attachment metadata to database."""

    alert_id: int = Field(..., ge=100, description="FK -> ALERT_HISTORY.alert_id")
    thread_id: Optional[int] = Field(default=None, description="Optional FK -> ISP_EMAIL_THREADS.thread_id")
    uploaded_by: str = Field(..., max_length=100, description="Username of the uploader (from JWT auth)")


class AttachmentRead(AttachmentBase):
    """Response model for file attachment metadata returned to API clients."""

    attachment_id: int = Field(..., ge=100, description="PK -> ATTACHMENTS.attachment_id")
    alert_id: int = Field(..., ge=100, description="FK -> ALERT_HISTORY.alert_id")
    thread_id: Optional[int] = None
    uploaded_by: str = Field(..., description="Authenticated username who uploaded the attachment")
    uploaded_at: datetime.datetime


class AttachmentPage(BaseModel):
    """Paginated collection of attachment records."""

    items: List[AttachmentRead]
    total: int
    limit: int
    offset: int
    has_more: bool