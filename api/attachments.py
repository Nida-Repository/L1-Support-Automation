"""FastAPI Router for Attachment Upload and Retrieval.

Provides secure multipart file uploads to MinIO Object Storage and stores
metadata strictly in the PostgreSQL ATTACHMENTS table.
"""
from __future__ import annotations

import datetime
import logging
import os
from typing import List

from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Query,
    UploadFile,
    status,
)
from fastapi.responses import StreamingResponse 
from sqlalchemy.orm import Session

from api.dependencies import get_current_user
from app.crud import (
    AlertHistoryRepository,
    AttachmentRepository,
    RepositoryError,
)
from app.database import get_db
from config.settings import settings
from models.attachment_model import AttachmentPage, AttachmentRead
from models.auth_model import UserRead
from services.minio_service import MinioServiceError, minio_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/alerts", tags=["Attachments"])


@router.post(
    "/{alert_id}/attachments",
    response_model=AttachmentRead,
    status_code=status.HTTP_201_CREATED,
    summary="Upload an RCA report or supporting document to MinIO",
)
async def upload_attachment(
    alert_id: int,
    file: UploadFile = File(..., description="File to upload (max 25MB)"),
    current_user: UserRead = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> AttachmentRead:
    """Upload a file to MinIO object storage and persist metadata in PostgreSQL.

    - **uploaded_by**: Automatically populated from authenticated engineer JWT.
    - **file_name**: Sanitized to prevent directory traversal.
    - **file_size**: Validated against configured size limits.
    """
    logger.info(
        "User '%s' uploading attachment '%s' for alert_id=%d",
        current_user.username,
        file.filename,
        alert_id,
    )

    alert_repo = AlertHistoryRepository(db)
    att_repo = AttachmentRepository(db)

    alert = alert_repo.get(alert_id)
    if not alert:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Alert with ID {alert_id} does not exist.",
        )

    # 1. Filename & Extension Sanitization
    filename = file.filename or "unnamed_attachment.bin"
    ext = os.path.splitext(filename)[1].lower()
    if settings.allowed_attachment_extensions and ext not in settings.allowed_attachment_extensions:
        allowed = ", ".join(settings.allowed_attachment_extensions)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File extension '{ext}' is not allowed. Allowed types: {allowed}",
        )

    # 2. Read File Contents
    file_bytes = await file.read()
    file_size = len(file_bytes)

    if file_size == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file is empty (0 bytes).",
        )

    if file_size > settings.max_attachment_size_bytes:
        max_mb = settings.max_attachment_size_bytes / (1024 * 1024)
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File size exceeds maximum allowed limit of {max_mb:.1f} MB.",
        )

    # 3. Upload to MinIO
    try:
        upload_meta = minio_service.upload_attachment(
            alert_id=alert_id,
            thread_id=0,  # Manual / RCA upload (thread_id NULL in DB)
            file_name=filename,
            file_data=file_bytes,
            content_type=file.content_type or "application/octet-stream",
        )
    except MinioServiceError as exc:
        logger.error("MinIO upload failed for alert_id=%d: %s", alert_id, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to store attachment in object storage.",
        )

    # 4. Save Metadata in PostgreSQL
    try:
        att_record = att_repo.create(
            alert_id=alert_id,
            thread_id=None,
            file_name=upload_meta["file_name"],
            file_type=upload_meta["file_type"],
            file_size=upload_meta["file_size"],
            bucket_name=upload_meta["bucket_name"],
            object_key=upload_meta["object_key"],
            etag=upload_meta.get("etag"),
            uploaded_by=current_user.username,
            uploaded_at=datetime.datetime.now(datetime.timezone.utc),
        )
        db.commit()
        logger.info(
            "Successfully saved attachment metadata (ID: %d, Key: %s) for alert_id=%d by '%s'",
            att_record.attachment_id,
            att_record.object_key,
            alert_id,
            current_user.username,
        )
        return AttachmentRead.model_validate(att_record)
    except RepositoryError as exc:
        db.rollback()
        logger.error("Failed to save attachment metadata in DB: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to save attachment record in database.",
        )


@router.get(
    "/{alert_id}/attachments",
    response_model=AttachmentPage,
    status_code=status.HTTP_200_OK,
    summary="List all attachments associated with an alert",
)
def list_attachments(
    alert_id: int,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    current_user: UserRead = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> AttachmentPage:
    """Retrieve all attachments and documents uploaded for an incident."""
    logger.info("User '%s' listing attachments for alert_id=%d", current_user.username, alert_id)
    att_repo = AttachmentRepository(db)
    try:
        page = att_repo.list_for_alert(alert_id, limit=limit, offset=offset)
        return AttachmentPage(
            items=[AttachmentRead.model_validate(item) for item in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
            has_more=page.has_more,
        )
    except RepositoryError as exc:
        logger.error("Error retrieving attachments for alert_id=%d: %s", alert_id, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to query attachments from database.",
        )

@router.get(
    "/{alert_id}/attachments/{attachment_id}/download",
    summary="Stream and download an attachment file directly from MinIO",
    response_class=StreamingResponse,
    responses={
        200: {"description": "File stream returned successfully"},
        404: {"description": "Alert or attachment record not found"},
        502: {"description": "MinIO object storage is unreachable or object missing"},
    },
)
def download_attachment(
    alert_id: int,
    attachment_id: int,
    current_user: UserRead = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> StreamingResponse:
    """Stream the actual attachment file bytes from MinIO object storage.

    - Validates that the attachment belongs to the given alert.
    - Streams the file in chunks — memory-efficient even for large files.
    - Sets Content-Disposition header to trigger browser file download.
    """
    logger.info(
        "User '%s' downloading attachment_id=%d for alert_id=%d",
        current_user.username,
        attachment_id,
        alert_id,
    )

    att_repo = AttachmentRepository(db)

    # 1. Fetch the attachment record from DB
    attachment = att_repo.get(attachment_id)
    if not attachment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Attachment with ID {attachment_id} does not exist.",
        )

    # 2. Confirm it belongs to the requested alert
    if attachment.alert_id != alert_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Attachment {attachment_id} does not belong to alert {alert_id}.",
        )

    # 3. Open a streaming connection to MinIO
    try:
        stream, content_type, content_length = minio_service.get_object_stream(
            object_key=attachment.object_key,
            bucket_name=attachment.bucket_name,
        )
    except MinioServiceError as exc:
        logger.error(
            "MinIO stream failed for attachment_id=%d (key=%s): %s",
            attachment_id,
            attachment.object_key,
            exc,
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to retrieve file from object storage.",
        )

    # 4. Build response headers
    safe_filename = attachment.file_name.replace('"', '_')
    headers = {
        "Content-Disposition": f'attachment; filename="{safe_filename}"',
        "Content-Length": str(content_length) if content_length else "",
        "Cache-Control": "no-store",
    }

    # 5. Return chunked streaming response (stream is closed after iteration)
    def _iter_chunks(response_stream, chunk_size: int = 65536):
        try:
            for chunk in response_stream.stream(chunk_size):
                yield chunk
        finally:
            response_stream.close()

    return StreamingResponse(
        _iter_chunks(stream),
        media_type=content_type,
        headers=headers,
    )    