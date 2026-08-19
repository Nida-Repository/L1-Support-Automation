"""FastAPI Router for Support Engineer Email Thread Management.

Provides endpoints to:
1. View complete chronological email thread for an alert with attachment metadata.
2. View single email message details.
3. Manually update email classification (e.g. Link Up and Stable, Planned Maintenance, etc.).
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.crud import (
    ConstraintViolationError,
    DuplicateError,
    IspEmailThreadRepository,
    NotFoundError,
    RepositoryError,
)
from app.database import get_db
from app.models import EmailClassificationType, EmailDirectionType
from models.email_thread_model import (
    EmailClassificationUpdate,
    EmailThreadPage,
    EmailThreadRead,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/emails", tags=["Email Threads"])


@router.get(
    "/threads/{alert_id}",
    response_model=EmailThreadPage,
    status_code=status.HTTP_200_OK,
    summary="Get complete email thread history for an alert",
)
def get_alert_email_threads(
    alert_id: int,
    limit: int = Query(default=50, ge=1, le=500, description="Items per page"),
    offset: int = Query(default=0, ge=0, description="Page offset"),
    direction: Optional[EmailDirectionType] = Query(default=None, description="Filter by direction (Incoming/Outgoing)"),
    classification: Optional[EmailClassificationType] = Query(default=None, description="Filter by classification"),
    db: Session = Depends(get_db),
) -> EmailThreadPage:
    """Retrieve all chronological emails and attachments associated with an alert."""
    logger.info("Fetching email threads for alert_id=%d (limit=%d, offset=%d)", alert_id, limit, offset)
    repo = IspEmailThreadRepository(db)
    try:
        page = repo.list_for_alert(
            alert_id=alert_id,
            direction=direction,
            classification_type=classification,
            limit=limit,
            offset=offset,
        )
        return EmailThreadPage(
            items=[EmailThreadRead.model_validate(item) for item in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
            has_more=page.has_more,
        )
    except RepositoryError as exc:
        logger.error("Repository error listing email threads for alert_id=%d: %s", alert_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve email threads from database",
        )


@router.get(
    "/threads/detail/{thread_id}",
    response_model=EmailThreadRead,
    status_code=status.HTTP_200_OK,
    summary="Get details of a single email thread record",
)
def get_email_thread_detail(
    thread_id: int,
    db: Session = Depends(get_db),
) -> EmailThreadRead:
    """Fetch an individual email thread entry with its attachments."""
    logger.debug("Fetching email thread detail for thread_id=%d", thread_id)
    repo = IspEmailThreadRepository(db)
    thread = repo.get_with_attachments(thread_id)
    if not thread:
        logger.warning("Email thread with thread_id=%d not found", thread_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Email thread with id {thread_id} not found",
        )
    return EmailThreadRead.model_validate(thread)


@router.patch(
    "/threads/{thread_id}/classification",
    response_model=EmailThreadRead,
    status_code=status.HTTP_200_OK,
    summary="Manually update email classification",
)
def update_email_thread_classification(
    thread_id: int,
    payload: EmailClassificationUpdate,
    db: Session = Depends(get_db),
) -> EmailThreadRead:
    """Allow Support Engineers to update email classification manually via API.

    Supported classifications include:
    - Link Up and Stable
    - Planned Maintenance
    - Request for Additional Information
    - Technical Issue Identified
    - Power Issue Detected by ISP
    - No Issue Found on ISP Side
    - Unknown / Manual Investigation
    """
    classification_val = (
        payload.classification.value
        if isinstance(payload.classification, EmailClassificationType)
        else str(payload.classification)
    )
    logger.info(
        "Manual classification update request for thread_id=%d -> %s",
        thread_id,
        classification_val,
    )
    repo = IspEmailThreadRepository(db)
    try:
        updated = repo.update_classification(thread_id, payload.classification)
        db.commit()
        logger.info("Successfully updated classification for thread_id=%d to %s", thread_id, classification_val)
        return EmailThreadRead.model_validate(updated)
    except NotFoundError:
        logger.warning("Thread id %d not found for classification update", thread_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Email thread with id {thread_id} not found",
        )
    except ConstraintViolationError as exc:
        logger.error("Constraint violation updating classification for thread_id=%d: %s", thread_id, exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid classification value or database constraint violation",
        )
    except RepositoryError as exc:
        logger.error("Repository error updating classification for thread_id=%d: %s", thread_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update email thread classification",
        )
