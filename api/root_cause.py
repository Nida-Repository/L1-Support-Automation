"""FastAPI Router for Root Cause Analysis (RCA) Management.

Provides endpoints for creating, updating, and viewing Root Cause records.
"""
from __future__ import annotations

import datetime
import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from api.dependencies import get_current_user
from app.crud import (
    AlertHistoryRepository,
    NotFoundError,
    RepositoryError,
    RootCauseRepository,
)
from app.database import get_db
from models.auth_model import UserRead
from models.root_cause_model import (
    RootCauseCreate,
    RootCauseRead,
    RootCauseUpdate,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/alerts", tags=["Root Cause Analysis"])


@router.post(
    "/{alert_id}/root-cause",
    response_model=RootCauseRead,
    status_code=status.HTTP_200_OK,
    summary="Submit or update Root Cause Analysis for an alert",
)
def save_root_cause(
    alert_id: int,
    payload: RootCauseCreate,
    current_user: UserRead = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> RootCauseRead:
    """Submit Root Cause Analysis.

    Note: identified_by represents the actual diagnostician (e.g. 'ISP Engineer', 'Field Engineer')
    and must be specified explicitly in the payload. It is NOT populated with the authenticated username.
    """
    logger.info(
        "User '%s' submitting Root Cause for alert_id=%d [Identified by: %s]",
        current_user.username,
        alert_id,
        payload.identified_by,
    )
    alert_repo = AlertHistoryRepository(db)
    rca_repo = RootCauseRepository(db)

    alert = alert_repo.get(alert_id)
    if not alert:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Alert with ID {alert_id} was not found.",
        )

    # Calculate default downtime if not explicitly provided
    total_downtime = None
    if payload.total_downtime_seconds is not None:
        total_downtime = datetime.timedelta(seconds=payload.total_downtime_seconds)
    elif alert.resolved_at and alert.triggered_at:
        total_downtime = alert.resolved_at - alert.triggered_at
        if total_downtime.total_seconds() < 0:
            total_downtime = datetime.timedelta(seconds=0)

    try:
        saved = rca_repo.upsert_for_alert(
            alert_id=alert_id,
            root_cause_name=payload.root_cause_name,
            category=payload.category,
            identified_by=payload.identified_by,
            description=payload.description,
            customer_confirmed=payload.customer_confirmed,
            total_downtime=total_downtime,
        )
        db.commit()
        logger.info("Successfully upserted Root Cause (ID: %d) for alert_id=%d", saved.root_cause_id, alert_id)
        return RootCauseRead.model_validate(saved)
    except RepositoryError as exc:
        db.rollback()
        logger.error("Repository error saving Root Cause for alert_id=%d: %s", alert_id, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to save Root Cause Analysis.",
        )


@router.get(
    "/{alert_id}/root-cause",
    response_model=RootCauseRead,
    status_code=status.HTTP_200_OK,
    summary="Retrieve Root Cause Analysis for a specific alert",
)
def get_root_cause(
    alert_id: int,
    current_user: UserRead = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> RootCauseRead:
    """Fetch existing Root Cause record for an incident."""
    logger.info("User '%s' fetching Root Cause for alert_id=%d", current_user.username, alert_id)
    rca_repo = RootCauseRepository(db)
    rca = rca_repo.get_by_alert(alert_id)
    if not rca:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No Root Cause Analysis has been submitted for Alert ID {alert_id}.",
        )
    return RootCauseRead.model_validate(rca)