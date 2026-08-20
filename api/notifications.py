"""FastAPI Router for Pending Closure Notifications.

Surfaces all sensors that have recovered but require support engineer Root Cause Analysis.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from api.dependencies import get_current_user
from app.crud import RepositoryError
from app.database import get_db
from models.auth_model import UserRead
from models.notification_model import PendingClosuresResponse
from services.incident_service import IncidentService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/notifications", tags=["Notifications"])


@router.get(
    "/pending-closures",
    response_model=PendingClosuresResponse,
    status_code=status.HTTP_200_OK,
    summary="List all sensors that have recovered but require Root Cause Analysis closure",
)
def get_pending_closure_notifications(
    current_user: UserRead = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> PendingClosuresResponse:
    """Retrieve all incidents where sensor status is recovered/UP, but incident closure is pending.

    The notification message format is:
    "Sensor '<sensor_name>' recovered successfully. Incident closure information is pending."
    """
    logger.info("User '%s' querying pending closure notifications", current_user.username)
    try:
        return IncidentService.get_pending_closures(db)
    except RepositoryError as exc:
        logger.error("Error retrieving pending closure notifications: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve pending closure notifications.",
        )