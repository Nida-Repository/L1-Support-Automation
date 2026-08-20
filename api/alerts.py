"""FastAPI Router for Alert Inquiries and Incident History.

Provides endpoints for listing open alerts, querying single alert topology,
and retrieving the complete chronological lifecycle history of an incident.
"""
from __future__ import annotations

import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from api.dependencies import get_current_user
from app.crud import NotFoundError, RepositoryError
from app.database import get_db
from models.auth_model import UserRead
from models.incident_history_model import (
    AlertListItemRead,
    AlertSummary,
    IncidentLifecycleHistoryRead,
)
from services.incident_service import IncidentService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/alerts", tags=["Incidents & Alerts"])


@router.get(
    "/open",
    response_model=List[AlertListItemRead],
    status_code=status.HTTP_200_OK,
    summary="List active open incidents and recovered alerts pending closure",
)
def get_open_alerts(
    limit: int = Query(default=50, ge=1, le=500, description="Max records to return"),
    offset: int = Query(default=0, ge=0, description="Offset for pagination"),
    current_user: UserRead = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> List[AlertListItemRead]:
    """Retrieve all unresolved alerts or recovered alerts awaiting Root Cause Analysis and closure."""
    logger.info("User '%s' requested open alerts list (limit=%d, offset=%d)", current_user.username, limit, offset)
    try:
        return IncidentService.get_open_alerts(db, limit=limit, offset=offset)
    except RepositoryError as exc:
        logger.error("Error fetching open alerts: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve active alerts from database.",
        )


@router.get(
    "/{alert_id}",
    response_model=AlertSummary,
    status_code=status.HTTP_200_OK,
    summary="Get topology and status summary for a specific alert",
)
def get_alert_detail(
    alert_id: int,
    current_user: UserRead = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> AlertSummary:
    """Retrieve structured alert details including sensor, site, and ISP topology."""
    logger.info("User '%s' requested alert detail for alert_id=%d", current_user.username, alert_id)
    try:
        return IncidentService.get_alert_summary(db, alert_id)
    except NotFoundError:
        logger.warning("Alert ID %d not found", alert_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Alert with ID {alert_id} was not found.",
        )
    except RepositoryError as exc:
        logger.error("Error retrieving alert %d: %s", alert_id, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve alert information.",
        )


@router.get(
    "/{alert_id}/history",
    response_model=IncidentLifecycleHistoryRead,
    status_code=status.HTTP_200_OK,
    summary="Retrieve complete chronological lifecycle history for an incident",
)
def get_incident_history(
    alert_id: int,
    current_user: UserRead = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> IncidentLifecycleHistoryRead:
    """Compile the entire operational lifecycle and audit trail for an incident in chronological order:

    - **Alert Information**: Topology, triggered time, resolved time, total downtime, status.
    - **Root Cause Analysis**: Saved RCA report and confirmation.
    - **Attachments**: Downloadable file metadata.
    - **Sensor Logs**: State transition timeline.
    - **Ping Diagnostics**: Automated telemetry batches.
    - **ISP Email Threads**: Chronological email exchanges and attached files.
    - **Reminder History**: Automated follow-up records.
    - **Escalation History**: Support and ISP escalation audit trail.
    """
    logger.info("User '%s' requested complete incident lifecycle history for alert_id=%d", current_user.username, alert_id)
    try:
        return IncidentService.get_incident_history(db, alert_id)
    except NotFoundError:
        logger.warning("Alert ID %d not found for history query", alert_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Alert with ID {alert_id} was not found.",
        )
    except RepositoryError as exc:
        logger.error("Error retrieving history for alert %d: %s", alert_id, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve incident lifecycle history.",
        )