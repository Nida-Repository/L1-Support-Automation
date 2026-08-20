"""FastAPI Router for Centralized Incident Closure.

Provides the single orchestration endpoint for finalizing incident closure
following automatic recovery or manual management decisions.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from api.dependencies import get_current_user
from app.database import get_db
from models.auth_model import UserRead
from models.closure_model import (
    ClosureResponse,
    CompleteClosureRequest,
    ManualClosureRequest,
)
from services.closure_service import ClosureService, ClosureValidationError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/alerts", tags=["Incident Closure"])


@router.post(
    "/{alert_id}/complete-closure",
    response_model=ClosureResponse,
    status_code=status.HTTP_200_OK,
    summary="Centralized endpoint to validate, finalize, and commit incident closure",
)
def complete_incident_closure(
    alert_id: int,
    payload: CompleteClosureRequest,
    current_user: UserRead = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> ClosureResponse:
    """Primary incident closure orchestration endpoint.

    Validates all business rules (mandatory RCA fields, supporting attachment presence),
    updates root cause, closes remaining open sensor logs, updates resolution metadata,
    and commits the final changes atomically in a single transaction.
    """
    logger.info(
        "User '%s' executing complete closure for alert_id=%d [Root Cause: %s]",
        current_user.username,
        alert_id,
        payload.root_cause.root_cause_name,
    )
    try:
        return ClosureService.execute_closure_transaction(
            db,
            alert_id=alert_id,
            rca_payload=payload.root_cause,
            authenticated_user=current_user.username,
            closure_reason=payload.closure_reason,
            is_manual_closure=payload.is_manual,
        )
    except ClosureValidationError as exc:
        logger.warning("Validation failure closing alert_id=%d: %s", alert_id, exc)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "CLOSURE_VALIDATION_ERROR",
                "field": exc.field,
                "reason": exc.reason,
                "suggestion": exc.suggestion,
            },
        )
    except Exception as exc:
        logger.error("Unexpected error finalizing closure for alert_id=%d: %s", alert_id, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to complete incident closure transaction.",
        )


@router.post(
    "/{alert_id}/manual-close",
    response_model=ClosureResponse,
    status_code=status.HTTP_200_OK,
    summary="Manually close an incident with management reason and RCA",
)
def manual_incident_closure(
    alert_id: int,
    payload: ManualClosureRequest,
    current_user: UserRead = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> ClosureResponse:
    """Execute a manual incident closure.

    Reuses the exact same centralized closure service and validation engine as automatic recovery.
    """
    logger.info(
        "User '%s' executing manual closure for alert_id=%d [Reason: %s]",
        current_user.username,
        alert_id,
        payload.closure_reason,
    )
    try:
        return ClosureService.execute_closure_transaction(
            db,
            alert_id=alert_id,
            rca_payload=payload.root_cause,
            authenticated_user=current_user.username,
            closure_reason=payload.closure_reason,
            is_manual_closure=True,
        )
    except ClosureValidationError as exc:
        logger.warning("Manual closure validation error for alert_id=%d: %s", alert_id, exc)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "CLOSURE_VALIDATION_ERROR",
                "field": exc.field,
                "reason": exc.reason,
                "suggestion": exc.suggestion,
            },
        )
    except Exception as exc:
        logger.error("Error during manual closure for alert_id=%d: %s", alert_id, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to execute manual incident closure transaction.",
        )