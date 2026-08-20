"""Centralized Incident Closure Service.

Provides a unified, atomic orchestration service for incident finalization across both
automatic recovery completion and manual management closure workflows.
"""
from __future__ import annotations

import datetime
import logging
from typing import Any, Dict, Optional, Sequence

from sqlalchemy.orm import Session

from app.crud import (
    AlertHistoryRepository,
    AttachmentRepository,
    RepositoryError,
    RootCauseRepository,
    SensorLogRepository,
    SensorRepository,
)
from app.models import (
    AlertHistory,
    LogLevelType,
    LogStatusType,
    RootCause,
)
from models.closure_model import ClosureRcaPayload, ClosureResponse
from models.root_cause_model import RootCauseRead
from processors.base import json_safe_dict

logger = logging.getLogger(__name__)


class ClosureValidationError(Exception):
    """Raised when business or state validation fails prior to incident closure."""

    def __init__(self, field: str, reason: str, suggestion: Optional[str] = None):
        self.field = field
        self.reason = reason
        self.suggestion = suggestion
        message = f"Closure validation failed on '{field}': {reason}"
        if suggestion:
            message += f" (Suggestion: {suggestion})"
        super().__init__(message)


class ClosureService:
    """Atomic incident closure orchestrator following SOLID principles."""

    @classmethod
    def validate_closure_requirements(
        cls,
        *,
        alert: AlertHistory,
        rca: ClosureRcaPayload,
        attachments: Sequence[Any],
    ) -> None:
        """Enforce strict business rules required before an incident can be closed.

        Validation rules:
        1. Alert must exist.
        2. At least one supporting attachment must be present.
        3. Root Cause Name must be non-empty and descriptive.
        4. Category must be non-empty.
        5. Description must be detailed and cover root cause, downtime reason, and responsible party.
        6. Identified By must be provided manually.
        7. Customer Confirmed must be an explicit boolean.
        """
        # Rule 1: Alert existence is verified before calling this method

        # Rule 2: Attachment requirement
        if not attachments or len(attachments) == 0:
            raise ClosureValidationError(
                field="attachments",
                reason="At least one supporting attachment (e.g. RCA report, screenshot, pcap) is mandatory to close an incident.",
                suggestion="Upload a supporting attachment via POST /alerts/{alert_id}/attachments before closing.",
            )

        # Rule 3: Root Cause Name
        if not rca.root_cause_name or len(rca.root_cause_name.strip()) < 3:
            raise ClosureValidationError(
                field="root_cause.root_cause_name",
                reason="Root cause name must be at least 3 characters long.",
                suggestion="Provide a clear title such as 'Core Uplink Fiber Cut' or 'Power Failure at Datacenter'.",
            )

        # Rule 4: Category
        if not rca.category or len(rca.category.strip()) < 2:
            raise ClosureValidationError(
                field="root_cause.category",
                reason="Root cause category is required.",
                suggestion="Select or provide a category such as 'Hardware', 'Fiber Cut', 'ISP Maintenance', or 'Power'.",
            )

        # Rule 5: Description
        desc = (rca.description or "").strip()
        if len(desc) < 10:
            raise ClosureValidationError(
                field="root_cause.description",
                reason="Root cause description is required and must contain full operational details (at least 10 chars).",
                suggestion="Include the actual root cause, reason for downtime, and responsible party (ISP, Customer, Vendor, Field Engineer).",
            )

        # Rule 6: Identified By
        if not rca.identified_by or len(rca.identified_by.strip()) < 2:
            raise ClosureValidationError(
                field="root_cause.identified_by",
                reason="Identified By field is mandatory and must be entered manually.",
                suggestion="Specify the person/entity who diagnosed the issue (e.g. 'ISP Senior NOC Engineer', 'Field Engineer').",
            )

        # Rule 7: Customer Confirmed
        if rca.customer_confirmed is None:
            raise ClosureValidationError(
                field="root_cause.customer_confirmed",
                reason="Customer confirmation selection (True/False) is mandatory.",
                suggestion="Set customer_confirmed to true if customer confirmed link stability, otherwise false.",
            )

    @classmethod
    def execute_closure_transaction(
        cls,
        db: Session,
        *,
        alert_id: int,
        rca_payload: ClosureRcaPayload,
        authenticated_user: str,
        closure_reason: Optional[str] = None,
        is_manual_closure: bool = False,
    ) -> ClosureResponse:
        """Execute complete incident closure in a single atomic database transaction.

        Orchestrates:
        1. Verification of alert existence and attachment evidence.
        2. Business rule validation.
        3. Downtime duration calculation or manual override application.
        4. Upsert of ROOT_CAUSE record.
        5. Closing open SENSOR_LOGS entries if any remain.
        6. Updating ALERT_HISTORY.resolved_at if manual closure or unpopulated.
        7. Creating a CLOSED INFO SENSOR_LOG audit entry.
        8. Atomic commit with total rollback on exception.
        """
        logger.info(
            "Initiating atomic incident closure for alert_id=%d [User: %s | Manual: %s]",
            alert_id,
            authenticated_user,
            is_manual_closure,
        )

        alert_repo = AlertHistoryRepository(db)
        rca_repo = RootCauseRepository(db)
        log_repo = SensorLogRepository(db)
        attachment_repo = AttachmentRepository(db)
        sensor_repo = SensorRepository(db)

        # 1. Fetch Alert
        alert = alert_repo.get(alert_id)
        if not alert:
            logger.warning("Closure rejected: Alert ID %d does not exist.", alert_id)
            raise ClosureValidationError(
                field="alert_id",
                reason=f"Alert with ID {alert_id} not found.",
                suggestion="Verify the alert_id and ensure it exists in the system.",
            )

        # 2. Fetch existing attachments for this alert
        attachments_page = attachment_repo.list_for_alert(alert_id, limit=500)
        existing_attachments = attachments_page.items

        # 3. Validate all business rules
        cls.validate_closure_requirements(
            alert=alert,
            rca=rca_payload,
            attachments=existing_attachments,
        )

        closure_now = datetime.datetime.now(datetime.timezone.utc)

        try:
            # 4. Handle resolution timestamp for manual closure or unpopulated resolved_at
            if is_manual_closure or alert.resolved_at is None:
                alert_repo.resolve(alert_id, resolved_at=closure_now)
                logger.info("Updated alert_id=%d resolved_at to %s", alert_id, closure_now)
                resolved_timestamp = closure_now
            else:
                resolved_timestamp = alert.resolved_at

            # Ensure resolved_timestamp has timezone info
            if resolved_timestamp.tzinfo is None:
                resolved_timestamp = resolved_timestamp.replace(tzinfo=datetime.timezone.utc)

            # Ensure triggered_at has timezone info
            triggered_timestamp = alert.triggered_at
            if triggered_timestamp.tzinfo is None:
                triggered_timestamp = triggered_timestamp.replace(tzinfo=datetime.timezone.utc)

            # 5. Determine Total Downtime
            if rca_payload.total_downtime_seconds is not None:
                total_downtime = datetime.timedelta(seconds=rca_payload.total_downtime_seconds)
            else:
                calculated_diff = resolved_timestamp - triggered_timestamp
                # Ensure downtime is non-negative
                total_downtime = max(calculated_diff, datetime.timedelta(seconds=0))

            # 6. Upsert Root Cause Record
            saved_rca = rca_repo.upsert_for_alert(
                alert_id=alert_id,
                root_cause_name=rca_payload.root_cause_name.strip(),
                category=rca_payload.category.strip(),
                description=rca_payload.description.strip(),
                identified_by=rca_payload.identified_by.strip(),
                customer_confirmed=rca_payload.customer_confirmed,
                total_downtime=total_downtime,
            )
            logger.info("Saved ROOT_CAUSE record (ID: %d) for alert_id=%d", saved_rca.root_cause_id, alert_id)

            # 7. Close any remaining open sensor logs
            closed_logs_count = log_repo.close_open_logs(alert.sensor_id)
            if isinstance(closed_logs_count, int) and closed_logs_count > 0:
                logger.info("Closed %d open sensor logs during closure for sensor_id=%d", closed_logs_count, alert.sensor_id)
            else:
                logger.info("Executed open sensor log closure for sensor_id=%d", alert.sensor_id)

            # 8. Create a CLOSED INFO SENSOR_LOG audit entry
            audit_message = (
                f"Incident successfully closed by '{authenticated_user}'. "
                f"Root Cause: '{saved_rca.root_cause_name}' ({saved_rca.category}). "
                f"Identified by: '{saved_rca.identified_by}'."
            )
            if is_manual_closure and closure_reason:
                audit_message += f" Manual closure reason: '{closure_reason}'."

            log_repo.create(
                sensor_id=alert.sensor_id,
                log_timestamp=closure_now,
                log_level=LogLevelType.INFO,
                log_status=LogStatusType.CLOSED,
                log_message=audit_message,
                log_details=json_safe_dict({
                    "action": "INCIDENT_FINAL_CLOSURE",
                    "alert_id": alert_id,
                    "closed_by": authenticated_user,
                    "is_manual": is_manual_closure,
                    "closure_reason": closure_reason,
                    "root_cause_id": saved_rca.root_cause_id,
                    "root_cause_name": saved_rca.root_cause_name,
                    "category": saved_rca.category,
                    "identified_by": saved_rca.identified_by,
                    "customer_confirmed": saved_rca.customer_confirmed,
                    "total_downtime_seconds": int(total_downtime.total_seconds()),
                    "attachments_count": len(existing_attachments),
                }),
            )

            # 9. Atomic Transaction Commit
            db.commit()
            logger.info("Atomic database transaction committed successfully for alert_id=%d", alert_id)

            # Build response
            rca_read = RootCauseRead.model_validate(saved_rca)
            return ClosureResponse(
                status="success",
                message="Incident closure completed and finalized successfully.",
                alert_id=alert_id,
                sensor_id=alert.sensor_id,
                resolved_at=resolved_timestamp,
                total_downtime_human=rca_read.total_downtime_human,
                root_cause=rca_read,
                attachments_count=len(existing_attachments),
                closed_at=closure_now,
            )

        except ClosureValidationError:
            db.rollback()
            raise
        except Exception as exc:
            db.rollback()
            logger.error("Transaction rolled back for alert_id=%d due to unexpected error: %s", alert_id, exc, exc_info=True)
            raise RepositoryError(f"Database transaction failed during incident closure: {exc}") from exc