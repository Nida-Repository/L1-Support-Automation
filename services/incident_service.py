"""Incident and Alert Lifecycle Query Service.

Compiles complete chronological operational timelines, alerts lists, and
pending closure notification streams for support engineers.
"""
from __future__ import annotations

import datetime
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload, selectinload

from app.crud import (
    AlertHistoryRepository,
    AttachmentRepository,
    EscalationRecordRepository,
    IspEmailThreadRepository,
    NotFoundError,
    PingDiagnosticRepository,
    ReminderHistoryRepository,
    RootCauseRepository,
    SensorLogRepository,
)
from app.models import (
    AlertHistory,
    Attachment,
    EscalationRecord,
    Isp,
    IspContactEmail,
    IspEmailThread,
    PingDiagnostic,
    ReminderHistory,
    RootCause,
    Sensor,
    SensorLog,
    Site,
    SiteIspAssignment,
)
from models.attachment_model import AttachmentRead
from models.incident_history_model import (
    AlertListItemRead,
    AlertSummary,
    IncidentLifecycleHistoryRead,
)
from models.notification_model import (
    PendingClosureNotification,
    PendingClosuresResponse,
)
from models.root_cause_model import RootCauseRead
from utils.json_utils import to_jsonable_python

logger = logging.getLogger(__name__)


class IncidentService:
    """Read-side service for compiling comprehensive incident audit streams."""

    @classmethod
    def get_open_alerts(
        cls,
        db: Session,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> List[AlertListItemRead]:
        """Fetch list of open alerts or alerts currently pending closure."""
        logger.debug("Listing open alerts (limit=%d, offset=%d)", limit, offset)

        stmt = (
            select(AlertHistory)
            .options(
                joinedload(AlertHistory.sensor)
                .joinedload(Sensor.site_isp_assignment)
                .joinedload(SiteIspAssignment.site),
                joinedload(AlertHistory.sensor)
                .joinedload(Sensor.site_isp_assignment)
                .joinedload(SiteIspAssignment.isp),
                joinedload(AlertHistory.state),
                joinedload(AlertHistory.root_cause),
            )
            .where(
                (AlertHistory.resolved_at.is_(None))
                | (AlertHistory.root_cause.has(RootCause.root_cause_id.is_(None)))
            )
            .order_by(AlertHistory.triggered_at.desc())
            .offset(offset)
            .limit(limit)
        )
        alerts = db.execute(stmt).scalars().all()

        results = []
        for a in alerts:
            sensor_name = a.sensor.sensor_name if a.sensor else f"Sensor #{a.sensor_id}"
            site_name = None
            isp_name = None
            if a.sensor and a.sensor.site_isp_assignment:
                if a.sensor.site_isp_assignment.site:
                    site_name = a.sensor.site_isp_assignment.site.site_name
                if a.sensor.site_isp_assignment.isp:
                    isp_name = a.sensor.site_isp_assignment.isp.isp_name

            state_name = a.state.state_name if a.state else "UNKNOWN"
            is_pending = (a.resolved_at is not None and a.root_cause is None)

            results.append(
                AlertListItemRead(
                    alert_id=a.alert_id,
                    sensor_id=a.sensor_id,
                    sensor_name=sensor_name,
                    site_name=site_name,
                    isp_name=isp_name,
                    state_name=state_name,
                    triggered_at=a.triggered_at,
                    resolved_at=a.resolved_at,
                    is_recovered_pending_closure=is_pending,
                    escalation_status=a.escalation_status,
                    alert_message=a.alert_message,
                )
            )
        return results

    @classmethod
    def get_alert_summary(cls, db: Session, alert_id: int) -> AlertSummary:
        """Fetch high-level alert summary with complete site and ISP topology."""
        stmt = (
            select(AlertHistory)
            .options(
                joinedload(AlertHistory.sensor)
                .joinedload(Sensor.site_isp_assignment)
                .joinedload(SiteIspAssignment.site),
                joinedload(AlertHistory.sensor)
                .joinedload(Sensor.site_isp_assignment)
                .joinedload(SiteIspAssignment.isp),
                joinedload(AlertHistory.state),
                joinedload(AlertHistory.root_cause),
            )
            .where(AlertHistory.alert_id == alert_id)
        )
        alert = db.execute(stmt).scalar_one_or_none()
        if not alert:
            raise NotFoundError(AlertHistory, alert_id)

        sensor = alert.sensor
        assignment = sensor.site_isp_assignment if sensor else None
        site = assignment.site if assignment else None
        isp = assignment.isp if assignment else None

        # Determine human status
        if alert.root_cause is not None:
            status_label = "CLOSED"
        elif alert.resolved_at is not None:
            status_label = "RECOVERED_PENDING_CLOSURE"
        else:
            status_label = "OPEN"

        downtime_human = None
        if alert.resolved_at and alert.triggered_at:
            total_seconds = int((alert.resolved_at - alert.triggered_at).total_seconds())
            hours, rem = divmod(total_seconds, 3600)
            mins, secs = divmod(rem, 60)
            parts = []
            if hours > 0:
                parts.append(f"{hours}h")
            if mins > 0 or hours > 0:
                parts.append(f"{mins}m")
            parts.append(f"{secs}s")
            downtime_human = " ".join(parts)

        return AlertSummary(
            alert_id=alert.alert_id,
            sensor_id=alert.sensor_id,
            sensor_name=sensor.sensor_name if sensor else f"Sensor #{alert.sensor_id}",
            sensor_type=sensor.sensor_type if sensor else None,
            site_id=site.site_id if site else None,
            site_name=site.site_name if site else None,
            primary_ip=str(site.primary_ip) if (site and site.primary_ip) else None,
            isp_id=isp.isp_id if isp else None,
            isp_name=isp.isp_name if isp else None,
            circuit_id=assignment.circuit_id if assignment else None,
            state_id=alert.state_id,
            state_name=alert.state.state_name if alert.state else "UNKNOWN",
            current_status=status_label,
            alert_message=alert.alert_message,
            escalation_status=alert.escalation_status,
            triggered_at=alert.triggered_at,
            resolved_at=alert.resolved_at,
            total_downtime_human=downtime_human,
        )

    @classmethod
    def get_incident_history(cls, db: Session, alert_id: int) -> IncidentLifecycleHistoryRead:
        """Compile the complete chronological lifecycle history for an incident."""
        logger.info("Compiling full incident lifecycle history for alert_id=%d", alert_id)

        summary = cls.get_alert_summary(db, alert_id)

        # 1. Root Cause
        rca_repo = RootCauseRepository(db)
        rca_record = rca_repo.get_by_alert(alert_id)
        rca_read = RootCauseRead.model_validate(rca_record) if rca_record else None

        # 2. Attachments
        att_repo = AttachmentRepository(db)
        attachments_page = att_repo.list_for_alert(alert_id, limit=500)
        attachments_read = [AttachmentRead.model_validate(a) for a in attachments_page.items]

        # 3. Chronological Sensor Logs
        log_repo = SensorLogRepository(db)
        sensor_logs = log_repo.list_for_sensor(summary.sensor_id, limit=500)
        # Filter sensor logs bounded roughly around this incident's timeframe
        sensor_logs_serialized = [
            {
                "log_id": lg.log_id,
                "timestamp": lg.log_timestamp.isoformat(),
                "level": lg.log_level.value if hasattr(lg.log_level, "value") else str(lg.log_level),
                "status": lg.log_status.value if hasattr(lg.log_status, "value") else str(lg.log_status),
                "message": lg.log_message,
                "details": to_jsonable_python(lg.log_details),
            }
            for lg in sorted(sensor_logs, key=lambda x: x.log_timestamp)
        ]

        # 4. Ping Diagnostics
        ping_repo = PingDiagnosticRepository(db)
        ping_diags = ping_repo.list_for_alert(alert_id)
        ping_diags_serialized = [
            {
                "ping_id": p.ping_id,
                "executed_at": p.executed_at.isoformat(),
                "packet_count": p.packet_count,
                "packet_loss_percent": float(p.packet_loss_percent) if p.packet_loss_percent is not None else None,
                "min_rtt_ms": float(p.min_rtt_ms) if p.min_rtt_ms is not None else None,
                "avg_rtt_ms": float(p.avg_rtt_ms) if p.avg_rtt_ms is not None else None,
                "max_rtt_ms": float(p.max_rtt_ms) if p.max_rtt_ms is not None else None,
                "technician_notes": p.technician_notes,
            }
            for p in sorted(ping_diags, key=lambda x: x.executed_at)
        ]

        # 5. ISP Email Threads
        thread_repo = IspEmailThreadRepository(db)
        threads_page = thread_repo.list_for_alert(alert_id, limit=500)
        email_threads_serialized = [
            {
                "thread_id": t.thread_id,
                "message_id": t.message_id,
                "in_reply_to": t.in_reply_to,
                "sender": t.sender,
                "receiver": t.receiver,
                "subject": t.subject,
                "direction": t.direction.value if hasattr(t.direction, "value") else str(t.direction),
                "classification": t.classification_type.value if hasattr(t.classification_type, "value") else str(t.classification_type),
                "sent_received_at": t.sent_received_at.isoformat(),
                "body": t.body,
                "attachments": [
                    {
                        "attachment_id": att.attachment_id,
                        "file_name": att.file_name,
                        "file_size": att.file_size,
                        "file_type": att.file_type,
                        "object_key": att.object_key,
                    }
                    for att in t.attachments
                ],
            }
            for t in sorted(threads_page.items, key=lambda x: x.sent_received_at)
        ]

        # 6. Reminder History
        reminder_repo = ReminderHistoryRepository(db)
        reminders_page = reminder_repo.list_for_alert(alert_id, limit=500)
        reminder_history_serialized = [
            {
                "reminder_id": rm.reminder_id,
                "reminder_number": rm.reminder_number,
                "sent_at": rm.sent_at.isoformat(),
                "status": rm.status.value if hasattr(rm.status, "value") else str(rm.status),
                "response_received": rm.response_received,
                "response_received_at": rm.response_received_at.isoformat() if rm.response_received_at else None,
            }
            for rm in sorted(reminders_page.items, key=lambda x: x.reminder_number)
        ]

        # 7. Escalation History
        esc_repo = EscalationRecordRepository(db)
        escalations = esc_repo.list_for_alert(alert_id)
        escalations_serialized = [
            {
                "escalation_id": esc.escalation_id,
                "escalated_to": esc.escalated_to,
                "recipient_email": esc.recipient_email,
                "sent_at": esc.sent_at.isoformat(),
                "email_subject": esc.email_subject,
                "response_received": esc.response_received,
                "response_notes": esc.response_notes,
            }
            for esc in sorted(escalations, key=lambda x: x.sent_at)
        ]

        return IncidentLifecycleHistoryRead(
            alert_information=summary,
            triggered_time=summary.triggered_at,
            resolved_time=summary.resolved_at,
            total_downtime=summary.total_downtime_human,
            current_status=summary.current_status,
            root_cause_analysis=rca_read,
            attachments=attachments_read,
            sensor_logs=sensor_logs_serialized,
            ping_diagnostics=ping_diags_serialized,
            isp_email_threads=email_threads_serialized,
            reminder_history=reminder_history_serialized,
            escalation_history=escalations_serialized,
        )

    @classmethod
    def get_pending_closures(cls, db: Session) -> PendingClosuresResponse:
        """Find all sensors that have recovered but are pending Root Cause Analysis / closure."""
        logger.debug("Querying all alerts pending incident closure...")

        # Alert has resolved_at set, but RootCause is NULL
        stmt = (
            select(AlertHistory)
            .options(
                joinedload(AlertHistory.sensor)
                .joinedload(Sensor.site_isp_assignment)
                .joinedload(SiteIspAssignment.site),
                joinedload(AlertHistory.sensor)
                .joinedload(Sensor.site_isp_assignment)
                .joinedload(SiteIspAssignment.isp),
                joinedload(AlertHistory.root_cause),
            )
            .where(
                AlertHistory.resolved_at.isnot(None),
                AlertHistory.root_cause == None,  # noqa: E711
            )
            .order_by(AlertHistory.resolved_at.desc())
        )
        alerts = db.execute(stmt).scalars().all()

        notifications: List[PendingClosureNotification] = []
        for a in alerts:
            sensor_name = a.sensor.sensor_name if a.sensor else f"Sensor #{a.sensor_id}"
            site_name = None
            isp_name = None
            circuit_id = None
            if a.sensor and a.sensor.site_isp_assignment:
                circuit_id = a.sensor.site_isp_assignment.circuit_id
                if a.sensor.site_isp_assignment.site:
                    site_name = a.sensor.site_isp_assignment.site.site_name
                if a.sensor.site_isp_assignment.isp:
                    isp_name = a.sensor.site_isp_assignment.isp.isp_name

            # Calculate downtime
            downtime_seconds = None
            downtime_human = None
            if a.resolved_at and a.triggered_at:
                downtime_seconds = int((a.resolved_at - a.triggered_at).total_seconds())
                hours, rem = divmod(downtime_seconds, 3600)
                mins, secs = divmod(rem, 60)
                parts = []
                if hours > 0:
                    parts.append(f"{hours}h")
                if mins > 0 or hours > 0:
                    parts.append(f"{mins}m")
                parts.append(f"{secs}s")
                downtime_human = " ".join(parts)

            msg = f"Sensor '{sensor_name}' recovered successfully. Incident closure information is pending."

            notifications.append(
                PendingClosureNotification(
                    alert_id=a.alert_id,
                    sensor_id=a.sensor_id,
                    sensor_name=sensor_name,
                    site_name=site_name,
                    isp_name=isp_name,
                    circuit_id=circuit_id,
                    triggered_at=a.triggered_at,
                    recovered_at=a.resolved_at,
                    downtime_seconds=downtime_seconds,
                    downtime_human=downtime_human,
                    notification_message=msg,
                )
            )

        logger.info("Found %d incidents pending closure", len(notifications))
        return PendingClosuresResponse(count=len(notifications), items=notifications)