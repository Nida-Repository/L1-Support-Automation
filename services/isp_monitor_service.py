"""ISP Reply Monitoring and Automated Reminder Service.

Orchestrates periodic reply scanning, timeout checks, threaded reminder email dispatch,
and notification emission without overloading the primary database.
"""
from __future__ import annotations

import datetime
import logging
from typing import Any, Dict, List, Optional

from app.crud import (
    AlertHistoryRepository,
    EscalationRecordRepository,
    IspEmailThreadRepository,
    NotFoundError,
    ReminderHistoryRepository,
    RepositoryError,
    session_scope,
)
from app.database import SessionLocal
from app.models import EmailClassificationType, EmailDirectionType, ReminderStatusType
from cache.redis_cache import EmailThreadCache, IspReplyMonitor
from clients.smtp_client import _render_template, _send_email, _send_threaded_email
from config.settings import settings

logger = logging.getLogger(__name__)


class IspReplyMonitorService:
    """Service handling ISP reply monitoring scans, reminder generation, and reply event triggers."""

    @classmethod
    def register_monitoring(
        cls,
        *,
        alert_id: int,
        escalation_id: int,
        message_id: str,
        isp_email: str,
        isp_email_id: Optional[int] = None,
        sensor_name: str = "",
        site_name: str = "",
        isp_name: str = "",
        circuit_id: str = "",
        original_subject: str = "",
        to_addresses: Optional[List[str]] = None,
        cc_addresses: Optional[List[str]] = None,
    ) -> bool:
        """Register a newly delivered ISP email for automated reminder tracking."""
        return IspReplyMonitor.register_monitor(
            alert_id=alert_id,
            escalation_id=escalation_id,
            message_id=message_id,
            isp_email=isp_email,
            isp_email_id=isp_email_id,
            sensor_name=sensor_name,
            site_name=site_name,
            isp_name=isp_name,
            circuit_id=circuit_id,
            original_subject=original_subject,
            to_addresses=to_addresses,
            cc_addresses=cc_addresses,
        )

    @classmethod
    def handle_reply_received(
        cls,
        *,
        alert_id: Optional[int] = None,
        message_id: Optional[str] = None,
        in_reply_to: Optional[str] = None,
        references: Optional[List[str]] = None,
    ) -> None:
        """Invoked when an inbound email reply is detected. Immediately updates Redis state and persists metadata."""
        logger.info(
            "Handling ISP reply received [Alert: %s | MsgID: %s | In-Reply-To: %s]",
            alert_id,
            message_id,
            in_reply_to,
        )

        monitor_state = IspReplyMonitor.mark_response_received(
            message_id=message_id,
            in_reply_to=in_reply_to,
            references=references,
            alert_id=alert_id,
        )

        resolved_alert_id = alert_id or (monitor_state.get("alert_id") if monitor_state else None)
        if not resolved_alert_id:
            logger.debug("No alert_id resolved for incoming reply [MsgID: %s]", message_id)
            return

        now_utc = datetime.datetime.now(datetime.timezone.utc)

        # Update ReminderHistory rows for this alert
        try:
            with session_scope(SessionLocal) as session:
                reminder_repo = ReminderHistoryRepository(session)
                reminders_page = reminder_repo.list_for_alert(resolved_alert_id, limit=100)
                for rm in reminders_page.items:
                    if not rm.response_received:
                        reminder_repo.mark_responded(rm.reminder_id, response_received_at=now_utc)
                        logger.info("Marked reminder #%d as responded for alert_id=%d", rm.reminder_number, resolved_alert_id)
        except Exception as exc:
            logger.error("Error updating ReminderHistory on reply for alert_id=%s: %s", resolved_alert_id, exc)

        # Emit structured log notification
        logger.info(
            "ISP_REPLY_NOTIFICATION | alert_id=%s | sensor_name=%s | site_name=%s | isp_name=%s | "
            "isp_email=%s | status=RESPONSE_RECEIVED | received_at=%s",
            resolved_alert_id,
            monitor_state.get("sensor_name") if monitor_state else "Unknown",
            monitor_state.get("site_name") if monitor_state else "Unknown",
            monitor_state.get("isp_name") if monitor_state else "Unknown",
            monitor_state.get("isp_email") if monitor_state else "Unknown",
            now_utc.isoformat(),
        )

    @classmethod
    def run_scan(cls) -> None:
        """Scan all active ISP monitors from Redis and process reminders or final timeouts."""
        if not IspReplyMonitor.acquire_scan_lock():
            logger.debug("ISP reply scan skipped: lock already acquired by another worker/beat tick.")
            return

        try:
            active_monitors = IspReplyMonitor.get_all_active_monitors()
            if not active_monitors:
                logger.debug("ISP reply scan: No active monitors in Redis.")
                return

            logger.info("ISP reply scan: checking %d active monitor(s)...", len(active_monitors))
            now_utc = datetime.datetime.now(datetime.timezone.utc)

            for state in active_monitors:
                try:
                    cls._process_single_monitor(state, now_utc)
                except Exception as mon_exc:
                    logger.error(
                        "Unexpected error processing monitor for alert_id=%s [MsgID: %s]: %s",
                        state.get("alert_id"),
                        state.get("message_id"),
                        mon_exc,
                        exc_info=True,
                    )
        finally:
            IspReplyMonitor.release_scan_lock()

    @classmethod
    def _process_single_monitor(cls, state: Dict[str, Any], now_utc: datetime.datetime) -> None:
        """Process a single monitor state from Redis."""
        alert_id = state.get("alert_id")
        orig_msg_id = state.get("message_id")
        if not orig_msg_id or not alert_id:
            return

        # 1. If response was already received, stop monitoring
        if state.get("response_received", False) or not state.get("monitoring_active", True):
            IspReplyMonitor.stop_monitoring(orig_msg_id, alert_id)
            return

        reminder_count = state.get("reminder_count", 0)
        max_reminders = settings.isp_max_reminders

        # 2. Check if max reminders reached
        if reminder_count >= max_reminders:
            logger.warning(
                "ISP monitor reached max reminders (%d/%d) without response [Alert: %s | MsgID: %s]",
                reminder_count,
                max_reminders,
                alert_id,
                orig_msg_id,
            )
            IspReplyMonitor.stop_monitoring(orig_msg_id, alert_id)
            cls._emit_max_reminders_reached_notification(state)
            return

        # 3. Check if timeout period has elapsed
        next_reminder_str = state.get("next_reminder_time")
        if not next_reminder_str:
            return

        try:
            next_reminder_dt = datetime.datetime.fromisoformat(next_reminder_str)
            if next_reminder_dt.tzinfo is None:
                next_reminder_dt = next_reminder_dt.replace(tzinfo=datetime.timezone.utc)
        except Exception:
            next_reminder_dt = now_utc

        if now_utc < next_reminder_dt:
            # Not yet due for reminder
            logger.debug(
                "Monitor for alert_id=%s not yet due [Next: %s | Now: %s]",
                alert_id,
                next_reminder_dt.isoformat(),
                now_utc.isoformat(),
            )
            return

        # 4. Due for reminder -> Send reminder email
        cls._send_and_record_reminder(state, now_utc)

    @classmethod
    def _send_and_record_reminder(cls, state: Dict[str, Any], now_utc: datetime.datetime) -> None:
        """Render and dispatch a reminder email in the same thread, updating DB and Redis."""
        alert_id = state["alert_id"]
        orig_msg_id = state["message_id"]
        reminder_number = state.get("reminder_count", 0) + 1
        circuit_id = state.get("circuit_id", "Unknown")
        sensor_name = state.get("sensor_name", "Unknown")
        site_name = state.get("site_name", "Unknown")
        isp_name = state.get("isp_name", "Unknown")
        isp_email = state.get("isp_email")
        isp_email_id = state.get("isp_email_id")
        to_addresses = state.get("to_addresses") or [isp_email]
        cc_addresses = state.get("cc_addresses") or []
        references = state.get("original_references") or []

        # Ensure support team email is in CC
        support_email = settings.support_team_email
        if support_email and support_email not in cc_addresses:
            cc_addresses.append(support_email)

        context = {
            "alert_id": alert_id,
            "reminder_number": reminder_number,
            "circuit_id": circuit_id,
            "sensor_name": sensor_name,
            "site_name": site_name,
            "isp_name": isp_name,
            "reminder_sent_at": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        }

        try:
            subject = _render_template("isp_reminder_subject.txt", context).replace("\r", "").replace("\n", "").strip()
            body_html = _render_template("isp_reminder_body.html", context)
        except Exception as exc:
            logger.error("Failed to render reminder template for alert_id=%s: %s", alert_id, exc)
            return

        logger.info(
            "Sending ISP reminder #%d for alert_id=%s to %s (In-Reply-To: %s)",
            reminder_number,
            alert_id,
            to_addresses,
            orig_msg_id,
        )

        reminder_msg_id: Optional[str] = None
        try:
            reminder_msg_id = _send_threaded_email(
                to_addresses=to_addresses,
                cc_addresses=cc_addresses,
                subject=subject,
                body_html=body_html,
                in_reply_to=orig_msg_id,
                references=references,
            )
        except Exception as exc:
            logger.error(
                "Failed to send reminder #%d email for alert_id=%s: %s",
                reminder_number,
                alert_id,
                exc,
                exc_info=True,
            )
            return

        # Persist ReminderHistory & IspEmailThread in database
        try:
            with session_scope(SessionLocal) as session:
                reminder_repo = ReminderHistoryRepository(session)
                reminder_repo.create(
                    alert_id=alert_id,
                    reminder_number=reminder_number,
                    sent_at=now_utc,
                    email_id=isp_email_id,
                    response_received=False,
                    status=ReminderStatusType.SENT,
                )

                if reminder_msg_id:
                    thread_repo = IspEmailThreadRepository(session)
                    ref_chain = references + [orig_msg_id]
                    thread_repo.create(
                        alert_id=alert_id,
                        message_id=reminder_msg_id,
                        in_reply_to=orig_msg_id,
                        email_references=ref_chain,
                        sender=settings.smtp_from_address,
                        receiver=", ".join(to_addresses),
                        cc=cc_addresses,
                        subject=subject,
                        body=body_html,
                        direction=EmailDirectionType.OUTGOING,
                        classification_type=EmailClassificationType.UNKNOWN,
                        sent_received_at=now_utc,
                    )

                alert_repo = AlertHistoryRepository(session)
                alert_repo.update(
                    alert_id,
                    escalation_status=f"Reminder #{reminder_number} Sent to ISP",
                )
        except Exception as db_exc:
            logger.error("Failed to persist reminder #%d in database for alert_id=%s: %s", reminder_number, alert_id, db_exc)

        # Cache new reminder Message-ID in Redis for thread lookups
        if reminder_msg_id:
            EmailThreadCache.set_message_id_mapping(
                reminder_msg_id,
                {
                    "alert_id": alert_id,
                    "escalation_id": state.get("escalation_id"),
                },
            )

        # Update Redis monitor state for next reminder calculation
        next_timeout = settings.isp_reply_timeout_minutes
        next_reminder_time = now_utc + datetime.timedelta(minutes=next_timeout)
        updated_references = list(references)
        if reminder_msg_id and reminder_msg_id not in updated_references:
            updated_references.append(reminder_msg_id)

        IspReplyMonitor.update_monitor(
            orig_msg_id,
            {
                "reminder_count": reminder_number,
                "original_references": updated_references,
                "last_reminder_sent_at": now_utc.isoformat(),
                "next_reminder_time": next_reminder_time.isoformat(),
            },
        )

        logger.info(
            "ISP_REMINDER_NOTIFICATION | alert_id=%d | sensor_name=%s | site_name=%s | "
            "isp_name=%s | isp_email=%s | reminder_number=%d | sent_at=%s | status=SENT | next_reminder=%s",
            alert_id,
            sensor_name,
            site_name,
            isp_name,
            isp_email,
            reminder_number,
            now_utc.isoformat(),
            next_reminder_time.isoformat(),
        )

    @classmethod
    def _emit_max_reminders_reached_notification(cls, state: Dict[str, Any]) -> None:
        """Send final escalation notification to support team when ISP fails to respond after maximum reminders."""
        alert_id = state.get("alert_id")
        sensor_name = state.get("sensor_name", "Unknown")
        site_name = state.get("site_name", "Unknown")
        isp_name = state.get("isp_name", "Unknown")
        isp_email = state.get("isp_email", "Unknown")
        max_reminders = state.get("reminder_count", settings.isp_max_reminders)
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        # Update alert escalation status in DB
        try:
            with session_scope(SessionLocal) as session:
                alert_repo = AlertHistoryRepository(session)
                alert_repo.update(
                    alert_id,
                    escalation_status=f"Max Reminders ({max_reminders}) Reached - No ISP Response",
                )
        except Exception as db_exc:
            logger.error("Failed to update alert escalation status on max reminders: %s", db_exc)

        # Notify internal support team via email
        support_email = settings.support_team_email
        if support_email:
            subject = f"[ESCALATION] No ISP Reply after {max_reminders} Reminders – Alert ID {alert_id} ({sensor_name})"
            body = f"""<html>
  <body style="font-family: Arial, sans-serif; font-size: 14px; color: #222222;">
    <h3 style="color: #d9534f;">ISP Escalation Alert: Maximum Reminders Exhausted</h3>
    <p>The ISP (<strong>{isp_name}</strong>, {isp_email}) has not replied to incident notifications for <strong>{sensor_name}</strong> at <strong>{site_name}</strong> after <strong>{max_reminders}</strong> automated reminders.</p>
    <p><strong>Alert ID:</strong> {alert_id}<br/>
       <strong>Circuit ID:</strong> {state.get('circuit_id', 'N/A')}<br/>
       <strong>Sent At:</strong> {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
    <p>Please perform manual phone or tier-2 escalation with the provider immediately.</p>
  </body>
</html>"""
            try:
                _send_email(
                    to_addresses=[support_email],
                    cc_addresses=None,
                    subject=subject,
                    body_html=body,
                )
                logger.info("Sent max-reminders escalation notice to support team (%s) for alert_id=%s", support_email, alert_id)
            except Exception as exc:
                logger.error("Failed to send max-reminders escalation notice: %s", exc)

        logger.critical(
            "ISP_MAX_REMINDERS_NOTIFICATION | alert_id=%s | sensor_name=%s | site_name=%s | "
            "isp_name=%s | isp_email=%s | total_reminders=%d | status=MAX_REMINDERS_REACHED",
            alert_id,
            sensor_name,
            site_name,
            isp_name,
            isp_email,
            max_reminders,
        )
