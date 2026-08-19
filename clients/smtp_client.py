"""SMTP Email Client and Notification Dispatcher.

Handles Jinja2 template rendering, MIME composition, SMTP dispatch over TLS/SSL,
and escalation audit trail persistence in PostgreSQL.
"""
from __future__ import annotations

import datetime
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import make_msgid
from pathlib import Path
from typing import Any, Dict, List, Optional

from email_validator import EmailNotValidError, validate_email
from jinja2 import Environment, FileSystemLoader, TemplateNotFound, select_autoescape

from app.crud import (
    AlertHistoryRepository,
    ConstraintViolationError,
    DuplicateError,
    EscalationRecordRepository,
    IspContactEmailRepository,
    IspEmailThreadRepository,
    NotFoundError,
    RepositoryError,
    SiteIspAssignmentRepository,
    session_scope,
)
from app.database import SessionLocal
from app.models import EmailClassificationType, EmailDirectionType
from cache.redis_cache import EmailThreadCache
from clients.email_utils import clean_message_id
from config.settings import settings

logger = logging.getLogger(__name__)

TEMPLATE_DIR = settings.project_root / "templates" / "email"

logger.info(
    "Initializing Email Service [Host: %s:%d | TLS: %s | SSL: %s | From: %s]",
    settings.smtp_host,
    settings.smtp_port,
    settings.smtp_use_tls,
    settings.smtp_use_ssl,
    settings.smtp_from_address,
)

if not TEMPLATE_DIR.exists():
    logger.warning("Email template directory does not exist at path: %s", TEMPLATE_DIR)
else:
    logger.debug("Email template directory loaded: %s", TEMPLATE_DIR)

_jinja_env = Environment(
    loader=FileSystemLoader(str(TEMPLATE_DIR)),
    autoescape=select_autoescape(["html", "xml"]),
    trim_blocks=True,
    lstrip_blocks=True,
)


class EmailDispatchError(Exception):
    """Raised when an email cannot be rendered or dispatched."""
    pass


# --------------------------------------------------------------------------- #
# Template Rendering & Email Validation
# --------------------------------------------------------------------------- #

def _render_template(template_name: str, context: Dict[str, Any]) -> str:
    logger.debug("Rendering email template '%s' with context keys: %s", template_name, list(context.keys()))
    try:
        template = _jinja_env.get_template(template_name)
    except TemplateNotFound as exc:
        logger.error("Email template not found: %s (looked in %s)", template_name, TEMPLATE_DIR)
        raise EmailDispatchError(f"template not found: {template_name}") from exc
    try:
        rendered = template.render(**context)
        logger.debug("Successfully rendered template '%s'", template_name)
        return rendered
    except Exception as exc:
        logger.exception("Failed to render template %s", template_name)
        raise EmailDispatchError(f"failed to render template: {template_name}") from exc


def _validated(email_address: Optional[str]) -> Optional[str]:
    if not email_address or not email_address.strip():
        return None
    try:
        normalized_email = validate_email(email_address.strip(), check_deliverability=False).normalized
        return normalized_email
    except EmailNotValidError as exc:
        logger.error("Invalid email address %r rejected: %s", email_address, exc)
        return None


# --------------------------------------------------------------------------- #
# SMTP Transport
# --------------------------------------------------------------------------- #

def _send_email(
    *,
    to_addresses: List[str],
    cc_addresses: Optional[List[str]],
    subject: str,
    body_html: str,
) -> str:
    if not to_addresses:
        logger.error("Email dispatch attempted without target recipients (To: field is empty)")
        raise EmailDispatchError("no recipient addresses provided")

    domain = settings.smtp_from_address.split("@")[-1] if "@" in settings.smtp_from_address else None
    msg_id = make_msgid(domain=domain)

    msg = MIMEMultipart("alternative")
    msg["Message-ID"] = msg_id
    msg["Subject"] = subject
    msg["From"] = settings.smtp_from_address
    msg["To"] = ", ".join(to_addresses)
    if cc_addresses:
        msg["Cc"] = ", ".join(cc_addresses)
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    all_recipients = list(to_addresses) + list(cc_addresses or [])

    logger.info(
        "Attempting SMTP dispatch [Host: %s:%d] | Message-ID: %s | To: %s | CC: %s | Subject: %r",
        settings.smtp_host,
        settings.smtp_port,
        msg_id,
        to_addresses,
        cc_addresses or [],
        subject,
    )

    try:
        smtp_cls = smtplib.SMTP_SSL if settings.smtp_use_ssl else smtplib.SMTP
        logger.debug("Connecting to SMTP server using %s", smtp_cls.__name__)

        with smtp_cls(settings.smtp_host, settings.smtp_port, timeout=settings.smtp_timeout_seconds) as server:
            server.ehlo()
            if settings.smtp_use_tls and not settings.smtp_use_ssl:
                logger.debug("Initiating STARTTLS for SMTP session...")
                server.starttls()
                server.ehlo()
            if settings.smtp_username and settings.smtp_password:
                logger.debug("Authenticating with SMTP server as user '%s'", settings.smtp_username)
                server.login(settings.smtp_username, settings.smtp_password)

            server.sendmail(settings.smtp_from_address, all_recipients, msg.as_string())
            logger.info("Email successfully sent via SMTP to %d recipient(s)", len(all_recipients))
            return msg_id

    except smtplib.SMTPAuthenticationError as exc:
        logger.error("SMTP authentication failed against %s:%s -> %s", settings.smtp_host, settings.smtp_port, exc)
        raise EmailDispatchError("SMTP authentication failed") from exc
    except smtplib.SMTPRecipientsRefused as exc:
        logger.error("SMTP server refused recipients %s: %s", all_recipients, exc)
        raise EmailDispatchError("recipients refused by SMTP server") from exc
    except smtplib.SMTPConnectError as exc:
        logger.error("Could not connect to SMTP server %s:%s -> %s", settings.smtp_host, settings.smtp_port, exc)
        raise EmailDispatchError("could not connect to SMTP server") from exc
    except smtplib.SMTPException as exc:
        logger.error("SMTP error while sending mail: %s", exc)
        raise EmailDispatchError("SMTP error") from exc
    except (TimeoutError, OSError) as exc:
        logger.error("Network error contacting SMTP host %s:%s -> %s", settings.smtp_host, settings.smtp_port, exc)
        raise EmailDispatchError("network error contacting SMTP server") from exc


# --------------------------------------------------------------------------- #
# Public Dispatch Functions
# --------------------------------------------------------------------------- #

def send_alert_notification(payload: Dict[str, Any]) -> None:
    """Dispatches an alert notification email to the primary ISP contact and logs escalation.

    Sync function designed to be invoked from worker tasks or background threads.
    """
    site_id = payload.get("site_id")
    alert_id = payload.get("alert_id")
    ping_diagnostic_id = payload.get("ping_diagnostic_id")
    ping_results = payload.get("ping_results") or {}

    logger.info("Processing alert notification for site_id=%s, alert_id=%s", site_id, alert_id)

    if site_id is None:
        logger.error("send_alert_notification called without site_id; payload=%r", payload)
        return
    if alert_id is None:
        logger.warning(
            "send_alert_notification called without alert_id for site_id=%s; escalation cannot be recorded, aborting.",
            site_id,
        )
        return

    try:
        with session_scope(SessionLocal) as session:
            logger.debug("Fetching primary ISP assignment for site_id=%s", site_id)
            assignment_repo = SiteIspAssignmentRepository(session)
            assignment = assignment_repo.get_primary_for_site(site_id)
            if assignment is None:
                logger.error("No primary ISP assignment found for site_id=%s", site_id)
                return
            circuit_id = assignment.circuit_id
            isp_id = assignment.isp_id

            logger.debug("Fetching active contact emails for isp_id=%s", isp_id)
            contact_repo = IspContactEmailRepository(session)
            isp_contacts = contact_repo.list_active_for_isp(isp_id)
    except NotFoundError:
        logger.error("Site or ISP not found while preparing escalation for site_id=%s", site_id)
        return
    except RepositoryError:
        logger.exception("Repository error looking up ISP details for site_id=%s", site_id)
        return
    except Exception:
        logger.exception("Unexpected error looking up ISP details for site_id=%s", site_id)
        return

    isp_recipient = _validated(isp_contacts[0].email_address) if isp_contacts else None
    support_recipient = _validated(settings.support_team_email)
    if not support_recipient:
        logger.warning("SUPPORT_TEAM_EMAIL is not configured (or invalid) in the environment.")

    template_context = {
        "site_id": site_id,
        "alert_id": alert_id,
        "circuit_id": circuit_id,
        "ping_diagnostic_id": ping_diagnostic_id,
        "packet_loss_percent": ping_results.get("packet_loss_percent"),
        "min_rtt_ms": ping_results.get("min_rtt_ms"),
        "avg_rtt_ms": ping_results.get("avg_rtt_ms"),
        "max_rtt_ms": ping_results.get("max_rtt_ms"),
    }

    if not isp_recipient:
        logger.error(
            "No usable active contact email for isp_id=%s (site_id=%s); escalation email not sent.",
            isp_id,
            site_id,
        )
        return

    cc_list: List[str] = [support_recipient] if support_recipient else []

    _handle_escalation(
        escalated_to="ISP",
        recipient_email=isp_recipient,
        cc_emails=cc_list if cc_list else None,
        subject_template="isp_alert_subject.txt",
        body_template="isp_alert_body.html",
        context=template_context,
        alert_id=alert_id,
    )


def _handle_escalation(
    *,
    escalated_to: str,
    recipient_email: str,
    cc_emails: Optional[List[str]],
    subject_template: str,
    body_template: str,
    context: Dict[str, Any],
    alert_id: int,
) -> None:
    """Render and send escalation email, then record the audit trail in the DB."""
    logger.debug("Executing escalation handler for alert_id=%s to target '%s'", alert_id, escalated_to)
    try:
        subject = _render_template(subject_template, context).strip()
        body = _render_template(body_template, context)
    except EmailDispatchError:
        logger.error(
            "Aborting %s escalation for alert_id=%s: template rendering failed",
            escalated_to,
            alert_id,
        )
        return

    email_sent = False
    msg_id: Optional[str] = None
    try:
        msg_id = _send_email(
            to_addresses=[recipient_email],
            cc_addresses=cc_emails,
            subject=subject,
            body_html=body,
        )
        email_sent = True
        logger.info(
            "Sent %s escalation email for alert_id=%s to %s (Message-ID: %s)",
            escalated_to,
            alert_id,
            recipient_email,
            msg_id,
        )
    except EmailDispatchError:
        logger.exception(
            "Failed to send %s escalation email for alert_id=%s to %s",
            escalated_to,
            alert_id,
            recipient_email,
        )

    clean_id = clean_message_id(msg_id) if msg_id else None
    thread_id = None
    escalation_id = None

    try:
        logger.debug("Persisting escalation audit log and email thread to database for alert_id=%s...", alert_id)
        with session_scope(SessionLocal) as session:
            escalation_repo = EscalationRecordRepository(session)
            escalation_rec = escalation_repo.create(
                alert_id=alert_id,
                escalated_to=escalated_to,
                recipient_email=recipient_email,
                cc_emails=cc_emails,
                email_subject=subject,
                email_body=body,
                response_received=False,
                response_notes=None if email_sent else "Email dispatch failed; see worker logs.",
            )
            escalation_id = getattr(escalation_rec, "escalation_id", None)

            # Store outgoing email thread record
            if email_sent and clean_id:
                thread_repo = IspEmailThreadRepository(session)
                thread_rec = thread_repo.create(
                    alert_id=alert_id,
                    message_id=clean_id,
                    sender=settings.smtp_from_address,
                    receiver=recipient_email,
                    cc=cc_emails,
                    subject=subject,
                    body=body,
                    direction=EmailDirectionType.OUTGOING,
                    classification_type=EmailClassificationType.UNKNOWN,
                    sent_received_at=datetime.datetime.now(datetime.timezone.utc),
                )
                thread_id = getattr(thread_rec, "thread_id", None)
                logger.info(
                    "Recorded OUTGOING email thread (thread_id=%s, msg_id=%s) for alert_id=%s",
                    thread_id,
                    clean_id,
                    alert_id,
                )

            alert_repo = AlertHistoryRepository(session)
            alert_repo.update(
                alert_id,
                escalation_status=(
                    f"Escalated to {escalated_to}" if email_sent else f"Escalation to {escalated_to} failed"
                ),
            )
        logger.info("Recorded %s escalation (sent=%s) for alert_id=%s", escalated_to, email_sent, alert_id)

        # Populate Redis cache immediately after successful commit
        if email_sent and clean_id:
            EmailThreadCache.set_message_id_mapping(
                clean_id,
                {
                    "alert_id": alert_id,
                    "thread_id": thread_id,
                    "escalation_id": escalation_id,
                },
            )
    except NotFoundError:
        logger.error("alert_id=%s not found while recording %s escalation", alert_id, escalated_to)
    except DuplicateError as exc:
        logger.error("Duplicate escalation or email thread record for alert_id=%s: %s", alert_id, exc)
    except ConstraintViolationError as exc:
        logger.error("Constraint violation recording escalation for alert_id=%s: %s", alert_id, exc)
    except RepositoryError:
        logger.exception("Repository error recording escalation for alert_id=%s", alert_id)
    except Exception:
        logger.exception("Unexpected DB error recording escalation for alert_id=%s", alert_id)


def send_warning_notification(payload: Dict[str, Any]) -> bool:
    """Dispatches a warning notification email to internal support team."""
    support_email = _validated(settings.support_team_email)
    if not support_email:
        logger.error("SUPPORT_TEAM_EMAIL is not configured in the environment.")
        return False

    try:
        context = {
            "site_name": payload.get("site_name") or payload.get("device", "Unknown Site"),
            "sensor_name": payload.get("sensor_name") or payload.get("sensor", "Unknown Sensor"),
            "status": payload.get("status", "Warning"),
            "message": payload.get("message") or payload.get("lastvalue", "No message provided"),
            "timestamp": payload.get("timestamp") or payload.get("datetime", ""),
        }

        subject = _render_template("warning_subject.txt", context).replace("\r", "").replace("\n", "").strip()
        body = _render_template("warning_body.html", context)

        _send_email(
            to_addresses=[support_email],
            cc_addresses=None,
            subject=subject,
            body_html=body,
        )
        logger.info("Warning email sent successfully to support team.")
        return True
    except Exception as exc:
        logger.exception("Failed to send warning email: %s", exc)
        return False


def send_paused_notification(payload: Dict[str, Any]) -> bool:
    """Dispatches a paused notification email to internal support team."""
    support_email = _validated(settings.support_team_email)
    if not support_email:
        logger.error("SUPPORT_TEAM_EMAIL is not configured in the environment.")
        return False

    try:
        context = {
            "site_name": payload.get("site_name") or payload.get("device", "Unknown Site"),
            "sensor_name": payload.get("sensor_name") or payload.get("sensor", "Unknown Sensor"),
            "status": payload.get("status", "Paused"),
            "message": payload.get("message") or payload.get("lastvalue", "No message provided"),
            "timestamp": payload.get("timestamp") or payload.get("datetime", ""),
        }

        subject = _render_template("paused_subject.txt", context).replace("\r", "").replace("\n", "").strip()
        body = _render_template("paused_body.html", context)

        _send_email(
            to_addresses=[support_email],
            cc_addresses=None,
            subject=subject,
            body_html=body,
        )
        logger.info("Sensor Paused email sent successfully to support team.")
        return True
    except Exception as exc:
        logger.exception("Failed to send paused email: %s", exc)
        return False