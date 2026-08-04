from __future__ import annotations

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from email_validator import EmailNotValidError, validate_email
from jinja2 import Environment, FileSystemLoader, TemplateNotFound, select_autoescape

from app.crud import (
    AlertHistoryRepository,
    ConstraintViolationError,
    DuplicateError,
    EscalationRecordRepository,
    IspContactEmailRepository,
    NotFoundError,
    RepositoryError,
    SiteIspAssignmentRepository,
    session_scope,
)
from app.database import SessionLocal

load_dotenv()

# Module Logger Definition
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Configuration (pulled from .env)
# --------------------------------------------------------------------------- #

SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").strip().lower() == "true"
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "false").strip().lower() == "true"
SMTP_FROM_ADDRESS = os.getenv("SMTP_FROM_ADDRESS", "noreply@example.com")
SMTP_TIMEOUT_SECONDS = float(os.getenv("SMTP_TIMEOUT_SECONDS", "15"))

SUPPORT_TEAM_EMAIL = os.getenv("SUPPORT_TEAM_EMAIL")

# templates/email/  (sibling of the app/)
TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "templates" / "email"

logger.info(
    "Initializing Email Service [Host: %s:%d | TLS: %s | SSL: %s | From: %s]",
    SMTP_HOST,
    SMTP_PORT,
    SMTP_USE_TLS,
    SMTP_USE_SSL,
    SMTP_FROM_ADDRESS,
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
    """Raised when an email could not be rendered or sent."""


# --------------------------------------------------------------------------- #
# Template rendering
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
    if not email_address:
        return None
    try:
        normalized_email = validate_email(email_address, check_deliverability=False).normalized
        return normalized_email
    except EmailNotValidError as exc:
        logger.error("Invalid email address %r rejected: %s", email_address, exc)
        return None


# --------------------------------------------------------------------------- #
# SMTP transport
# --------------------------------------------------------------------------- #

def _send_email(
    *,
    to_addresses: List[str],
    cc_addresses: Optional[List[str]],
    subject: str,
    body_html: str,
) -> None:
    if not to_addresses:
        logger.error("Email dispatch attempted without target recipients (To: field is empty)")
        raise EmailDispatchError("no recipient addresses provided")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM_ADDRESS
    msg["To"] = ", ".join(to_addresses)
    if cc_addresses:
        msg["Cc"] = ", ".join(cc_addresses)
    msg.attach(MIMEText(body_html, "html"))

    all_recipients = list(to_addresses) + list(cc_addresses or [])

    logger.info(
        "Attempting SMTP dispatch [Host: %s:%d] | To: %s | CC: %s | Subject: %r",
        SMTP_HOST,
        SMTP_PORT,
        to_addresses,
        cc_addresses or [],
        subject,
    )

    try:
        smtp_cls = smtplib.SMTP_SSL if SMTP_USE_SSL else smtplib.SMTP
        logger.debug("Connecting to SMTP server using protocol class %s", smtp_cls.__name__)
        
        with smtp_cls(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT_SECONDS) as server:
            server.ehlo()
            if SMTP_USE_TLS and not SMTP_USE_SSL:
                logger.debug("Initiating STARTTLS for SMTP session...")
                server.starttls()
                server.ehlo()
            if SMTP_USERNAME and SMTP_PASSWORD:
                logger.debug("Authenticating with SMTP server as user '%s'", SMTP_USERNAME)
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            
            server.sendmail(SMTP_FROM_ADDRESS, all_recipients, msg.as_string())
            logger.info("Email successfully sent via SMTP to %d recipient(s)", len(all_recipients))

    except smtplib.SMTPAuthenticationError as exc:
        logger.error("SMTP authentication failed against %s:%s -> %s", SMTP_HOST, SMTP_PORT, exc)
        raise EmailDispatchError("SMTP authentication failed") from exc
    except smtplib.SMTPRecipientsRefused as exc:
        logger.error("SMTP server refused recipients %s: %s", all_recipients, exc)
        raise EmailDispatchError("recipients refused by SMTP server") from exc
    except smtplib.SMTPConnectError as exc:
        logger.error("Could not connect to SMTP server %s:%s -> %s", SMTP_HOST, SMTP_PORT, exc)
        raise EmailDispatchError("could not connect to SMTP server") from exc
    except smtplib.SMTPException as exc:
        logger.error("SMTP error while sending mail: %s", exc)
        raise EmailDispatchError("SMTP error") from exc
    except (TimeoutError, OSError) as exc:
        logger.error("Network error contacting SMTP host %s:%s -> %s", SMTP_HOST, SMTP_PORT, exc)
        raise EmailDispatchError("network error contacting SMTP server") from exc


# --------------------------------------------------------------------------- #
# Public entry point (called from ping_service.py)
# --------------------------------------------------------------------------- #

def send_alert_notification(payload: Dict[str, Any]) -> None:
    """
    Called (sync) from the ping diagnostic Celery task once a target has been
    unreachable for all batches.

    Given payload = {site_id, alert_id, ping_diagnostic_id, ping_results}:
      1. Looks up the site's primary ISP assignment -> circuit_id + isp_id.
      2. Looks up the ISP's active contact emails.
      3. Renders and sends a single email to the primary ISP contact, CC'ing
         ONLY the internal support team (address from .env).
      4. Persists one EscalationRecord row for the email, and updates
         ALERT_HISTORY.escalation_status accordingly.

    Never raises -- all failures are logged so a bad recipient/template/DB
    hiccup can't crash the worker or block the Celery task chain.
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
            "send_alert_notification called without alert_id for site_id=%s; "
            "escalation cannot be recorded, aborting.",
            site_id,
        )
        return

    # ---  circuit id, isp id, and ISP contact emails -----------------
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

    # Only extract the primary contact email (first in list)
    isp_recipient = _validated(isp_contacts[0].email_address) if isp_contacts else None

    support_recipient = _validated(SUPPORT_TEAM_EMAIL)
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

    # --- single email to primary ISP contact, CC'ing ONLY support team ---
    if not isp_recipient:
        logger.error(
            "No usable active contact email for isp_id=%s (site_id=%s); escalation email not sent.",
            isp_id, site_id,
        )
        return

    # Build CC list with only support_recipient (if available)
    cc_list: List[str] = []
    if support_recipient:
        cc_list.append(support_recipient)

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
    """Render + send one escalation email, then persist the audit trail."""
    logger.debug("Executing escalation handler for alert_id=%s to target '%s'", alert_id, escalated_to)
    try:
        subject = _render_template(subject_template, context).strip()
        body = _render_template(body_template, context)
    except EmailDispatchError:
        logger.error(
            "Aborting %s escalation for alert_id=%s: template rendering failed",
            escalated_to, alert_id,
        )
        return

    email_sent = False
    try:
        _send_email(
            to_addresses=[recipient_email],
            cc_addresses=cc_emails,
            subject=subject,
            body_html=body,
        )
        email_sent = True
        logger.info(
            "Sent %s escalation email for alert_id=%s to %s", escalated_to, alert_id, recipient_email
        )
    except EmailDispatchError:
        logger.exception(
            "Failed to send %s escalation email for alert_id=%s to %s",
            escalated_to, alert_id, recipient_email,
        )

    # Persist the escalation record + update alert_history regardless of
    # send success, so a failed send is still auditable via response_notes.
    try:
        logger.debug("Persisting escalation audit log to database for alert_id=%s...", alert_id)
        with session_scope(SessionLocal) as session:
            escalation_repo = EscalationRecordRepository(session)
            escalation_repo.create(
                alert_id=alert_id,
                escalated_to=escalated_to,
                recipient_email=recipient_email,
                cc_emails=cc_emails,
                email_subject=subject,
                email_body=body,
                response_received=False,
                response_notes=None if email_sent else "Email dispatch failed; see worker logs.",
            )

            alert_repo = AlertHistoryRepository(session)
            alert_repo.update(
                alert_id,
                escalation_status=(
                    f"Escalated to {escalated_to}" if email_sent else f"Escalation to {escalated_to} failed"
                ),
            )
        logger.info(
            "Recorded %s escalation (sent=%s) for alert_id=%s", escalated_to, email_sent, alert_id
        )
    except NotFoundError:
        logger.error("alert_id=%s not found while recording %s escalation", alert_id, escalated_to)
    except DuplicateError as exc:
        logger.error("Duplicate escalation record for alert_id=%s: %s", alert_id, exc)
    except ConstraintViolationError as exc:
        logger.error("Constraint violation recording escalation for alert_id=%s: %s", alert_id, exc)
    except RepositoryError:
        logger.exception("Repository error recording escalation for alert_id=%s", alert_id)
    except Exception:
        logger.exception("Unexpected DB error recording escalation for alert_id=%s", alert_id)


def send_warning_notification(payload: Dict[str, Any]) -> bool:
    support_email = _validated(SUPPORT_TEAM_EMAIL)

    if not support_email:
        logger.error("SUPPORT_TEAM_EMAIL is not configured.")
        return False

    try:
        context = {
            "site_name": payload.get("site_name") or payload.get("device", "Unknown Site"),
            "sensor_name": payload.get("sensor_name") or payload.get("sensor", "Unknown Sensor"),
            "status": payload.get("status", "Warning"),
            "message": payload.get("message") or payload.get("lastvalue", "No message provided"),
            "timestamp": payload.get("timestamp") or payload.get("datetime", ""),
        }

        subject = _render_template(
            "warning_subject.txt",
            context,
        ).replace("\r", "").replace("\n", "").strip()

        body = _render_template(
            "warning_body.html",
            context,
        )

        _send_email(
            to_addresses=[support_email],
            cc_addresses=None,
            subject=subject,
            body_html=body,
        )

        logger.info("Warning email sent successfully to support team.")
        return True

    except (EmailDispatchError, Exception) as exc:
        logger.exception("Failed to send warning email: %s", exc)
        return False


def send_paused_notification(payload: Dict[str, Any]) -> bool:
    support_email = _validated(SUPPORT_TEAM_EMAIL)

    if not support_email:
        logger.error("SUPPORT_TEAM_EMAIL is not configured.")
        return False

    try:
        context = {
            "site_name": payload.get("site_name") or payload.get("device", "Unknown Site"),
            "sensor_name": payload.get("sensor_name") or payload.get("sensor", "Unknown Sensor"),
            "status": payload.get("status", "Paused"),
            "message": payload.get("message") or payload.get("lastvalue", "No message provided"),
            "timestamp": payload.get("timestamp") or payload.get("datetime", ""),
        }

        subject = _render_template(
            "paused_subject.txt",
            context,
        ).replace("\r", "").replace("\n", "").strip()

        body = _render_template(
            "paused_body.html",
            context,
        )

        _send_email(
            to_addresses=[support_email],
            cc_addresses=None,
            subject=subject,
            body_html=body,
        )

        logger.info("Sensor Paused email sent successfully to support team.")
        return True

    except (EmailDispatchError, Exception) as exc:
        logger.exception("Failed to send paused email: %s", exc)
        return False

if __name__ == "__main__":
    from config.logging_config import setup_logging
    
    # Initialize logging if module is run standalone
    setup_logging()
    
    logger.info("Executing email notification module as standalone script...")
    
    payload = {
        "site_id": 2198,
        "alert_id": 105,
        "ping_diagnostic_id": 104,
        "ping_results": {
            "packet_loss_percent": 100,
            "min_rtt_ms": None,
            "avg_rtt_ms": None,
            "max_rtt_ms": None,
        },
    }

    send_alert_notification(payload)