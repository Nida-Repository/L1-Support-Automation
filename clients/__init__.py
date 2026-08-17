"""Clients Package."""
from clients.email_utils import decode_mime_header, extract_email_body
from clients.smtp_client import (
    EmailDispatchError,
    send_alert_notification,
    send_paused_notification,
    send_warning_notification,
)

__all__ = [
    "decode_mime_header",
    "extract_email_body",
    "EmailDispatchError",
    "send_alert_notification",
    "send_warning_notification",
    "send_paused_notification",
]
