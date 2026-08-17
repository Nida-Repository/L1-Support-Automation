"""IMAP Mail Search and Retrieval Utility.

Fetches specific emails by Message-ID header, decodes MIME structures,
and extracts body contents for inspection.
"""
from __future__ import annotations

import email
import imaplib
import logging
import ssl
from typing import Optional

from clients.email_utils import decode_mime_header, extract_email_body
from config.logging_config import setup_logging
from config.settings import settings

setup_logging()
logger = logging.getLogger(__name__)


def fetch_email_by_message_id(target_message_id: str) -> Optional[dict[str, str]]:
    """Fetch an email matching a specific Message-ID header over IMAP SSL."""
    if not settings.imap_server:
        logger.critical("IMAP_SERVER is not configured in settings.")
        return None

    ssl_context = ssl.create_default_context()
    logger.info("Searching for email matching Message-ID: %s", target_message_id)

    try:
        with imaplib.IMAP4_SSL(
            settings.imap_server,
            settings.imap_port,
            ssl_context=ssl_context,
        ) as mail:
            mail.login(settings.email_account, settings.email_password)
            mail.select("INBOX")

            search_criterion = f'HEADER Message-ID "{target_message_id}"'
            status, messages = mail.search(None, search_criterion)

            if status != "OK" or not messages or not messages[0]:
                logger.warning("No email found matching Message-ID: '%s'", target_message_id)
                return None

            email_ids = messages[0].split()
            latest_email_id = email_ids[-1]

            status, msg_data = mail.fetch(latest_email_id, "(RFC822)")
            if status != "OK" or not msg_data:
                logger.error("Failed to fetch message payload for ID: %s", latest_email_id)
                return None

            raw_email = None
            for response_part in msg_data:
                if isinstance(response_part, tuple) and len(response_part) > 1:
                    raw_email = response_part[1]
                    break

            if not raw_email:
                logger.error("Could not extract raw bytes from IMAP message data.")
                return None

            msg = email.message_from_bytes(raw_email)
            subject = decode_mime_header(msg.get("Subject"))
            sender = decode_mime_header(msg.get("From"))
            date = msg.get("Date") or msg.get("Delivery-Date") or msg.get("Received") or "Unknown Date"
            body = extract_email_body(msg)

            logger.info("Successfully fetched email: Subject='%s', From='%s', Date='%s'", subject, sender, date)

            return {
                "message_id": target_message_id,
                "subject": subject,
                "sender": sender,
                "date": str(date),
                "body": body,
            }

    except Exception:
        logger.exception("Error connecting or fetching email with Message-ID: %s", target_message_id)
        return None


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        target_id = sys.argv[1]
        result = fetch_email_by_message_id(target_id)
        if result:
            print(f"Subject: {result['subject']}\nFrom: {result['sender']}\nDate: {result['date']}\n\n{result['body']}")
    else:
        logger.info("Provide a Message-ID as argument: python read_mail.py '<message-id>'")