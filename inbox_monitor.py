"""IMAP Inbox Monitoring Daemon.

Monitors incoming email via IMAP IDLE, decodes message headers, extracts text/html
body payloads, and triggers alert processing workflows.
"""
from __future__ import annotations

import email
import logging
import sys
import time

from imapclient import IMAPClient
from imapclient.exceptions import IMAPClientError

from clients.email_utils import decode_mime_header, extract_email_body
from config.logging_config import setup_logging
from config.settings import settings

setup_logging()
logger = logging.getLogger(__name__)


def process_new_email(client: IMAPClient, uid: int) -> None:
    """Fetch, parse, and log details for a given email UID."""
    try:
        raw_response = client.fetch([uid], ["RFC822"])
        if uid not in raw_response:
            logger.warning("UID %d not found in IMAP response payload", uid)
            return

        raw_email = raw_response[uid][b"RFC822"]
        msg = email.message_from_bytes(raw_email)

        subject = decode_mime_header(msg.get("Subject"))
        sender = decode_mime_header(msg.get("From"))
        date = msg.get("Date")
        body = extract_email_body(msg)

        logger.info(
            "New Email Received (UID: %d) | From: %s | Subject: %s | Date: %s | BodyPreview: %.100s...",
            uid,
            sender,
            subject,
            date,
            body.replace("\n", " "),
        )
    except Exception:
        logger.exception("Error processing email UID %d", uid)


def monitor_inbox() -> None:
    """Connect to IMAP server and monitor the INBOX indefinitely using IDLE."""
    if not settings.imap_server:
        logger.critical("IMAP_SERVER is not configured in settings. Aborting monitoring daemon.")
        return

    logger.info(
        "Starting Inbox Monitor on host %s:%d (account: %s)...",
        settings.imap_server,
        settings.imap_port,
        settings.email_account or "[NOT SET]",
    )

    reconnect_delay = 5.0
    max_reconnect_delay = 60.0

    while True:
        try:
            logger.info("Connecting to IMAP server %s:%d...", settings.imap_server, settings.imap_port)
            with IMAPClient(
                settings.imap_server,
                port=settings.imap_port,
                use_uid=True,
                ssl=True,
            ) as client:
                client.login(settings.email_account, settings.email_password)
                client.select_folder("INBOX")
                logger.info("Connected to INBOX! Monitoring for new messages via IDLE...")
                reconnect_delay = 5.0  # reset on successful connection

                while True:
                    client.idle()
                    events = client.idle_check(timeout=600)
                    client.idle_done()

                    if events:
                        for event in events:
                            if len(event) > 1 and event[1] == b"EXISTS":
                                messages = client.search(["UNSEEN"])
                                for uid in messages:
                                    process_new_email(client, uid)

        except (KeyboardInterrupt, SystemExit):
            logger.info("Inbox monitoring stopped by user/system signal.")
            sys.exit(0)
        except (IMAPClientError, OSError) as exc:
            logger.error(
                "IMAP network/protocol error: %s. Reconnecting in %.1fs...",
                exc,
                reconnect_delay,
            )
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)
        except Exception:
            logger.exception(
                "Unexpected error in inbox monitor loop. Reconnecting in %.1fs...",
                reconnect_delay,
            )
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)


if __name__ == "__main__":
    monitor_inbox()