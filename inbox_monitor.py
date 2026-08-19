"""IMAP Inbox Monitoring Daemon.

Monitors incoming email via IMAP IDLE, extracts RFC 5322 headers, matches email threads
strictly against In-Reply-To and References via Redis (with DB fallback), and dispatches
lightweight ingestion tasks to RabbitMQ / Celery.
"""
from __future__ import annotations

import logging
import sys
import time
from typing import Any, List, Optional

from imapclient import IMAPClient
from imapclient.exceptions import IMAPClientError

from app.crud import IspEmailThreadRepository, session_scope
from app.database import SessionLocal
from cache.redis_cache import EmailThreadCache
from clients.email_utils import clean_message_id, parse_rfc5322_email
from config.logging_config import setup_logging
from config.settings import settings
from task_queue.tasks import process_incoming_email_task

setup_logging()
logger = logging.getLogger(__name__)


def match_email_thread(
    in_reply_to: Optional[str],
    references: List[str],
) -> Optional[dict[str, Any]]:
    """Match an incoming email to an existing alert/thread using In-Reply-To and References headers.

    Matching Algorithm:
    1. Check Redis cache first for In-Reply-To and all References IDs.
    2. If found in Redis: use Redis data immediately without querying PostgreSQL.
    3. If Redis misses: query PostgreSQL via IspEmailThreadRepository.get_by_message_id().
       If found in PostgreSQL, repopulate Redis cache.
    4. If no match exists: return None.
    5. NEVER match using Subject, Sender, Receiver, or Alert ID.
    """
    candidate_ids: list[str] = []
    if in_reply_to:
        clean_irt = clean_message_id(in_reply_to)
        if clean_irt:
            candidate_ids.append(clean_irt)

    for ref in references:
        clean_ref = clean_message_id(ref)
        if clean_ref and clean_ref not in candidate_ids:
            candidate_ids.append(clean_ref)

    if not candidate_ids:
        logger.debug("Incoming email has no In-Reply-To or References headers; cannot match thread.")
        return None

    # Step 1: Check Redis first
    for msg_id in candidate_ids:
        cached_info = EmailThreadCache.get_thread_by_message_id(msg_id)
        if cached_info and cached_info.get("alert_id"):
            logger.info(
                "Thread match found via Redis cache [Header ID: %s | Alert ID: %s | Thread ID: %s]",
                msg_id,
                cached_info.get("alert_id"),
                cached_info.get("thread_id"),
            )
            return cached_info

    # Step 2: Fallback to PostgreSQL database lookup
    logger.debug("Redis cache miss for candidate IDs %s; querying PostgreSQL...", candidate_ids)
    try:
        with session_scope(SessionLocal) as session:
            thread_repo = IspEmailThreadRepository(session)
            for msg_id in candidate_ids:
                thread = thread_repo.get_by_message_id(msg_id)
                if thread:
                    logger.info(
                        "Thread match found via PostgreSQL [Header ID: %s | Alert ID: %s | Thread ID: %s]",
                        msg_id,
                        thread.alert_id,
                        thread.thread_id,
                    )
                    thread_data = {
                        "alert_id": thread.alert_id,
                        "thread_id": thread.thread_id,
                        "escalation_id": None,
                    }
                    # Repopulate Redis cache for faster subsequent lookups
                    EmailThreadCache.set_message_id_mapping(msg_id, thread_data)
                    return thread_data
    except Exception as db_exc:
        logger.exception("Database error while checking email thread matching: %s", db_exc)

    return None


def process_new_email(client: IMAPClient, uid: int) -> None:
    """Fetch, parse, match, and publish an incoming email event."""
    try:
        raw_response = client.fetch([uid], ["RFC822"])
        if uid not in raw_response:
            logger.warning("UID %d not found in IMAP fetch response payload", uid)
            return

        raw_email_bytes = raw_response[uid][b"RFC822"]
        parsed_email = parse_rfc5322_email(raw_email_bytes)

        msg_id = parsed_email["message_id"] or f"UID-{uid}"
        in_reply_to = parsed_email["in_reply_to"]
        references = parsed_email["references"]
        subject = parsed_email["subject"]
        sender = parsed_email["sender"]

        logger.info(
            "Parsed Inbound Email [UID: %d | Message-ID: %s | In-Reply-To: %s | Refs: %d | From: %s | Subject: %r | Attachments: %d]",
            uid,
            msg_id,
            in_reply_to,
            len(references),
            sender,
            subject,
            len(parsed_email["attachments"]),
        )

        # Thread Matching
        matched_context = match_email_thread(in_reply_to, references)
        if not matched_context:
            logger.info(
                "Unmatched incoming email [UID: %d | Message-ID: %s | In-Reply-To: %s | Subject: %r] — safely ignored.",
                uid,
                msg_id,
                in_reply_to,
                subject,
            )
            return

        alert_id = matched_context["alert_id"]
        thread_id = matched_context.get("thread_id")

        # Construct lightweight task payload for Celery / RabbitMQ
        task_payload = {
            "alert_id": alert_id,
            "thread_id": thread_id,
            "message_id": msg_id,
            "in_reply_to": in_reply_to,
            "references": references,
            "sender": sender,
            "receiver": parsed_email["receiver"],
            "cc": parsed_email["cc"],
            "subject": subject,
            "body": parsed_email["body"],
            "received_at": parsed_email["date"],
            "attachment_metadata": parsed_email["attachments"],
        }

        # Publish to RabbitMQ
        process_incoming_email_task.delay(task_payload)
        logger.info(
            "Published inbound email task to RabbitMQ [Alert ID: %s | Message-ID: %s | Attachments: %d]",
            alert_id,
            msg_id,
            len(parsed_email["attachments"]),
        )

    except Exception as exc:
        logger.exception("Unexpected error processing email UID %d: %s", uid, exc)


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

                # Process any unread messages waiting on startup
                unseen = client.search(["UNSEEN"])
                if unseen:
                    logger.info("Processing %d pending unseen email(s) on initial connect...", len(unseen))
                    for uid in unseen:
                        process_new_email(client, uid)

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