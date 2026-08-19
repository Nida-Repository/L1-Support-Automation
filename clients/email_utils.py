"""Email Utilities for MIME Parsing, Header Decoding, and RFC 5322 Ingestion.

Shared across inbox monitoring, mail reading, task queues, and email dispatch services.
"""
from __future__ import annotations

import base64
import email
from email.header import decode_header
from html.parser import HTMLParser
import io
import logging
import re
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


class _HTMLToTextParser(HTMLParser):
    """Lightweight HTML-to-plain-text stripper using Python standard library."""

    def __init__(self) -> None:
        super().__init__()
        self._pieces: list[str] = []
        self._ignore_stack: int = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        tag_lower = tag.lower()
        if tag_lower in ("script", "style", "head"):
            self._ignore_stack += 1
        elif tag_lower in ("p", "div", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6"):
            self._pieces.append("\n")
        elif tag_lower == "br":
            self._pieces.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag_lower = tag.lower()
        if tag_lower in ("script", "style", "head") and self._ignore_stack > 0:
            self._ignore_stack -= 1
        elif tag_lower in ("p", "div", "tr", "li"):
            self._pieces.append("\n")

    def handle_data(self, data: str) -> None:
        if self._ignore_stack == 0 and data:
            self._pieces.append(data)

    def get_text(self) -> str:
        raw_text = "".join(self._pieces)
        lines = [line.strip() for line in raw_text.splitlines()]
        cleaned = "\n".join(chunk for chunk in lines if chunk)
        return cleaned


def html_to_plain_text(html_content: Optional[str]) -> str:
    """Convert HTML formatted string into readable plain text."""
    if not html_content or not html_content.strip():
        return ""
    try:
        parser = _HTMLToTextParser()
        parser.feed(html_content)
        parser.close()
        return parser.get_text().strip()
    except Exception as exc:
        logger.warning("HTML to text parsing encountered error: %s; falling back to regex stripping", exc)
        clean = re.sub(r"<[^>]+>", " ", html_content)
        return re.sub(r"\s+", " ", clean).strip()


def clean_message_id(header_value: Optional[str]) -> str:
    """Standardize Message-ID or In-Reply-To header string by stripping angle brackets and spaces."""
    if not header_value:
        return ""
    match = re.search(r"<([^>]+)>", header_value)
    if match:
        return match.group(1).strip()
    return header_value.strip().strip("<>").strip()


def extract_references_list(references_header: Optional[str]) -> List[str]:
    """Parse References or In-Reply-To header into a list of normalized Message-IDs."""
    if not references_header:
        return []
    found = re.findall(r"<([^>]+)>", references_header)
    if found:
        return [ref.strip() for ref in found if ref.strip()]
    parts = re.split(r"[\s,]+", references_header.strip())
    return [clean_message_id(p) for p in parts if clean_message_id(p)]


def decode_mime_header(header_value: Optional[str]) -> str:
    """Safely decode encoded MIME email headers into human-readable Unicode strings."""
    if not header_value:
        return ""
    try:
        decoded_parts = decode_header(header_value)
        result = ""
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                result += part.decode(encoding or "utf-8", errors="replace")
            else:
                result += str(part)
        return result.strip()
    except Exception as exc:
        logger.warning("Failed to decode MIME header %r: %s", header_value, exc)
        return str(header_value).strip()


def extract_email_body(msg: email.message.Message) -> str:
    """Extract plain text or HTML body from an email message with charset fallback."""
    plain_text: Optional[str] = None
    html_text: Optional[str] = None

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition") or "")

            if "attachment" in content_disposition.lower():
                continue

            charset = part.get_content_charset() or "utf-8"
            payload = part.get_payload(decode=True)
            if payload is None:
                continue

            if content_type == "text/plain" and plain_text is None:
                plain_text = payload.decode(charset, errors="replace")
            elif content_type == "text/html" and html_text is None:
                html_text = payload.decode(charset, errors="replace")
    else:
        charset = msg.get_content_charset() or "utf-8"
        payload = msg.get_payload(decode=True)
        if payload is not None:
            text = payload.decode(charset, errors="replace")
            if msg.get_content_type() == "text/html":
                html_text = text
            else:
                plain_text = text

    if plain_text and plain_text.strip():
        return plain_text.strip()
    if html_text and html_text.strip():
        return html_to_plain_text(html_text)
    return "[No readable text content found]"


def parse_rfc5322_email(raw_email_bytes: bytes) -> dict[str, Any]:
    """Parse complete RFC 5322 email bytes into structured metadata and attachments."""
    msg = email.message_from_bytes(raw_email_bytes)

    raw_msg_id = msg.get("Message-ID")
    raw_in_reply_to = msg.get("In-Reply-To")
    raw_references = msg.get("References")

    message_id = clean_message_id(raw_msg_id)
    in_reply_to = clean_message_id(raw_in_reply_to) if raw_in_reply_to else None
    references = extract_references_list(raw_references)

    subject = decode_mime_header(msg.get("Subject"))
    sender = decode_mime_header(msg.get("From"))
    receiver = decode_mime_header(msg.get("To"))
    raw_cc = msg.get("Cc")
    cc_decoded = decode_mime_header(raw_cc) if raw_cc else None
    cc_list = [c.strip() for c in cc_decoded.split(",") if c.strip()] if cc_decoded else []

    date_str = msg.get("Date") or msg.get("Delivery-Date") or msg.get("Received") or ""

    plain_text: Optional[str] = None
    html_text: Optional[str] = None
    attachments: list[dict[str, Any]] = []

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition") or "")
            filename = part.get_filename()

            is_attachment = (
                "attachment" in content_disposition.lower()
                or (filename is not None and "inline" not in content_disposition.lower())
            )

            if is_attachment:
                decoded_filename = decode_mime_header(filename) if filename else "unnamed_attachment"
                payload = part.get_payload(decode=True)
                if payload:
                    attachments.append({
                        "file_name": decoded_filename,
                        "content_type": content_type or "application/octet-stream",
                        "file_size": len(payload),
                        "payload_base64": base64.b64encode(payload).decode("ascii"),
                    })
                continue

            charset = part.get_content_charset() or "utf-8"
            payload = part.get_payload(decode=True)
            if payload is None:
                continue

            if content_type == "text/plain" and plain_text is None:
                plain_text = payload.decode(charset, errors="replace")
            elif content_type == "text/html" and html_text is None:
                html_text = payload.decode(charset, errors="replace")
    else:
        charset = msg.get_content_charset() or "utf-8"
        payload = msg.get_payload(decode=True)
        if payload is not None:
            text = payload.decode(charset, errors="replace")
            if msg.get_content_type() == "text/html":
                html_text = text
            else:
                plain_text = text

    if plain_text and plain_text.strip():
        body = plain_text.strip()
    elif html_text and html_text.strip():
        body = html_to_plain_text(html_text)
    else:
        body = "[No readable text content found]"

    return {
        "message_id": message_id,
        "in_reply_to": in_reply_to,
        "references": references,
        "subject": subject,
        "sender": sender,
        "receiver": receiver,
        "cc": cc_list,
        "date": str(date_str),
        "body": body,
        "attachments": attachments,
    }
