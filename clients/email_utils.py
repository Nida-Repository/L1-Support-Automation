"""Email Utilities for MIME Parsing and Header Decoding.

Shared across inbox monitoring, mail reading, and email dispatch services.
"""
from __future__ import annotations

import email
from email.header import decode_header
from typing import Optional


def decode_mime_header(header_value: Optional[str]) -> str:
    """Safely decode encoded MIME email headers into human-readable Unicode strings."""
    if not header_value:
        return ""
    decoded_parts = decode_header(header_value)
    result = ""
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            result += part.decode(encoding or "utf-8", errors="replace")
        else:
            result += str(part)
    return result.strip()


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

    body = plain_text or html_text
    return body.strip() if body else "[No readable text content found]"
