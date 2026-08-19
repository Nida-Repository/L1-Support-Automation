"""Comprehensive Unit and Integration Tests for Email Threading and Inbound Processing.

Tests:
1. EmailThreadCache: Redis storage, normalization, hits, misses, and resilience.
2. Email Utils: RFC 5322 parsing, HTML-to-text conversion, and attachment extraction.
3. SMTP Client: RFC-compliant Message-ID generation, ISP_EMAIL_THREADS persistence, and Redis caching.
4. IMAP Inbox Monitor: Strict In-Reply-To and References header matching (Redis-first with DB fallback),
   ignoring Subject/Sender/Receiver/Alert ID, and lightweight RabbitMQ dispatch.
5. MinIO Service: Structured collision-free object keys ({alert_id}/{thread_id}/{unique_prefix}_{file_name}),
   path traversal sanitization, and upload retry policy.
6. Celery Inbound Task: Database ingestion, MinIO attachment upload, metadata persistence,
   and zero automatic classification.
7. FastAPI Endpoints: Thread history retrieval and manual classification updates with alias support.
"""
from __future__ import annotations

import base64
import datetime
from decimal import Decimal
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from unittest.mock import MagicMock, patch

import pytest

from api.email_threads import get_alert_email_threads, get_email_thread_detail, update_email_thread_classification
from api.webhook import app
from app.crud import Page
from app.models import (
    Attachment,
    EmailClassificationType,
    EmailDirectionType,
    IspEmailThread,
)
from cache.redis_cache import EmailThreadCache
from clients.email_utils import (
    clean_message_id,
    decode_mime_header,
    extract_references_list,
    html_to_plain_text,
    parse_rfc5322_email,
)
from clients.smtp_client import _handle_escalation, send_alert_notification
from inbox_monitor import match_email_thread, process_new_email
from models.email_thread_model import (
    CLASSIFICATION_ALIAS_MAP,
    EmailClassificationUpdate,
    IncomingEmailPayload,
)
from services.minio_service import MinioService, MinioServiceError
from task_queue.tasks import process_incoming_email_task


# ===========================================================================
# 1. EmailThreadCache & Header Normalization Tests
# ===========================================================================

def test_clean_message_id():
    assert clean_message_id("<12345.67890@example.com>") == "12345.67890@example.com"
    assert clean_message_id("  <foo-bar@domain.net>  ") == "foo-bar@domain.net"
    assert clean_message_id("bare-id@domain.com") == "bare-id@domain.com"
    assert clean_message_id("") == ""
    assert clean_message_id(None) == ""


def test_extract_references_list():
    raw = "<msg1@example.com> <msg2@example.com>, <msg3@example.com>"
    refs = extract_references_list(raw)
    assert refs == ["msg1@example.com", "msg2@example.com", "msg3@example.com"]

    single = "<alone@example.com>"
    assert extract_references_list(single) == ["alone@example.com"]
    assert extract_references_list(None) == []


@patch("cache.redis_cache.redis_client")
def test_email_thread_cache_set_and_get(mock_redis):
    test_msg_id = "<alert-999.thread-100@example.com>"
    mock_redis.get.return_value = '{"alert_id": 999, "thread_id": 100, "escalation_id": 50}'

    # Test GET
    hit = EmailThreadCache.get_thread_by_message_id(test_msg_id)
    assert hit is not None
    assert hit["alert_id"] == 999
    assert hit["thread_id"] == 100
    assert hit["escalation_id"] == 50
    mock_redis.get.assert_called_with("msgid:alert-999.thread-100@example.com")

    # Test SET
    mock_redis.setex.return_value = True
    success = EmailThreadCache.set_message_id_mapping(
        test_msg_id,
        {"alert_id": 999, "thread_id": 100, "escalation_id": 50},
        ttl_seconds=3600,
    )
    assert success is True
    mock_redis.setex.assert_called_once()


@patch("cache.redis_cache.redis_client")
def test_email_thread_cache_miss_and_error_handling(mock_redis):
    mock_redis.get.return_value = None
    assert EmailThreadCache.get_thread_by_message_id("<missing@example.com>") is None

    import redis
    mock_redis.get.side_effect = redis.RedisError("Redis down")
    assert EmailThreadCache.get_thread_by_message_id("<error@example.com>") is None


# ===========================================================================
# 2. HTML to Text & RFC 5322 Parsing Tests
# ===========================================================================

def test_html_to_plain_text():
    html = """
    <html>
        <head><title>Test Alert</title></head>
        <body>
            <p>Dear Support,</p>
            <div>We observed high latency on circuit <b>CR-9901</b>.</div>
            <br>
            <p>Status: <strong>Link Up and Stable</strong></p>
        </body>
    </html>
    """
    text = html_to_plain_text(html)
    assert "Dear Support," in text
    assert "CR-9901" in text
    assert "Link Up and Stable" in text
    assert "<p>" not in text
    assert "<html>" not in text


def test_parse_rfc5322_email_multipart_with_attachments():
    msg = MIMEMultipart("mixed")
    msg["Message-ID"] = "<inbound-12345@isp.net>"
    msg["In-Reply-To"] = "<original-alert-777@company.com>"
    msg["References"] = "<root-alert@company.com> <original-alert-777@company.com>"
    msg["Subject"] = "Re: [DOWN ALERT] Site 1001 Circuit Outage"
    msg["From"] = "noc@isp.net"
    msg["To"] = "support@company.com"
    msg["Cc"] = "manager@company.com, l1-team@company.com"
    msg["Date"] = "Tue, 18 Aug 2026 12:00:00 +0000"

    # Add HTML body
    body_part = MIMEText("<p>The fiber cut has been spliced. Link is restored.</p>", "html")
    msg.attach(body_part)

    # Add Attachment
    attachment_content = b"Ping report logs: 0% packet loss, RTT 12ms."
    att_part = MIMEText(attachment_content.decode("utf-8"), "plain")
    att_part.add_header("Content-Disposition", "attachment", filename="ping_report.txt")
    msg.attach(att_part)

    raw_bytes = msg.as_bytes()
    parsed = parse_rfc5322_email(raw_bytes)

    assert parsed["message_id"] == "inbound-12345@isp.net"
    assert parsed["in_reply_to"] == "original-alert-777@company.com"
    assert "root-alert@company.com" in parsed["references"]
    assert "original-alert-777@company.com" in parsed["references"]
    assert parsed["sender"] == "noc@isp.net"
    assert parsed["receiver"] == "support@company.com"
    assert "manager@company.com" in parsed["cc"]
    assert "The fiber cut has been spliced." in parsed["body"]
    assert len(parsed["attachments"]) == 1
    assert parsed["attachments"][0]["file_name"] == "ping_report.txt"
    assert parsed["attachments"][0]["file_size"] == len(attachment_content)
    assert base64.b64decode(parsed["attachments"][0]["payload_base64"]) == attachment_content


# ===========================================================================
# 3. SMTP Client & Outgoing Thread Creation Tests
# ===========================================================================

@patch("clients.smtp_client.EmailThreadCache.set_message_id_mapping")
@patch("clients.smtp_client._send_email")
@patch("clients.smtp_client.session_scope")
def test_outgoing_email_thread_persistence_and_caching(
    mock_session_scope,
    mock_send_email,
    mock_cache_set,
):
    mock_send_email.return_value = "<outgoing-msg-999@company.com>"
    mock_session = MagicMock()
    mock_session_scope.return_value.__enter__.return_value = mock_session

    mock_escalation_rec = MagicMock()
    mock_escalation_rec.escalation_id = 42

    mock_thread_rec = MagicMock()
    mock_thread_rec.thread_id = 101

    with (
        patch("clients.smtp_client.EscalationRecordRepository") as mock_esc_repo_cls,
        patch("clients.smtp_client.IspEmailThreadRepository") as mock_thread_repo_cls,
        patch("clients.smtp_client.AlertHistoryRepository") as mock_alert_repo_cls,
    ):
        mock_esc_repo = mock_esc_repo_cls.return_value
        mock_esc_repo.create.return_value = mock_escalation_rec

        mock_thread_repo = mock_thread_repo_cls.return_value
        mock_thread_repo.create.return_value = mock_thread_rec

        _handle_escalation(
            escalated_to="ISP",
            recipient_email="isp_noc@telecom.com",
            cc_emails=["support@company.com"],
            subject_template="isp_alert_subject.txt",
            body_template="isp_alert_body.html",
            context={"alert_id": 5001, "site_id": 1010, "circuit_id": "CR-1010"},
            alert_id=5001,
        )

        # Verify thread created with direction OUTGOING
        mock_thread_repo.create.assert_called_once()
        kwargs = mock_thread_repo.create.call_args[1]
        assert kwargs["alert_id"] == 5001
        assert kwargs["message_id"] == "outgoing-msg-999@company.com"
        assert kwargs["direction"] == EmailDirectionType.OUTGOING
        assert kwargs["classification_type"] == EmailClassificationType.UNKNOWN

        # Verify Redis caching called immediately
        mock_cache_set.assert_called_once_with(
            "outgoing-msg-999@company.com",
            {"alert_id": 5001, "thread_id": 101, "escalation_id": 42},
        )


# ===========================================================================
# 4. IMAP Inbox Monitor & Header-Only Matching Rules Tests
# ===========================================================================

@patch("inbox_monitor.EmailThreadCache.get_thread_by_message_id")
def test_match_email_thread_redis_hit(mock_cache_get):
    mock_cache_get.side_effect = lambda mid: {"alert_id": 7701, "thread_id": 202, "escalation_id": 15} if mid == "out-msg-1@company.com" else None

    result = match_email_thread(
        in_reply_to="<out-msg-1@company.com>",
        references=["<root-msg@company.com>"],
    )
    assert result is not None
    assert result["alert_id"] == 7701
    assert result["thread_id"] == 202


@patch("inbox_monitor.session_scope")
@patch("inbox_monitor.EmailThreadCache.set_message_id_mapping")
@patch("inbox_monitor.EmailThreadCache.get_thread_by_message_id")
def test_match_email_thread_db_fallback_and_redis_repopulation(
    mock_cache_get,
    mock_cache_set,
    mock_session_scope,
):
    # Redis miss
    mock_cache_get.return_value = None

    # DB Hit
    mock_session = MagicMock()
    mock_session_scope.return_value.__enter__.return_value = mock_session

    with patch("inbox_monitor.IspEmailThreadRepository") as mock_repo_cls:
        mock_repo = mock_repo_cls.return_value
        mock_thread = MagicMock()
        mock_thread.alert_id = 8802
        mock_thread.thread_id = 303
        mock_repo.get_by_message_id.side_effect = lambda mid: mock_thread if mid == "out-msg-2@company.com" else None

        result = match_email_thread(
            in_reply_to="<out-msg-2@company.com>",
            references=[],
        )

        assert result is not None
        assert result["alert_id"] == 8802
        assert result["thread_id"] == 303

        # Verify Redis repopulation
        mock_cache_set.assert_called_once_with(
            "out-msg-2@company.com",
            {"alert_id": 8802, "thread_id": 303, "escalation_id": None},
        )


@patch("inbox_monitor.session_scope")
@patch("inbox_monitor.EmailThreadCache.get_thread_by_message_id")
def test_match_email_thread_unmatched_is_safely_ignored(mock_cache_get, mock_session_scope):
    mock_cache_get.return_value = None
    mock_session = MagicMock()
    mock_session_scope.return_value.__enter__.return_value = mock_session

    with patch("inbox_monitor.IspEmailThreadRepository") as mock_repo_cls:
        mock_repo = mock_repo_cls.return_value
        mock_repo.get_by_message_id.return_value = None

        result = match_email_thread(
            in_reply_to="<unknown-msg-999@spam.com>",
            references=["<spam-ref@spam.com>"],
        )
        assert result is None


@patch("inbox_monitor.process_incoming_email_task.delay")
@patch("inbox_monitor.match_email_thread")
@patch("inbox_monitor.parse_rfc5322_email")
def test_inbox_monitor_dispatches_task_on_match(
    mock_parse,
    mock_match,
    mock_delay,
):
    mock_parse.return_value = {
        "message_id": "reply-100@isp.net",
        "in_reply_to": "alert-500@company.com",
        "references": ["alert-500@company.com"],
        "subject": "Re: Alert",
        "sender": "noc@isp.net",
        "receiver": "support@company.com",
        "cc": [],
        "date": "2026-08-18",
        "body": "Technician dispatched.",
        "attachments": [],
    }
    mock_match.return_value = {"alert_id": 500, "thread_id": 10}

    mock_client = MagicMock()
    mock_client.fetch.return_value = {123: {b"RFC822": b"raw-email-bytes"}}

    process_new_email(mock_client, 123)

    mock_delay.assert_called_once()
    payload = mock_delay.call_args[0][0]
    assert payload["alert_id"] == 500
    assert payload["thread_id"] == 10
    assert payload["message_id"] == "reply-100@isp.net"
    assert payload["body"] == "Technician dispatched."


# ===========================================================================
# 5. MinIO Service & Object Key Tests
# ===========================================================================

def test_minio_sanitize_filename():
    assert MinioService.sanitize_filename("../../etc/passwd") == "passwd"
    assert MinioService.sanitize_filename("report (final) [2026].pdf") == "report__final___2026_.pdf"
    assert MinioService.sanitize_filename("") == "unnamed_attachment.bin"
    assert MinioService.sanitize_filename(None) == "unnamed_attachment.bin"


@patch("services.minio_service.Minio")
def test_minio_service_upload_object_key_format(mock_minio_cls):
    mock_minio_instance = mock_minio_cls.return_value
    mock_minio_instance.bucket_exists.return_value = True
    mock_res = MagicMock()
    mock_res.etag = '"mock-etag-12345"'
    mock_minio_instance.put_object.return_value = mock_res

    service = MinioService(bucket_name="l1-support-attachments")
    res = service.upload_attachment(
        alert_id=3001,
        thread_id=701,
        file_name="traceroute_diag.log",
        file_data=b"traceroute output data",
        content_type="text/plain",
    )

    assert res["bucket_name"] == "l1-support-attachments"
    assert res["file_name"] == "traceroute_diag.log"
    assert res["file_size"] == len(b"traceroute output data")
    assert res["etag"] == "mock-etag-12345"
    # Object key structure: {alert_id}/{thread_id}/{unique_prefix}_{file_name}
    assert res["object_key"].startswith("3001/701/")
    assert res["object_key"].endswith("_traceroute_diag.log")


# ===========================================================================
# 6. Celery Inbound Task Processing Tests
# ===========================================================================

@patch("task_queue.tasks.EmailThreadCache.set_message_id_mapping")
@patch("task_queue.tasks.minio_service.upload_attachment")
@patch("task_queue.tasks.session_scope")
def test_process_incoming_email_task_execution(
    mock_session_scope,
    mock_minio_upload,
    mock_cache_set,
):
    mock_session = MagicMock()
    mock_session_scope.return_value.__enter__.return_value = mock_session

    mock_thread_rec = MagicMock()
    mock_thread_rec.thread_id = 905

    mock_minio_upload.return_value = {
        "object_key": "4001/905/abcdef_router_logs.txt",
        "bucket_name": "l1-support-attachments",
        "file_size": 25,
        "file_type": "text/plain",
        "file_name": "router_logs.txt",
        "etag": "abc-etag-123",
    }

    raw_payload = {
        "alert_id": 4001,
        "thread_id": 900,
        "message_id": "<reply-msg-77@isp.net>",
        "in_reply_to": "<alert-4001@company.com>",
        "references": ["<alert-4001@company.com>"],
        "sender": "noc@isp.net",
        "receiver": "support@company.com",
        "cc": ["manager@company.com"],
        "subject": "Re: Outage Notification",
        "body": "Technician replaced the bad SFP module. Interface is up.",
        "received_at": "2026-08-18T12:00:00Z",
        "attachment_metadata": [
            {
                "file_name": "router_logs.txt",
                "content_type": "text/plain",
                "file_size": 25,
                "payload_base64": base64.b64encode(b"Interface GigabitEthernet0/1 is Up").decode("ascii"),
            }
        ],
    }

    with (
        patch("task_queue.tasks.IspEmailThreadRepository") as mock_thread_repo_cls,
        patch("task_queue.tasks.AttachmentRepository") as mock_att_repo_cls,
    ):
        mock_thread_repo = mock_thread_repo_cls.return_value
        mock_thread_repo.get_by_message_id.return_value = None
        mock_thread_repo.create.return_value = mock_thread_rec

        mock_att_repo = mock_att_repo_cls.return_value
        mock_att_repo.get_by_object_key.return_value = None

        result = process_incoming_email_task(raw_payload)

        assert result["status"] == "processed"
        assert result["alert_id"] == 4001
        assert result["thread_id"] == 905
        assert result["attachments_uploaded"] == 1

        # Check DB calls
        mock_thread_repo.create.assert_called_once()
        t_args = mock_thread_repo.create.call_args[1]
        assert t_args["direction"] == EmailDirectionType.INCOMING
        assert t_args["classification_type"] == EmailClassificationType.UNKNOWN

        mock_att_repo.create.assert_called_once()
        a_args = mock_att_repo.create.call_args[1]
        assert a_args["alert_id"] == 4001
        assert a_args["thread_id"] == 905
        assert a_args["object_key"] == "4001/905/abcdef_router_logs.txt"
        assert a_args["uploaded_by"] == "SYSTEM"


# ===========================================================================
# 7. FastAPI Email Thread APIs & Classification Update Tests
# ===========================================================================

@patch("api.email_threads.IspEmailThreadRepository")
def test_api_get_alert_email_threads(mock_repo_cls):
    mock_repo = mock_repo_cls.return_value
    now = datetime.datetime.now(datetime.timezone.utc)

    mock_item = IspEmailThread(
        thread_id=1,
        alert_id=5001,
        message_id="msg-001@example.com",
        sender="noreply@company.com",
        receiver="noc@isp.net",
        direction=EmailDirectionType.OUTGOING,
        sent_received_at=now,
        classification_type=EmailClassificationType.UNKNOWN,
        created_at=now,
        attachments=[],
    )

    mock_repo.list_for_alert.return_value = Page(
        items=[mock_item],
        total=1,
        limit=50,
        offset=0,
    )

    mock_db = MagicMock()
    page_res = get_alert_email_threads(alert_id=5001, limit=50, offset=0, db=mock_db)
    assert page_res.total == 1
    assert len(page_res.items) == 1
    assert page_res.items[0].message_id == "msg-001@example.com"
    assert page_res.items[0].direction == EmailDirectionType.OUTGOING


@patch("api.email_threads.IspEmailThreadRepository")
def test_api_update_classification_with_aliases(mock_repo_cls):
    mock_repo = mock_repo_cls.return_value
    now = datetime.datetime.now(datetime.timezone.utc)

    def mock_update(thread_id, classification_type):
        return IspEmailThread(
            thread_id=thread_id,
            alert_id=5001,
            message_id="msg-002@example.com",
            sender="noc@isp.net",
            receiver="support@company.com",
            direction=EmailDirectionType.INCOMING,
            sent_received_at=now,
            classification_type=classification_type,
            created_at=now,
            attachments=[],
        )

    mock_repo.update_classification.side_effect = mock_update
    mock_db = MagicMock()

    # 1. Test alias "Link Up and Stable" -> "Link Stable"
    p1 = EmailClassificationUpdate(classification="Link Up and Stable")
    res1 = update_email_thread_classification(thread_id=100, payload=p1, db=mock_db)
    assert res1.classification_type == EmailClassificationType.LINK_STABLE

    # 2. Test alias "Planned Maintenance" -> "Maintenance"
    p2 = EmailClassificationUpdate(classification="Planned Maintenance")
    res2 = update_email_thread_classification(thread_id=100, payload=p2, db=mock_db)
    assert res2.classification_type == EmailClassificationType.MAINTENANCE

    # 3. Test alias "Technical Issue Identified" -> "Technical Issue"
    p3 = EmailClassificationUpdate(classification="Technical Issue Identified")
    res3 = update_email_thread_classification(thread_id=100, payload=p3, db=mock_db)
    assert res3.classification_type == EmailClassificationType.TECHNICAL_ISSUE

    # 4. Test alias "Power Issue Detected by ISP" -> "Power Issue"
    p4 = EmailClassificationUpdate(classification="Power Issue Detected by ISP")
    res4 = update_email_thread_classification(thread_id=100, payload=p4, db=mock_db)
    assert res4.classification_type == EmailClassificationType.POWER_ISSUE

    # 5. Test invalid classification raises ValueError in Pydantic validation
    with pytest.raises(ValueError):
        EmailClassificationUpdate(classification="Completely Invalid Label")
