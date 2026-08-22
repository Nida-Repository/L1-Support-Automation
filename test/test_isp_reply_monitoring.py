"""Tests for ISP Reply Monitoring and Automated Reminder Workflow."""
import datetime
import json
from unittest.mock import MagicMock, patch
import pytest

from cache.redis_cache import IspReplyMonitor
from services.isp_monitor_service import IspReplyMonitorService


@patch("cache.redis_cache.redis_client")
def test_isp_reply_monitor_registration_and_retrieval(mock_redis):
    """Test registering a monitor in Redis and retrieving its state."""
    msg_id = "test-alert-101@domain.com"
    alert_id = 9901
    escalation_id = 8801

    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe

    registered = IspReplyMonitor.register_monitor(
        alert_id=alert_id,
        escalation_id=escalation_id,
        message_id=msg_id,
        isp_email="noc@isp.com",
        isp_email_id=123,
        sensor_name="Router Sensor",
        site_name="DataCenter East",
        isp_name="Telecom ISP",
        circuit_id="CIRC-999",
        original_subject="Ticket ID 9901 - Network Outage Alert",
        to_addresses=["noc@isp.com"],
        cc_addresses=["support@company.com"],
        timeout_minutes=60,
    )

    assert registered is True
    assert mock_pipe.setex.call_count == 2
    assert mock_pipe.sadd.call_count == 1
    mock_pipe.execute.assert_called_once()

    # Test GET
    fake_state = {
        "alert_id": alert_id,
        "escalation_id": escalation_id,
        "message_id": msg_id,
        "reminder_count": 0,
        "response_received": False,
        "monitoring_active": True,
    }
    mock_redis.get.return_value = json.dumps(fake_state)

    monitor = IspReplyMonitor.get_monitor(msg_id)
    assert monitor is not None
    assert monitor["alert_id"] == alert_id
    assert monitor["message_id"] == msg_id
    assert monitor["reminder_count"] == 0
    assert monitor["response_received"] is False
    assert monitor["monitoring_active"] is True


@patch("cache.redis_cache.redis_client")
def test_isp_reply_monitor_response_received_matching(mock_redis):
    """Test that incoming replies match and cancel monitoring in Redis immediately."""
    msg_id = "test-alert-102@domain.com"
    reply_msg_id = "reply-from-isp-202@isp.com"
    alert_id = 9902

    fake_state = {
        "alert_id": alert_id,
        "escalation_id": 8802,
        "message_id": msg_id,
        "reminder_count": 0,
        "response_received": False,
        "monitoring_active": True,
    }
    mock_redis.get.return_value = json.dumps(fake_state)
    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe

    # Simulate inbound reply with In-Reply-To matching original message
    res = IspReplyMonitor.mark_response_received(
        message_id=reply_msg_id,
        in_reply_to=msg_id,
        references=[msg_id],
        alert_id=alert_id,
    )

    assert res is not None
    assert res["response_received"] is True
    assert res["monitoring_active"] is False
    mock_pipe.srem.assert_called_once_with(IspReplyMonitor.INDEX_KEY, msg_id)
    mock_pipe.execute.assert_called_once()


@patch("cache.redis_cache.redis_client")
@patch("services.isp_monitor_service.session_scope")
@patch("services.isp_monitor_service._send_threaded_email")
def test_isp_reply_monitor_scan_due_and_reminder_dispatch(mock_send, mock_scope, mock_redis):
    """Test scan logic when reminder is due."""
    msg_id = "test-alert-103@domain.com"
    alert_id = 9903
    mock_send.return_value = "reminder-1-msg-id@domain.com"

    mock_redis.set.return_value = True  # lock acquired
    mock_redis.smembers.return_value = {msg_id}

    past_time = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=10)).isoformat()
    fake_state = {
        "alert_id": alert_id,
        "escalation_id": 8803,
        "message_id": msg_id,
        "isp_email": "noc@isp.com",
        "isp_email_id": 125,
        "sensor_name": "Firewall",
        "site_name": "Branch 3",
        "isp_name": "Speed ISP",
        "circuit_id": "CIRC-1003",
        "original_subject": "Ticket ID 9903 - Network Outage Alert - Circuit CIRC-1003",
        "reminder_count": 0,
        "response_received": False,
        "monitoring_active": True,
        "next_reminder_time": past_time,
        "original_references": [],
        "to_addresses": ["noc@isp.com"],
        "cc_addresses": [],
    }

    mock_pipe = MagicMock()
    mock_pipe.execute.return_value = [json.dumps(fake_state)]
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.get.return_value = json.dumps(fake_state)
    mock_redis.ttl.return_value = 604800

    IspReplyMonitorService.run_scan()

    assert mock_send.called
    call_kwargs = mock_send.call_args.kwargs
    assert call_kwargs["in_reply_to"] == msg_id
    assert "[REMINDER #1]" in call_kwargs["subject"]
    assert "CIRC-1003" in call_kwargs["subject"]


@patch("cache.redis_cache.redis_client")
@patch("services.isp_monitor_service.session_scope")
def test_isp_reply_to_reminder_email_matching(mock_session_scope, mock_redis):
    """Test when an ISP replies directly to Reminder #2 instead of original email.

    Ensures:
    1. References containing original message_id or reminder IDs match the alert.
    2. Monitoring is cancelled immediately in Redis.
    3. ReminderHistory rows are updated with response_received=True.
    """
    orig_msg_id = "original-alert-9904@company.com"
    reminder_1_id = "reminder-1-9904@company.com"
    reminder_2_id = "reminder-2-9904@company.com"
    isp_reply_id = "isp-reply-to-reminder2@isp.com"
    alert_id = 9904

    fake_state = {
        "alert_id": alert_id,
        "escalation_id": 8804,
        "message_id": orig_msg_id,
        "reminder_count": 2,
        "response_received": False,
        "monitoring_active": True,
        "original_references": [reminder_1_id, reminder_2_id],
    }

    # When querying for original ID from references chain:
    mock_redis.get.side_effect = lambda key: json.dumps(fake_state) if (orig_msg_id in key or reminder_2_id in key) else None
    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe

    mock_session = MagicMock()
    mock_session_scope.return_value.__enter__.return_value = mock_session

    with patch("services.isp_monitor_service.ReminderHistoryRepository") as mock_repo_cls:
        mock_repo = mock_repo_cls.return_value
        mock_reminder = MagicMock()
        mock_reminder.reminder_id = 501
        mock_reminder.reminder_number = 2
        mock_reminder.response_received = False
        mock_repo.list_for_alert.return_value.items = [mock_reminder]

        # ISP replies to reminder #2: In-Reply-To is reminder_2_id, References has [orig_msg_id, reminder_1_id, reminder_2_id]
        IspReplyMonitorService.handle_reply_received(
            alert_id=alert_id,
            message_id=isp_reply_id,
            in_reply_to=reminder_2_id,
            references=[orig_msg_id, reminder_1_id, reminder_2_id],
        )

        # Verify Redis state was marked as responded
        mock_pipe.setex.assert_called()
        mock_pipe.srem.assert_called_with(IspReplyMonitor.INDEX_KEY, orig_msg_id)

        # Verify DB ReminderHistory was marked as responded
        mock_repo.mark_responded.assert_called_once()
        assert mock_repo.mark_responded.call_args[0][0] == 501

