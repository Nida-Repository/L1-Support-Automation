"""Comprehensive Production-Readiness Unit Tests.

Tests configuration management, secret masking, email utilities, processor helpers,
repository abstractions, workflow processors, and webhook endpoints without
connecting to or accessing live production database data.
"""
from __future__ import annotations

import email
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.webhook import app
from clients.email_utils import decode_mime_header, extract_email_body
from config.settings import Settings, mask_secret, mask_url_password
from models.alert_history_model import AlertHistoryCreate, AlertHistoryRead
from models.prtg_alert import PRTGWebhookPayload, SensorStatus
from models.sensor_log_model import LogLevel, LogStatus, SensorLogCreate
from processors.base import (
    extract_field,
    extract_sensor_id,
    sanitize_status,
    serialize_payload_for_json,
)
from processors.down_processor import DownWorkflow
from processors.paused_processor import PausedWorkflow
from processors.unusual_processor import UnusualWorkflow
from processors.up_processor import UpWorkflow
from processors.warning_processor import WarningWorkflow


# ===========================================================================
# 1. Settings & Secret Masking Tests
# ===========================================================================

def test_mask_secret():
    assert mask_secret(None) == "[NOT SET]"
    assert mask_secret("") == "[NOT SET]"
    assert mask_secret("short") == "******"
    assert mask_secret("my_super_secret_password", visible_chars=3) == "my_******ord"


def test_mask_url_password():
    assert mask_url_password(None) == "[NOT SET]"
    db_url = "postgresql+psycopg://user:super_secret@db.host.internal:5432/mydb"
    masked = mask_url_password(db_url)
    assert "super_secret" not in masked
    assert "******" in masked
    assert "db.host.internal:5432/mydb" in masked

    redis_url = "redis://:redis_password123@redis.host:6379/0"
    masked_redis = mask_url_password(redis_url)
    assert "redis_password123" not in masked_redis
    assert "******" in masked_redis


def test_settings_defaults():
    s = Settings(database_url="postgresql+psycopg://postgres:pass@localhost:5432/db")
    assert s.db_pool_size >= 1
    assert s.redis_cache_ttl_seconds > 0
    assert "pass" not in s.safe_database_url


# ===========================================================================
# 2. Email Utilities Tests
# ===========================================================================

def test_decode_mime_header():
    assert decode_mime_header(None) == ""
    assert decode_mime_header("Simple Subject") == "Simple Subject"
    encoded = "=?utf-8?b?VGVzdCBTdWJqZWN0?="
    assert decode_mime_header(encoded) == "Test Subject"


def test_extract_email_body_plain():
    raw_msg = (
        "From: sender@example.com\r\n"
        "To: recv@example.com\r\n"
        "Subject: Test\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n\r\n"
        "This is the plain body text."
    )
    msg = email.message_from_string(raw_msg)
    body = extract_email_body(msg)
    assert body == "This is the plain body text."


def test_extract_email_body_multipart():
    raw_msg = (
        "From: sender@example.com\r\n"
        "To: recv@example.com\r\n"
        "Subject: Multipart Test\r\n"
        "Content-Type: multipart/alternative; boundary=\"boundary123\"\r\n\r\n"
        "--boundary123\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n\r\n"
        "Plain alternative text.\r\n"
        "--boundary123\r\n"
        "Content-Type: text/html; charset=utf-8\r\n\r\n"
        "<p>HTML alternative text.</p>\r\n"
        "--boundary123--"
    )
    msg = email.message_from_string(raw_msg)
    body = extract_email_body(msg)
    assert body == "Plain alternative text."


# ===========================================================================
# 3. Processor Helper Tests
# ===========================================================================

def test_extract_sensor_id():
    assert extract_sensor_id({"sensor_id": 1234}) == 1234
    assert extract_sensor_id({"sensorid": "5678"}) == 5678
    assert extract_sensor_id(None) is None
    assert extract_sensor_id({"sensor_id": "invalid"}) is None


def test_extract_field():
    payload = {"status": "Warning", "message": "High latency"}
    assert extract_field(payload, "status") == "Warning"
    assert extract_field(payload, "missing", default="Fallback") == "Fallback"


def test_sanitize_status():
    assert sanitize_status(SensorStatus.DOWN) == "Down"
    assert sanitize_status("SensorStatus.WARNING") == "WARNING"
    assert sanitize_status(None, default="Paused") == "Paused"


def test_serialize_payload_for_json():
    class DummyModel:
        def model_dump(self, **kwargs):
            return {"key": "value"}

    serialized = serialize_payload_for_json(DummyModel())
    assert serialized == {"key": "value"}
    assert serialize_payload_for_json({"a": 1}) == {"a": 1}


# ===========================================================================
# 4. Pydantic Schemas Validation Tests
# ===========================================================================

def test_prtg_webhook_payload():
    raw = {
        "sensorid": "1042",
        "sensorname": "Ping Gateway",
        "laststatus": "Down",
        "message": "Timeout occurred",
    }
    payload = PRTGWebhookPayload.model_validate(raw)
    assert payload.sensor_id == 1042
    assert payload.sensor_name == "Ping Gateway"
    assert payload.status == SensorStatus.DOWN
    assert payload.message == "Timeout occurred"


def test_sensor_log_schema():
    raw = {
        "sensor_id": 1050,
        "log_level": "CRITICAL",
        "log_status": "opened",
        "log_message": "Circuit offline",
    }
    log_create = SensorLogCreate.model_validate(raw)
    assert log_create.sensor_id == 1050
    assert log_create.log_level == LogLevel.CRITICAL
    assert log_create.log_status == LogStatus.OPENED


def test_alert_history_schema():
    raw = {
        "sensor_id": 1050,
        "state_id": 1001,
        "alert_message": "Fiber cut detected",
    }
    alert_create = AlertHistoryCreate.model_validate(raw)
    assert alert_create.sensor_id == 1050
    assert alert_create.state_id == 1001


# ===========================================================================
# 5. Workflow Execution Tests (Mocked DB & Network)
# ===========================================================================

@pytest.mark.asyncio
@patch("processors.down_processor.session_scope")
@patch("processors.down_processor.PingIp.execute", new_callable=AsyncMock)
async def test_down_workflow_execution(mock_ping_exec, mock_session_scope):
    mock_session = MagicMock()
    mock_session_scope.return_value.__enter__.return_value = mock_session

    mock_sensor = MagicMock(sensor_id=1042, site_isp_assignment_id=2001)
    mock_assignment = MagicMock(assignment_id=2001, site_id=3001)
    mock_site = MagicMock(site_id=3001, primary_ip="192.168.1.1")
    mock_state = MagicMock(state_id=101)
    mock_alert = MagicMock(alert_id=501)

    with patch("processors.down_processor.SensorRepository") as mock_sensor_repo, \
         patch("processors.down_processor.SiteIspAssignmentRepository") as mock_assign_repo, \
         patch("processors.down_processor.SiteRepository") as mock_site_repo, \
         patch("processors.down_processor.AlertStateRepository") as mock_state_repo, \
         patch("processors.down_processor.AlertHistoryRepository") as mock_alert_repo, \
         patch("processors.down_processor.SensorLogRepository") as mock_log_repo:

        mock_sensor_repo.return_value.get.return_value = mock_sensor
        mock_assign_repo.return_value.get.return_value = mock_assignment
        mock_site_repo.return_value.get.return_value = mock_site
        mock_state_repo.return_value.get_by_name.return_value = mock_state
        mock_alert_repo.return_value.create.return_value = mock_alert
        mock_ping_exec.return_value = {"packet_loss_percent": Decimal("100.00")}

        workflow = DownWorkflow()
        await workflow.execute({"sensor_id": 1042})

        mock_ping_exec.assert_called_once()
        mock_log_repo.return_value.create.assert_called_once()


@pytest.mark.asyncio
@patch("processors.paused_processor.session_scope")
@patch("processors.paused_processor.send_paused_notification", return_value=True)
async def test_paused_workflow_execution(mock_send_email, mock_session_scope):
    mock_session = MagicMock()
    mock_session_scope.return_value.__enter__.return_value = mock_session

    mock_sensor = MagicMock(sensor_id=1042, sensor_name="Core Ping", site_isp_assignment_id=2001)
    mock_assignment = MagicMock(assignment_id=2001, site_id=3001)
    mock_site = MagicMock(site_id=3001, site_name="Primary DC")
    mock_state = MagicMock(state_id=102)
    mock_alert = MagicMock(alert_id=502)

    with patch("processors.paused_processor.SensorRepository") as mock_sensor_repo, \
         patch("processors.paused_processor.SiteIspAssignmentRepository") as mock_assign_repo, \
         patch("processors.paused_processor.SiteRepository") as mock_site_repo, \
         patch("processors.paused_processor.AlertStateRepository") as mock_state_repo, \
         patch("processors.paused_processor.AlertHistoryRepository") as mock_alert_repo, \
         patch("processors.paused_processor.EscalationRecordRepository") as mock_esc_repo, \
         patch("processors.paused_processor.SensorLogRepository") as mock_log_repo:

        mock_sensor_repo.return_value.get.return_value = mock_sensor
        mock_assign_repo.return_value.get.return_value = mock_assignment
        mock_site_repo.return_value.get.return_value = mock_site
        mock_state_repo.return_value.get_by_name.return_value = mock_state
        mock_alert_repo.return_value.create.return_value = mock_alert

        workflow = PausedWorkflow()
        await workflow.execute({"sensor_id": 1042, "status": "Paused"})

        mock_send_email.assert_called_once()
        mock_esc_repo.return_value.create.assert_called_once()
        mock_log_repo.return_value.create.assert_called_once()


@pytest.mark.asyncio
@patch("processors.warning_processor.session_scope")
@patch("processors.warning_processor.send_warning_notification", return_value=True)
async def test_warning_workflow_execution(mock_send_email, mock_session_scope):
    mock_session = MagicMock()
    mock_session_scope.return_value.__enter__.return_value = mock_session

    mock_sensor = MagicMock(sensor_id=1042, sensor_name="Core Ping", site_isp_assignment_id=2001)
    mock_assignment = MagicMock(assignment_id=2001, site_id=3001)
    mock_site = MagicMock(site_id=3001, site_name="Primary DC")
    mock_state = MagicMock(state_id=103)
    mock_alert = MagicMock(alert_id=503)

    with patch("processors.warning_processor.SensorRepository") as mock_sensor_repo, \
         patch("processors.warning_processor.SiteIspAssignmentRepository") as mock_assign_repo, \
         patch("processors.warning_processor.SiteRepository") as mock_site_repo, \
         patch("processors.warning_processor.AlertStateRepository") as mock_state_repo, \
         patch("processors.warning_processor.AlertHistoryRepository") as mock_alert_repo, \
         patch("processors.warning_processor.EscalationRecordRepository") as mock_esc_repo, \
         patch("processors.warning_processor.SensorLogRepository") as mock_log_repo:

        mock_sensor_repo.return_value.get.return_value = mock_sensor
        mock_assign_repo.return_value.get.return_value = mock_assignment
        mock_site_repo.return_value.get.return_value = mock_site
        mock_state_repo.return_value.get_by_name.return_value = mock_state
        mock_alert_repo.return_value.create.return_value = mock_alert

        workflow = WarningWorkflow()
        await workflow.execute({"sensor_id": 1042, "status": "Warning", "message": "High packet loss"})

        mock_send_email.assert_called_once()
        mock_esc_repo.return_value.create.assert_called_once()
        mock_log_repo.return_value.create.assert_called_once()


@pytest.mark.asyncio
@patch("processors.up_processor.session_scope")
async def test_up_workflow_execution(mock_session_scope):
    mock_session = MagicMock()
    mock_session_scope.return_value.__enter__.return_value = mock_session

    mock_sensor = MagicMock(sensor_id=1042)
    mock_open_alert = MagicMock(alert_id=501, resolved_at=None)

    with patch("processors.up_processor.SensorRepository") as mock_sensor_repo, \
         patch("processors.up_processor.AlertHistoryRepository") as mock_alert_repo, \
         patch("processors.up_processor.SensorLogRepository") as mock_log_repo:

        mock_sensor_repo.return_value.get.return_value = mock_sensor
        mock_log_repo.return_value.close_open_logs.return_value = 1
        mock_alert_repo.return_value.list_for_sensor.return_value = [mock_open_alert]

        workflow = UpWorkflow()
        await workflow.execute({"sensor_id": 1042, "status": "Up"})

        mock_log_repo.return_value.close_open_logs.assert_called_once_with(1042)
        mock_alert_repo.return_value.resolve.assert_called_once()
        mock_log_repo.return_value.create.assert_called_once()


@pytest.mark.asyncio
@patch("processors.unusual_processor.session_scope")
async def test_unusual_workflow_execution(mock_session_scope):
    mock_session = MagicMock()
    mock_session_scope.return_value.__enter__.return_value = mock_session

    mock_sensor = MagicMock(sensor_id=1042)

    with patch("processors.unusual_processor.SensorRepository") as mock_sensor_repo, \
         patch("processors.unusual_processor.SensorLogRepository") as mock_log_repo:

        mock_sensor_repo.return_value.get.return_value = mock_sensor

        workflow = UnusualWorkflow()
        await workflow.execute({"sensor_id": 1042, "status": "Unusual", "message": "Jitter spike"})

        mock_log_repo.return_value.create.assert_called_once()


# ===========================================================================
# 6. Webhook Endpoint Direct Unit Tests
# ===========================================================================

@pytest.mark.asyncio
async def test_authenticate_prtg_invalid():
    from fastapi import HTTPException
    from api.webhook import authenticate_prtg

    with pytest.raises(HTTPException) as exc_info:
        authenticate_prtg(x_prtg_token="invalid_token_12345")
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
@patch("api.webhook.IncidentStateTracker.ping", return_value=True)
async def test_health_endpoint_direct(mock_ping):
    from api.webhook import health_check

    with patch("api.webhook.process_prtg_webhook_task") as mock_task:
        mock_conn = MagicMock()
        mock_task.app.connection_for_write.return_value.__enter__.return_value = mock_conn
        response = await health_check()
        assert response["status"] == "healthy"
        assert response["redis"] == "up"
        assert response["broker"] == "up"
