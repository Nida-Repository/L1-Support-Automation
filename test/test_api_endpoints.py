"""End-to-End API Integration Tests for Authentication, Incidents, Closure, and Notifications.

Tests routing, authentication requirements, dependency injection, and endpoint responses.
"""
from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from api.webhook import app
from app.database import get_db
from config.settings import settings
from services.auth_service import auth_service

client = TestClient(app)


@pytest.fixture
def auth_headers():
    token = auth_service.create_access_token(subject=settings.admin_username)
    return {"Authorization": f"Bearer {token}"}


# ===========================================================================
# 1. Authentication Endpoints
# ===========================================================================

def test_login_success():
    response = client.post(
        "/auth/login",
        json={"username": settings.admin_username, "password": settings.admin_password},
    )
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
    assert data["expires_in"] > 0


def test_login_invalid_credentials():
    response = client.post(
        "/auth/login",
        json={"username": settings.admin_username, "password": "WrongPassword123!"},
    )
    assert response.status_code == 401
    assert "Incorrect username" in response.json()["detail"]


# ===========================================================================
# 2. Public vs Protected Endpoints
# ===========================================================================

def test_protected_endpoints_reject_unauthenticated():
    # Attempting to access without auth header
    res1 = client.get("/alerts/open")
    assert res1.status_code == 401

    res2 = client.get("/notifications/pending-closures")
    assert res2.status_code == 401

    res3 = client.get("/alerts/100/history")
    assert res3.status_code == 401


def test_public_webhook_accessible_with_prtg_token():
    payload = {
        "sensorid": "1042",
        "sensorname": "Ping Gateway",
        "laststatus": "Up",
        "message": "Recovered",
    }
    with patch("api.webhook.process_prtg_webhook_task"):
        response = client.post(
            "/webhook/prtg",
            json=payload,
            headers={"X-PRTG-Token": settings.prtg_webhook_secret or "MySecureWebhookToken2026!"},
        )
        assert response.status_code == 200
        assert response.json()["status"] == "success"


def test_public_health_endpoint():
    with patch("api.webhook.IncidentStateTracker.ping", return_value=True), \
         patch("api.webhook.process_prtg_webhook_task"):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"


# ===========================================================================
# 3. Incident Lifecycle & Notifications
# ===========================================================================

def test_get_open_alerts(auth_headers):
    with patch("api.alerts.IncidentService.get_open_alerts", return_value=[]):
        response = client.get("/alerts/open", headers=auth_headers)
        assert response.status_code == 200
        assert response.json() == []


def test_get_pending_closures(auth_headers):
    mock_res = {"count": 0, "items": []}
    with patch("api.notifications.IncidentService.get_pending_closures", return_value=mock_res):
        response = client.get("/notifications/pending-closures", headers=auth_headers)
        assert response.status_code == 200
        assert response.json()["count"] == 0


def test_get_alert_history(auth_headers):
    mock_history = {
        "alert_information": {
            "alert_id": 501,
            "sensor_id": 1042,
            "sensor_name": "Ping Core",
            "state_id": 101,
            "state_name": "Down",
            "current_status": "CLOSED",
            "triggered_at": "2026-08-20T10:00:00Z",
        },
        "triggered_time": "2026-08-20T10:00:00Z",
        "current_status": "CLOSED",
        "attachments": [],
        "sensor_logs": [],
        "ping_diagnostics": [],
        "isp_email_threads": [],
        "reminder_history": [],
        "escalation_history": [],
    }
    with patch("api.alerts.IncidentService.get_incident_history", return_value=mock_history):
        response = client.get("/alerts/501/history", headers=auth_headers)
        assert response.status_code == 200
        assert response.json()["alert_information"]["alert_id"] == 501


# ===========================================================================
# 4. Attachment Upload Endpoint
# ===========================================================================

def test_attachment_upload_success(auth_headers):
    mock_db = MagicMock()
    app.dependency_overrides[get_db] = lambda: mock_db

    mock_alert = MagicMock(alert_id=501)
    mock_att = MagicMock(
        attachment_id=101,
        alert_id=501,
        thread_id=None,
        file_name="rca_diagram.png",
        file_type="image/png",
        file_size=1024,
        bucket_name="l1-support-attachments",
        object_key="501/0/abcdef_rca_diagram.png",
        etag="abc123etag",
        uploaded_by=settings.admin_username,
        uploaded_at="2026-08-20T10:30:00Z",
    )

    with patch("api.attachments.AlertHistoryRepository") as mock_alert_repo, \
         patch("api.attachments.AttachmentRepository") as mock_att_repo, \
         patch("api.attachments.minio_service.upload_attachment") as mock_upload:

        mock_alert_repo.return_value.get.return_value = mock_alert
        mock_upload.return_value = {
            "object_key": "501/0/abcdef_rca_diagram.png",
            "bucket_name": "l1-support-attachments",
            "file_size": 1024,
            "file_type": "image/png",
            "file_name": "rca_diagram.png",
            "etag": "abc123etag",
        }
        mock_att_repo.return_value.create.return_value = mock_att

        file_data = io.BytesIO(b"fake image data")
        response = client.post(
            "/alerts/501/attachments",
            headers=auth_headers,
            files={"file": ("rca_diagram.png", file_data, "image/png")},
        )

        assert response.status_code == 201
        data = response.json()
        assert data["file_name"] == "rca_diagram.png"
        assert data["uploaded_by"] == settings.admin_username

    app.dependency_overrides.pop(get_db, None)


# ===========================================================================
# 5. Incident Closure Endpoint
# ===========================================================================

def test_complete_closure_endpoint(auth_headers):
    closure_payload = {
        "root_cause": {
            "root_cause_name": "Fiber Cut near Gateway",
            "category": "Fiber Cut",
            "description": "Metro line severed during highway expansion work. Spliced by field tech.",
            "identified_by": "ISP Field Tech Team",
            "customer_confirmed": True,
            "total_downtime_seconds": 1800,
        },
        "closure_reason": "Resolved and tested link stability",
    }
    from models.closure_model import ClosureResponse
    from models.root_cause_model import RootCauseRead
    import datetime

    mock_rca = RootCauseRead(
        root_cause_id=201,
        alert_id=501,
        root_cause_name="Fiber Cut near Gateway",
        category="Fiber Cut",
        description="Metro line severed during highway expansion work.",
        identified_by="ISP Field Tech Team",
        identified_at=datetime.datetime(2026, 8, 20, 10, 30, 0, tzinfo=datetime.timezone.utc),
        customer_confirmed=True,
        total_downtime_seconds=1800,
        total_downtime_human="30m 0s",
    )
    mock_res = ClosureResponse(
        status="success",
        message="Incident closure completed and finalized successfully.",
        alert_id=501,
        sensor_id=1042,
        resolved_at=datetime.datetime(2026, 8, 20, 10, 30, 0, tzinfo=datetime.timezone.utc),
        total_downtime_human="30m 0s",
        root_cause=mock_rca,
        attachments_count=1,
        closed_at=datetime.datetime(2026, 8, 20, 10, 30, 0, tzinfo=datetime.timezone.utc),
    )
    with patch("api.closure.ClosureService.execute_closure_transaction", return_value=mock_res):
        response = client.post(
            "/alerts/501/complete-closure",
            json=closure_payload,
            headers=auth_headers,
        )
        assert response.status_code == 200
        assert response.json()["status"] == "success"