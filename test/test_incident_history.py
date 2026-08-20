"""Unit Tests for Incident Lifecycle and Notifications Service.

Tests timeline compilation, chronological ordering, and pending closure notifications.
"""
from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pytest

from app.models import AlertHistory, Sensor, Site, SiteIspAssignment
from services.incident_service import IncidentService


@pytest.fixture
def mock_open_alert():
    alert = MagicMock(spec=AlertHistory)
    alert.alert_id = 101
    alert.sensor_id = 1001
    alert.state_id = 101
    alert.triggered_at = datetime.datetime(2026, 8, 20, 10, 0, 0, tzinfo=datetime.timezone.utc)
    alert.resolved_at = datetime.datetime(2026, 8, 20, 10, 30, 0, tzinfo=datetime.timezone.utc)
    alert.escalation_status = "Resolved"
    alert.alert_message = "Ping loss exceeded 100%"
    alert.root_cause = None

    mock_site = MagicMock(spec=Site)
    mock_site.site_id = 2001
    mock_site.site_name = "Frankfurt DC-1"
    mock_site.primary_ip = "192.168.10.1"

    mock_assignment = MagicMock(spec=SiteIspAssignment)
    mock_assignment.circuit_id = "CIR-FRA-992"
    mock_assignment.site = mock_site
    mock_assignment.isp = MagicMock(isp_id=3001, isp_name="Deutsche Telekom")

    mock_sensor = MagicMock(spec=Sensor)
    mock_sensor.sensor_name = "Core Switch Gateway"
    mock_sensor.sensor_type = "Ping"
    mock_sensor.site_isp_assignment = mock_assignment

    alert.sensor = mock_sensor
    alert.state = MagicMock(state_name="Down")
    return alert


def test_get_pending_closures_notification_generation(mock_open_alert):
    mock_db = MagicMock()
    mock_db.execute.return_value.scalars.return_value.all.return_value = [mock_open_alert]

    response = IncidentService.get_pending_closures(mock_db)
    assert response.count == 1
    assert len(response.items) == 1

    item = response.items[0]
    assert item.alert_id == 101
    assert item.sensor_id == 1001
    assert item.sensor_name == "Core Switch Gateway"
    assert item.site_name == "Frankfurt DC-1"
    assert item.isp_name == "Deutsche Telekom"
    assert "Sensor 'Core Switch Gateway' recovered successfully" in item.notification_message
    assert "pending" in item.notification_message


def test_get_incident_history_chronological_compilation(mock_open_alert):
    mock_db = MagicMock()

    with patch.object(IncidentService, "get_alert_summary") as mock_summary, \
         patch("services.incident_service.RootCauseRepository") as mock_rca_repo, \
         patch("services.incident_service.AttachmentRepository") as mock_att_repo, \
         patch("services.incident_service.SensorLogRepository") as mock_log_repo, \
         patch("services.incident_service.PingDiagnosticRepository") as mock_ping_repo, \
         patch("services.incident_service.IspEmailThreadRepository") as mock_thread_repo, \
         patch("services.incident_service.ReminderHistoryRepository") as mock_rem_repo, \
         patch("services.incident_service.EscalationRecordRepository") as mock_esc_repo:

        from models.incident_history_model import AlertSummary
        mock_summary.return_value = AlertSummary(
            alert_id=101,
            sensor_id=1001,
            sensor_name="Core Switch Gateway",
            sensor_type="Ping",
            site_id=2001,
            site_name="Frankfurt DC-1",
            primary_ip="192.168.10.1",
            isp_id=3001,
            isp_name="Deutsche Telekom",
            circuit_id="CIR-FRA-992",
            state_id=101,
            state_name="Down",
            current_status="RECOVERED_PENDING_CLOSURE",
            alert_message="Ping loss exceeded",
            escalation_status="Resolved",
            triggered_at=mock_open_alert.triggered_at,
            resolved_at=mock_open_alert.resolved_at,
            total_downtime_human="30m 0s",
        )
        mock_rca_repo.return_value.get_by_alert.return_value = None
        mock_att_repo.return_value.list_for_alert.return_value = MagicMock(items=[])
        mock_log_repo.return_value.list_for_sensor.return_value = []
        mock_ping_repo.return_value.list_for_alert.return_value = []
        mock_thread_repo.return_value.list_for_alert.return_value = MagicMock(items=[])
        mock_rem_repo.return_value.list_for_alert.return_value = MagicMock(items=[])
        mock_esc_repo.return_value.list_for_alert.return_value = []

        history = IncidentService.get_incident_history(mock_db, alert_id=101)

        assert history.alert_information.alert_id == 101
        assert history.current_status == "RECOVERED_PENDING_CLOSURE"
        assert history.total_downtime == "30m 0s"
        assert history.root_cause_analysis is None
        assert isinstance(history.sensor_logs, list)
        assert isinstance(history.ping_diagnostics, list)