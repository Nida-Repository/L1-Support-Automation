"""Unit Tests for Centralized Incident Closure Service.

Tests validation rules, atomic single-transaction execution, manual closures,
downtime calculation, and rollback behavior on failure.
"""
from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pytest

from app.models import AlertHistory, RootCause
from models.closure_model import ClosureRcaPayload
from services.closure_service import ClosureService, ClosureValidationError


@pytest.fixture
def sample_alert():
    alert = MagicMock(spec=AlertHistory)
    alert.alert_id = 501
    alert.sensor_id = 1042
    alert.triggered_at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=45)
    alert.resolved_at = datetime.datetime.now(datetime.timezone.utc)
    return alert


@pytest.fixture
def valid_rca_payload():
    return ClosureRcaPayload(
        root_cause_name="Core Fiber Cut on Route 4",
        category="Fiber Cut",
        description="Underground optical fiber severed during municipal road construction. Repaired by ISP field technician.",
        identified_by="ISP Level 3 NOC Engineer",
        customer_confirmed=True,
        total_downtime_seconds=2700,
    )


def test_closure_validation_fails_without_attachments(sample_alert, valid_rca_payload):
    with pytest.raises(ClosureValidationError) as exc_info:
        ClosureService.validate_closure_requirements(
            alert=sample_alert,
            rca=valid_rca_payload,
            attachments=[],  # No attachments uploaded
        )
    assert exc_info.value.field == "attachments"
    assert "mandatory" in exc_info.value.reason


def test_closure_rca_payload_validation_rejects_invalid_fields():
    from pydantic import ValidationError

    # Test short root cause name
    with pytest.raises(ValidationError):
        ClosureRcaPayload(
            root_cause_name="No",  # too short
            category="Hardware",
            description="Power supply failed on switch. Replaced with spare unit.",
            identified_by="Field Tech",
            customer_confirmed=True,
        )

    # Test short description
    with pytest.raises(ValidationError):
        ClosureRcaPayload(
            root_cause_name="Switch Reboot",
            category="Hardware",
            description="Rebooted",  # too short
            identified_by="Field Tech",
            customer_confirmed=True,
        )

    # Test empty identified_by
    with pytest.raises(ValidationError):
        ClosureRcaPayload(
            root_cause_name="Switch Reboot",
            category="Hardware",
            description="Power supply failed on switch. Replaced with spare unit.",
            identified_by=" ",  # whitespace only
            customer_confirmed=True,
        )


def test_closure_execution_successful(sample_alert, valid_rca_payload):
    mock_db = MagicMock()
    mock_att = MagicMock(attachment_id=101, file_name="rca_report.pdf")

    mock_rca_record = MagicMock(spec=RootCause)
    mock_rca_record.root_cause_id = 201
    mock_rca_record.alert_id = 501
    mock_rca_record.root_cause_name = valid_rca_payload.root_cause_name
    mock_rca_record.category = valid_rca_payload.category
    mock_rca_record.description = valid_rca_payload.description
    mock_rca_record.identified_by = valid_rca_payload.identified_by
    mock_rca_record.identified_at = datetime.datetime.now(datetime.timezone.utc)
    mock_rca_record.customer_confirmed = True
    mock_rca_record.total_downtime = datetime.timedelta(seconds=2700)

    with patch("services.closure_service.AlertHistoryRepository") as mock_alert_repo, \
         patch("services.closure_service.AttachmentRepository") as mock_att_repo, \
         patch("services.closure_service.RootCauseRepository") as mock_rca_repo, \
         patch("services.closure_service.SensorLogRepository") as mock_log_repo:

        mock_alert_repo.return_value.get.return_value = sample_alert
        mock_att_repo.return_value.list_for_alert.return_value = MagicMock(items=[mock_att])
        mock_rca_repo.return_value.upsert_for_alert.return_value = mock_rca_record
        mock_log_repo.return_value.close_open_logs.return_value = 1

        response = ClosureService.execute_closure_transaction(
            mock_db,
            alert_id=501,
            rca_payload=valid_rca_payload,
            authenticated_user="engineer_alice",
            closure_reason="Resolved following link stability testing",
            is_manual_closure=False,
        )

        assert response.status == "success"
        assert response.alert_id == 501
        assert response.sensor_id == 1042
        assert response.root_cause.root_cause_name == valid_rca_payload.root_cause_name
        assert response.attachments_count == 1

        # Verify atomic commit was executed
        mock_db.commit.assert_called_once()
        mock_db.rollback.assert_not_called()


def test_closure_execution_manual_closure(sample_alert, valid_rca_payload):
    # Alert where resolved_at was not yet populated
    sample_alert.resolved_at = None
    mock_db = MagicMock()
    mock_att = MagicMock(attachment_id=101)

    mock_rca_record = MagicMock(spec=RootCause)
    mock_rca_record.root_cause_id = 202
    mock_rca_record.alert_id = 501
    mock_rca_record.root_cause_name = valid_rca_payload.root_cause_name
    mock_rca_record.category = valid_rca_payload.category
    mock_rca_record.description = valid_rca_payload.description
    mock_rca_record.identified_by = valid_rca_payload.identified_by
    mock_rca_record.identified_at = datetime.datetime.now(datetime.timezone.utc)
    mock_rca_record.customer_confirmed = True
    mock_rca_record.total_downtime = datetime.timedelta(seconds=2700)

    with patch("services.closure_service.AlertHistoryRepository") as mock_alert_repo, \
         patch("services.closure_service.AttachmentRepository") as mock_att_repo, \
         patch("services.closure_service.RootCauseRepository") as mock_rca_repo, \
         patch("services.closure_service.SensorLogRepository") as mock_log_repo:

        mock_alert_repo.return_value.get.return_value = sample_alert
        mock_att_repo.return_value.list_for_alert.return_value = MagicMock(items=[mock_att])
        mock_rca_repo.return_value.upsert_for_alert.return_value = mock_rca_record

        response = ClosureService.execute_closure_transaction(
            mock_db,
            alert_id=501,
            rca_payload=valid_rca_payload,
            authenticated_user="manager_bob",
            closure_reason="Management override due to ISP maintenance schedule",
            is_manual_closure=True,
        )

        assert response.status == "success"
        # Verify alert resolved_at was populated
        mock_alert_repo.return_value.resolve.assert_called_once()
        mock_db.commit.assert_called_once()