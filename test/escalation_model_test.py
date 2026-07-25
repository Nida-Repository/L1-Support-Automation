import pytest
from datetime import datetime
from pydantic import ValidationError

from models.escalation_model import (
    EscalatedTo,
    EscalationRecordCreate,
    EscalationRecordRead,
    EscalationRecordUpdate,
)


# ===========================================================================
# 1. Tests for EscalationRecordCreate
# ===========================================================================

def test_create_valid_payload():
    """Test creating a valid record with full fields."""
    data = {
        "alert_id": 10,
        "escalated_to": "ISP",
        "recipient_email": "admin@example.com",
        "cc_emails": ["ops@example.com", "OPS@example.com", "dev@example.com"],
        "email_subject": "  Urgent Alert  ",  # Tests whitespace stripping
        "email_body": "Please look into this.",
    }
    record = EscalationRecordCreate(**data)

    assert record.alert_id == 10
    assert record.escalated_to == EscalatedTo.ISP
    assert record.recipient_email == "admin@example.com"
    # Whitespace stripping on str fields
    assert record.email_subject == "Urgent Alert"
    # Deduplication check (case-insensitive deduplication)
    assert record.cc_emails == ["ops@example.com", "dev@example.com"]


def test_create_invalid_alert_id():
    """Test that alert_id <= 0 fails validation (gt=0 requirement)."""
    with pytest.raises(ValidationError) as exc_info:
        EscalationRecordCreate(
            alert_id=0,
            escalated_to=EscalatedTo.SUPPORT_TEAM,
            recipient_email="test@example.com",
        )
    assert "Input should be greater than 0" in str(exc_info.value)


def test_create_invalid_enum_value():
    """Test that an invalid enum value triggers a validation error."""
    with pytest.raises(ValidationError):
        EscalationRecordCreate(
            alert_id=1,
            escalated_to="MANAGEMENT",  # Not in EscalatedTo enum
            recipient_email="test@example.com",
        )


def test_create_invalid_email():
    """Test email syntax validation."""
    with pytest.raises(ValidationError):
        EscalationRecordCreate(
            alert_id=1,
            escalated_to=EscalatedTo.ISP,
            recipient_email="invalid-email-string",
        )


def test_create_extra_fields_forbidden():
    """Test that unexpected extra fields raise an error due to extra='forbid'."""
    with pytest.raises(ValidationError) as exc_info:
        EscalationRecordCreate(
            alert_id=1,
            escalated_to=EscalatedTo.ISP,
            recipient_email="test@example.com",
            unknown_field="some_value",
        )
    assert "Extra inputs are not permitted" in str(exc_info.value)


def test_cc_emails_empty_list_returns_none():
    """Test that passing an empty cc_emails list transforms to None."""
    record = EscalationRecordCreate(
        alert_id=1,
        escalated_to=EscalatedTo.ISP,
        recipient_email="test@example.com",
        cc_emails=[],
    )
    assert record.cc_emails is None


# ===========================================================================
# 2. Tests for EscalationRecordUpdate
# ===========================================================================

def test_update_partial_payload():
    """Test updating only a subset of fields."""
    update = EscalationRecordUpdate(
        response_received=True,
        response_notes="Issue resolved by ISP.",
    )
    assert update.response_received is True
    assert update.response_notes == "Issue resolved by ISP."
    assert update.cc_emails is None


def test_update_cc_emails_deduplication():
    """Test duplicate cleanup in update model."""
    update = EscalationRecordUpdate(
        cc_emails=["a@test.com", "A@test.com", "b@test.com"]
    )
    assert update.cc_emails == ["a@test.com", "b@test.com"]


# ===========================================================================
# 3. Tests for EscalationRecordRead & ORM Compatibility
# ===========================================================================

def test_read_model_from_dict():
    """Test reading full record including generated DB fields."""
    now = datetime.now()
    data = {
        "escalation_id": 1,
        "sent_at": now,
        "alert_id": 5,
        "escalated_to": EscalatedTo.SUPPORT_TEAM,
        "recipient_email": "support@company.com",
    }
    record = EscalationRecordRead(**data)
    assert record.escalation_id == 1
    assert record.sent_at == now


def test_read_model_from_orm_object():
    """Test loading data from an ORM-like Python object (from_attributes=True)."""
    class MockORMObject:
        escalation_id = 99
        sent_at = datetime.now()
        alert_id = 42
        escalated_to = EscalatedTo.ISP
        recipient_email = "isp-help@isp.com"
        cc_emails = ["team@company.com"]
        email_subject = "Outage Report"
        email_body = "Details here"
        response_received = False
        response_notes = None

    orm_obj = MockORMObject()
    record = EscalationRecordRead.model_validate(orm_obj)

    assert record.escalation_id == 99
    assert record.alert_id == 42
    assert record.recipient_email == "isp-help@isp.com"