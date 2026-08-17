"""Model Representation Unit Tests.

Validates that __repr__ methods of all ORM models format correctly without
requiring a live database connection.
"""
from __future__ import annotations

from app.models import (
    AlertHistory,
    AlertState,
    Attachment,
    EscalationRecord,
    Isp,
    IspContactEmail,
    IspEmailRole,
    IspEmailThread,
    PingDiagnostic,
    ReminderHistory,
    RootCause,
    Sensor,
    SensorLog,
    Site,
    SiteIspAssignment,
)


def test_model_reprs():
    site = Site(site_id=1001, site_name="New York DC")
    assert repr(site) == "<Site 1001 'New York DC'>"

    isp = Isp(isp_id=2001, isp_name="Tier 1 ISP")
    assert repr(isp) == "<Isp 2001 'Tier 1 ISP'>"

    email_obj = IspContactEmail(email_id=3001, email_address="noc@isp.com", email_type=IspEmailRole.NOC)
    assert repr(email_obj) == "<IspContactEmail 3001 'noc@isp.com'>"

    assignment = SiteIspAssignment(assignment_id=4001, site_id=1001, isp_id=2001)
    assert repr(assignment) == "<SiteIspAssignment 4001 site=1001 isp=2001>"

    sensor = Sensor(sensor_id=5001, sensor_name="Core Router Ping")
    assert repr(sensor) == "<Sensor 5001 'Core Router Ping'>"

    alert_state = AlertState(state_id=1001, state_name="Down")
    assert repr(alert_state) == "<AlertState 1001 'Down'>"

    alert = AlertHistory(alert_id=6001, sensor_id=5001)
    assert repr(alert) == "<AlertHistory 6001 sensor=5001>"

    sensor_log = SensorLog(log_id=7001, sensor_id=5001, log_level="CRITICAL")
    assert "7001" in repr(sensor_log)

    ping_diag = PingDiagnostic(ping_id=8001, alert_id=6001)
    assert repr(ping_diag) == "<PingDiagnostic 8001 alert=6001>"

    escalation = EscalationRecord(escalation_id=9001, alert_id=6001)
    assert repr(escalation) == "<EscalationRecord 9001 alert=6001>"

    thread = IspEmailThread(thread_id=101, alert_id=6001, direction="Incoming", subject="Re: Outage")
    assert "101" in repr(thread)

    reminder = ReminderHistory(reminder_id=201, alert_id=6001, reminder_number=1, status="Sent")
    assert "201" in repr(reminder)

    root_cause = RootCause(root_cause_id=301, alert_id=6001, root_cause_name="Hardware Failure")
    assert "301" in repr(root_cause)

    attachment = Attachment(attachment_id=401, file_name="ping.log", object_key="logs/ping.log")
    assert "401" in repr(attachment)