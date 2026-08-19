"""Backward compatibility bridge for new_models module.

All declarative models and enums are centrally defined in app.models.
This module cleanly re-exports them to prevent any breaking changes for legacy imports.
"""
from __future__ import annotations

from app.models import (
    AlertHistory,
    AlertState,
    Attachment,
    EmailClassificationType,
    EmailDirectionType,
    EscalationRecord,
    Isp,
    IspContactEmail,
    IspEmailRole,
    IspEmailThread,
    LogLevelType,
    LogStatusType,
    PingDiagnostic,
    ReminderHistory,
    ReminderStatusType,
    RootCause,
    Sensor,
    SensorLog,
    Site,
    SiteIspAssignment,
    email_classification_enum,
    email_direction_enum,
    isp_email_role_enum,
    log_level_type_enum,
    log_status_type_enum,
    reminder_status_enum,
)

__all__ = [
    "IspEmailRole",
    "LogStatusType",
    "LogLevelType",
    "EmailClassificationType",
    "EmailDirectionType",
    "ReminderStatusType",
    "isp_email_role_enum",
    "log_status_type_enum",
    "log_level_type_enum",
    "email_classification_enum",
    "email_direction_enum",
    "reminder_status_enum",
    "Site",
    "Isp",
    "IspContactEmail",
    "SiteIspAssignment",
    "Sensor",
    "AlertState",
    "AlertHistory",
    "SensorLog",
    "PingDiagnostic",
    "EscalationRecord",
    "IspEmailThread",
    "ReminderHistory",
    "RootCause",
    "Attachment",
]