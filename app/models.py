"""SQLAlchemy Declarative ORM Models.

Defines all database entities, PostgreSQL enum types, constraints, indexes,
and static relationship mappings for the L1 Support Automation system.
"""
from __future__ import annotations

import datetime
import decimal
import enum
import ipaddress
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKey,
    Identity,
    Index,
    Integer,
    Interval,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, INET, JSONB, TIME, TIMESTAMP
from sqlalchemy.dialects.postgresql import ENUM as PGEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


# ---------------------------------------------------------------------------
# 1. Enums
# ---------------------------------------------------------------------------

class IspEmailRole(str, enum.Enum):
    PRIMARY = "Primary"
    BACKUP = "Backup"
    NOC = "NOC"
    ESCALATION = "Escalation"


class LogStatusType(str, enum.Enum):
    OPENED = "opened"
    CLOSED = "closed"


class LogLevelType(str, enum.Enum):
    GOOD = "GOOD"
    INFO = "INFO"
    WARNING = "WARNING"
    SEVERE = "SEVERE"
    CRITICAL = "CRITICAL"


class EmailClassificationType(str, enum.Enum):
    LINK_STABLE = "Link Stable"
    MAINTENANCE = "Maintenance"
    NEED_PING = "Need Ping"
    NEED_TRACEROUTE = "Need Traceroute"
    TECHNICAL_ISSUE = "Technical Issue"
    NO_ISP_ISSUE = "No ISP Issue"
    POWER_ISSUE = "Power Issue"
    UNKNOWN = "Unknown"


class EmailDirectionType(str, enum.Enum):
    INCOMING = "Incoming"
    OUTGOING = "Outgoing"


class ReminderStatusType(str, enum.Enum):
    SENT = "Sent"
    FAILED = "Failed"
    DELIVERED = "Delivered"
    BOUNCED = "Bounced"


# Native PostgreSQL ENUM types
isp_email_role_enum = PGEnum(
    IspEmailRole, name="isp_email_role", values_callable=lambda e: [m.value for m in e]
)
log_status_type_enum = PGEnum(
    LogStatusType, name="log_status_type", values_callable=lambda e: [m.value for m in e]
)
log_level_type_enum = PGEnum(
    LogLevelType, name="log_level_type", values_callable=lambda e: [m.value for m in e]
)
email_classification_enum = PGEnum(
    EmailClassificationType,
    name="email_classification_type",
    values_callable=lambda obj: [e.value for e in obj],
)
email_direction_enum = PGEnum(
    EmailDirectionType,
    name="email_direction_type",
    values_callable=lambda obj: [e.value for e in obj],
)
reminder_status_enum = PGEnum(
    ReminderStatusType,
    name="reminder_status_type",
    values_callable=lambda obj: [e.value for e in obj],
)


# ---------------------------------------------------------------------------
# 2. Topology & Base Tables
# ---------------------------------------------------------------------------

class Site(Base):
    __tablename__ = "sites"

    site_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    primary_ip: Mapped[ipaddress.IPv4Address | ipaddress.IPv6Address] = mapped_column(
        INET, nullable=False
    )
    location: Mapped[Optional[str]] = mapped_column(String(30))
    timezone: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )

    assignments: Mapped[list["SiteIspAssignment"]] = relationship(
        back_populates="site", cascade="save-update, merge"
    )

    __table_args__ = (
        CheckConstraint("site_id BETWEEN 1000 AND 9999", name="chk_site_id_4_digit"),
    )

    def __repr__(self) -> str:
        return f"<Site {self.site_id} {self.site_name!r}>"


class Isp(Base):
    __tablename__ = "isps"

    isp_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    isp_name: Mapped[str] = mapped_column(String(255), nullable=False)
    account_manager: Mapped[Optional[str]] = mapped_column(String(255))
    support_phone: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )

    contact_emails: Mapped[list["IspContactEmail"]] = relationship(
        back_populates="isp", cascade="save-update, merge"
    )
    assignments: Mapped[list["SiteIspAssignment"]] = relationship(
        back_populates="isp", cascade="save-update, merge"
    )

    __table_args__ = (
        CheckConstraint("isp_id BETWEEN 1000 AND 9999", name="chk_isp_id_4_digit"),
    )

    def __repr__(self) -> str:
        return f"<Isp {self.isp_id} {self.isp_name!r}>"


class IspContactEmail(Base):
    __tablename__ = "isp_contact_emails"

    email_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    isp_id: Mapped[int] = mapped_column(
        ForeignKey("isps.isp_id", ondelete="RESTRICT"), nullable=False
    )
    email_address: Mapped[str] = mapped_column(String(255), nullable=False)
    email_type: Mapped[IspEmailRole] = mapped_column(isp_email_role_enum, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")

    isp: Mapped["Isp"] = relationship(back_populates="contact_emails")
    reminders: Mapped[list["ReminderHistory"]] = relationship(back_populates="email")

    __table_args__ = (
        CheckConstraint("email_id BETWEEN 1000 AND 9999", name="chk_email_id_4_digit"),
        CheckConstraint(
            r"email_address ~* '^[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'",
            name="valid_email_format",
        ),
        CheckConstraint("priority > 0", name="isp_contact_emails_priority_check"),
        Index("idx_isp_contact_emails_isp", "isp_id"),
    )

    def __repr__(self) -> str:
        return f"<IspContactEmail {self.email_id} {self.email_address!r}>"


class SiteIspAssignment(Base):
    __tablename__ = "site_isp_assignments"

    assignment_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(
        ForeignKey("sites.site_id", ondelete="RESTRICT"), nullable=False
    )
    isp_id: Mapped[int] = mapped_column(
        ForeignKey("isps.isp_id", ondelete="RESTRICT"), nullable=False
    )
    is_primary_isp: Mapped[Optional[bool]] = mapped_column(Boolean, server_default="true")
    circuit_id: Mapped[str] = mapped_column(String(100), nullable=False)
    bandwidth_mbps: Mapped[Optional[int]] = mapped_column(Integer)

    site: Mapped["Site"] = relationship(back_populates="assignments")
    isp: Mapped["Isp"] = relationship(back_populates="assignments")
    sensors: Mapped[list["Sensor"]] = relationship(
        back_populates="site_isp_assignment", cascade="save-update, merge"
    )

    __table_args__ = (
        CheckConstraint(
            "assignment_id BETWEEN 1000 AND 9999", name="chk_assignment_id_4_digit"
        ),
        CheckConstraint(
            "bandwidth_mbps > 0", name="site_isp_assignments_bandwidth_mbps_check"
        ),
        UniqueConstraint("site_id", "isp_id", name="uq_site_isp"),
        Index("idx_site_isp_assignments_isp", "isp_id"),
    )

    def __repr__(self) -> str:
        return f"<SiteIspAssignment {self.assignment_id} site={self.site_id} isp={self.isp_id}>"


class Sensor(Base):
    __tablename__ = "sensors"

    sensor_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sensor_name: Mapped[str] = mapped_column(String(100), nullable=False)
    sensor_type: Mapped[str] = mapped_column(String(100), nullable=False)
    site_isp_assignment_id: Mapped[int] = mapped_column(
        ForeignKey("site_isp_assignments.assignment_id", ondelete="RESTRICT"),
        nullable=False,
    )
    prtg_core_ip: Mapped[Optional[ipaddress.IPv4Address | ipaddress.IPv6Address]] = (
        mapped_column(INET)
    )
    warning_threshold: Mapped[Optional[dict]] = mapped_column(JSONB)
    report_generation_time: Mapped[datetime.time] = mapped_column(TIME, nullable=False)
    report_time_zone: Mapped[str] = mapped_column(String(64), nullable=False)

    site_isp_assignment: Mapped["SiteIspAssignment"] = relationship(back_populates="sensors")
    alert_history: Mapped[list["AlertHistory"]] = relationship(
        back_populates="sensor", cascade="save-update, merge"
    )
    sensor_logs: Mapped[list["SensorLog"]] = relationship(
        back_populates="sensor", cascade="save-update, merge"
    )

    __table_args__ = (
        CheckConstraint("sensor_id BETWEEN 1000 AND 9999", name="chk_sensor_id_4_digit"),
        Index("idx_sensors_assignment", "site_isp_assignment_id"),
    )

    def __repr__(self) -> str:
        return f"<Sensor {self.sensor_id} {self.sensor_name!r}>"


class AlertState(Base):
    __tablename__ = "alert_states"

    state_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    state_name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)

    alert_history: Mapped[list["AlertHistory"]] = relationship(back_populates="state")

    __table_args__ = (
        CheckConstraint("state_id BETWEEN 1000 AND 9999", name="chk_state_id_4_digit"),
    )

    def __repr__(self) -> str:
        return f"<AlertState {self.state_id} {self.state_name!r}>"


# ---------------------------------------------------------------------------
# 3. High-Volume Log & Alert History Tables
# ---------------------------------------------------------------------------

class AlertHistory(Base):
    __tablename__ = "alert_history"

    alert_id: Mapped[int] = mapped_column(
        BigInteger, Identity(always=True, start=100), primary_key=True
    )
    sensor_id: Mapped[int] = mapped_column(
        ForeignKey("sensors.sensor_id", ondelete="RESTRICT"), nullable=False
    )
    state_id: Mapped[int] = mapped_column(
        ForeignKey("alert_states.state_id"), nullable=False
    )
    triggered_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )
    resolved_at: Mapped[Optional[datetime.datetime]] = mapped_column(TIMESTAMP(timezone=True))
    alert_message: Mapped[Optional[str]] = mapped_column(Text)
    escalation_status: Mapped[Optional[str]] = mapped_column(String(50))

    sensor: Mapped["Sensor"] = relationship(back_populates="alert_history")
    state: Mapped["AlertState"] = relationship(back_populates="alert_history")
    ping_diagnostics: Mapped[list["PingDiagnostic"]] = relationship(
        back_populates="alert", cascade="all, delete-orphan", passive_deletes=True
    )
    escalation_records: Mapped[list["EscalationRecord"]] = relationship(
        back_populates="alert", cascade="all, delete-orphan", passive_deletes=True
    )
    email_threads: Mapped[list["IspEmailThread"]] = relationship(
        back_populates="alert", cascade="all, delete-orphan", passive_deletes=True
    )
    reminders: Mapped[list["ReminderHistory"]] = relationship(
        back_populates="alert", cascade="all, delete-orphan", passive_deletes=True
    )
    root_cause: Mapped[Optional["RootCause"]] = relationship(
        back_populates="alert", cascade="all, delete-orphan", passive_deletes=True, uselist=False
    )
    attachments: Mapped[list["Attachment"]] = relationship(
        back_populates="alert", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        Index("idx_alert_history_sensor_time", "sensor_id", triggered_at.desc()),
        Index(
            "idx_alert_history_unresolved",
            "state_id",
            triggered_at.desc(),
            postgresql_where=(resolved_at.is_(None)),
        ),
    )

    def __repr__(self) -> str:
        return f"<AlertHistory {self.alert_id} sensor={self.sensor_id}>"


class SensorLog(Base):
    __tablename__ = "sensor_logs"

    log_id: Mapped[int] = mapped_column(
        BigInteger, Identity(always=True, start=100), primary_key=True
    )
    sensor_id: Mapped[int] = mapped_column(
        ForeignKey("sensors.sensor_id", ondelete="RESTRICT"), nullable=False
    )
    log_timestamp: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )
    log_level: Mapped[LogLevelType] = mapped_column(
        log_level_type_enum, nullable=False, server_default=LogLevelType.SEVERE.value
    )
    log_status: Mapped[LogStatusType] = mapped_column(
        log_status_type_enum, nullable=False, server_default=LogStatusType.OPENED.value
    )
    log_message: Mapped[Optional[str]] = mapped_column(Text)
    log_details: Mapped[Optional[dict]] = mapped_column(JSONB)

    sensor: Mapped["Sensor"] = relationship(back_populates="sensor_logs")

    __table_args__ = (
        Index("idx_sensor_logs_sensor_time", "sensor_id", log_timestamp.desc()),
    )

    def __repr__(self) -> str:
        return f"<SensorLog {self.log_id} sensor={self.sensor_id} level={self.log_level}>"


class PingDiagnostic(Base):
    __tablename__ = "ping_diagnostics"

    ping_id: Mapped[int] = mapped_column(
        BigInteger, Identity(always=True, start=100), primary_key=True
    )
    alert_id: Mapped[int] = mapped_column(
        ForeignKey("alert_history.alert_id", ondelete="CASCADE"), nullable=False
    )
    packet_count: Mapped[int] = mapped_column(Integer, nullable=False)
    packet_loss_percent: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(5, 2))
    min_rtt_ms: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(7, 2))
    avg_rtt_ms: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(7, 2))
    max_rtt_ms: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(7, 2))
    executed_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )
    technician_notes: Mapped[Optional[str]] = mapped_column(Text)

    alert: Mapped["AlertHistory"] = relationship(back_populates="ping_diagnostics")

    __table_args__ = (
        CheckConstraint("packet_count > 0", name="ping_diagnostics_packet_count_check"),
        CheckConstraint(
            "packet_loss_percent BETWEEN 0.00 AND 100.00", name="chk_packet_loss"
        ),
        CheckConstraint("min_rtt_ms >= 0", name="ping_diagnostics_min_rtt_ms_check"),
        CheckConstraint("avg_rtt_ms >= 0", name="ping_diagnostics_avg_rtt_ms_check"),
        CheckConstraint("max_rtt_ms >= 0", name="ping_diagnostics_max_rtt_ms_check"),
        Index(
            "idx_ping_diagnostics_alert_covered",
            "alert_id",
            postgresql_include=["packet_loss_percent", "avg_rtt_ms", "executed_at"],
        ),
    )

    def __repr__(self) -> str:
        return f"<PingDiagnostic {self.ping_id} alert={self.alert_id}>"


class EscalationRecord(Base):
    __tablename__ = "escalation_records"

    escalation_id: Mapped[int] = mapped_column(
        BigInteger, Identity(always=True, start=100), primary_key=True
    )
    alert_id: Mapped[int] = mapped_column(
        ForeignKey("alert_history.alert_id", ondelete="CASCADE"), nullable=False
    )
    escalated_to: Mapped[str] = mapped_column(String(50), nullable=False)
    recipient_email: Mapped[str] = mapped_column(String(255), nullable=False)
    cc_emails: Mapped[Optional[list[str]]] = mapped_column(ARRAY(Text))
    email_subject: Mapped[Optional[str]] = mapped_column(Text)
    email_body: Mapped[Optional[str]] = mapped_column(Text)
    sent_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )
    response_received: Mapped[Optional[bool]] = mapped_column(Boolean, server_default="false")
    response_notes: Mapped[Optional[str]] = mapped_column(Text)

    alert: Mapped["AlertHistory"] = relationship(back_populates="escalation_records")

    __table_args__ = (
        CheckConstraint(
            "escalated_to IN ('ISP', 'SUPPORT TEAM')", name="chk_escalated_to"
        ),
        Index("idx_escalation_records_alert", "alert_id"),
    )

    def __repr__(self) -> str:
        return f"<EscalationRecord {self.escalation_id} alert={self.alert_id}>"


# ---------------------------------------------------------------------------
# 4. Email Threads, Reminders, Root Cause & Attachments
# ---------------------------------------------------------------------------

class IspEmailThread(Base):
    __tablename__ = "isp_email_threads"

    thread_id: Mapped[int] = mapped_column(
        Identity(start=100, always=True), primary_key=True
    )
    alert_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("alert_history.alert_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    message_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    in_reply_to: Mapped[Optional[str]] = mapped_column(String(255))
    email_references: Mapped[Optional[list[str]]] = mapped_column(ARRAY(Text))
    subject: Mapped[Optional[str]] = mapped_column(Text)
    sender: Mapped[str] = mapped_column(String(255), nullable=False)
    receiver: Mapped[str] = mapped_column(String(255), nullable=False)
    cc: Mapped[Optional[list[str]]] = mapped_column(ARRAY(Text))
    direction: Mapped[EmailDirectionType] = mapped_column(
        email_direction_enum, nullable=False
    )
    sent_received_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )
    body: Mapped[Optional[str]] = mapped_column(Text)
    classification_type: Mapped[EmailClassificationType] = mapped_column(
        email_classification_enum,
        nullable=False,
        server_default=EmailClassificationType.UNKNOWN.value,
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )

    alert: Mapped["AlertHistory"] = relationship(back_populates="email_threads")
    attachments: Mapped[list["Attachment"]] = relationship(
        back_populates="thread", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        Index("idx_isp_email_threads_alert", "alert_id"),
        Index(
            "idx_isp_email_threads_reply",
            "in_reply_to",
            postgresql_where=(in_reply_to.isnot(None)),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<IspEmailThread id={self.thread_id} alert_id={self.alert_id} "
            f"direction={self.direction!r} subject={self.subject!r}>"
        )


class ReminderHistory(Base):
    __tablename__ = "reminder_history"

    reminder_id: Mapped[int] = mapped_column(
        Identity(start=100, always=True), primary_key=True
    )
    alert_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("alert_history.alert_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    reminder_number: Mapped[int] = mapped_column(nullable=False)
    sent_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )
    email_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("isp_contact_emails.email_id", ondelete="SET NULL")
    )
    response_received: Mapped[bool] = mapped_column(nullable=False, server_default="false")
    response_received_at: Mapped[Optional[datetime.datetime]] = mapped_column(TIMESTAMP(timezone=True))
    status: Mapped[ReminderStatusType] = mapped_column(
        reminder_status_enum,
        nullable=False,
        server_default=ReminderStatusType.SENT.value,
    )

    alert: Mapped["AlertHistory"] = relationship(back_populates="reminders")
    email: Mapped[Optional["IspContactEmail"]] = relationship(back_populates="reminders")

    __table_args__ = (
        CheckConstraint("reminder_number > 0", name="reminder_history_reminder_number_check"),
        CheckConstraint(
            "response_received_at IS NULL OR response_received_at >= sent_at",
            name="chk_response_time",
        ),
        Index("idx_reminder_history_alert", "alert_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<ReminderHistory id={self.reminder_id} alert_id={self.alert_id} "
            f"number={self.reminder_number} status={self.status!r}>"
        )


class RootCause(Base):
    __tablename__ = "root_cause"

    root_cause_id: Mapped[int] = mapped_column(
        Identity(start=100, always=True), primary_key=True
    )
    alert_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("alert_history.alert_id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    root_cause_name: Mapped[str] = mapped_column(String(100), nullable=False)
    category: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    identified_by: Mapped[str] = mapped_column(String(100), nullable=False)
    identified_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )
    customer_confirmed: Mapped[bool] = mapped_column(nullable=False, server_default="false")
    total_downtime: Mapped[Optional[datetime.timedelta]] = mapped_column(Interval)

    alert: Mapped["AlertHistory"] = relationship(back_populates="root_cause")

    def __repr__(self) -> str:
        return f"<RootCause id={self.root_cause_id} alert_id={self.alert_id} name={self.root_cause_name!r}>"


class Attachment(Base):
    __tablename__ = "attachments"

    attachment_id: Mapped[int] = mapped_column(
        Identity(start=100, always=True), primary_key=True
    )
    alert_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("alert_history.alert_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    thread_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("isp_email_threads.thread_id", ondelete="CASCADE")
    )
    file_name: Mapped[str] = mapped_column(String(100), nullable=False)
    file_type: Mapped[str] = mapped_column(String(100), nullable=False)
    file_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    bucket_name: Mapped[str] = mapped_column(String(100), nullable=False)
    object_key: Mapped[str] = mapped_column(String(500), nullable=False, unique=True)
    etag: Mapped[Optional[str]] = mapped_column(String(100))
    uploaded_by: Mapped[str] = mapped_column(String(100), nullable=False)
    uploaded_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )

    alert: Mapped["AlertHistory"] = relationship(back_populates="attachments")
    thread: Mapped[Optional["IspEmailThread"]] = relationship(back_populates="attachments")

    __table_args__ = (
        CheckConstraint("file_size > 0", name="attachments_file_size_check"),
        Index("idx_attachments_alert", "alert_id"),
        Index(
            "idx_attachments_thread",
            "thread_id",
            postgresql_where=(thread_id.isnot(None)),
        ),
    )

    def __repr__(self) -> str:
        return f"<Attachment id={self.attachment_id} file_name={self.file_name!r} object_key={self.object_key!r}>"
