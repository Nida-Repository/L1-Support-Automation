from datetime import datetime, time, date
from typing import List, Optional, Any
from decimal import Decimal

from sqlalchemy import (
    Integer, BigInteger, String, Boolean, ForeignKey,
    DateTime, Text, Numeric, ARRAY, CheckConstraint,
    Time, Date, Enum, func
)
from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


# =========================================================================
# ENUM TYPE
# =========================================================================

isp_email_role_enum = Enum(
    'Primary', 'Backup', 'NOC', 'Escalation', 
    name='isp_email_role'
)


# =========================================================================
# 1. ISP TABLES
# =========================================================================

class Isp(Base):
    __tablename__ = "isps"

    isp_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    isp_name: Mapped[str] = mapped_column(String(255), nullable=False)
    account_manager: Mapped[Optional[str]] = mapped_column(String(255))
    support_phone: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        server_default=func.now()
    )

    emails: Mapped[List["IspContactEmail"]] = relationship(
        "IspContactEmail", back_populates="isp", cascade="all, delete-orphan"
    )
    assignments: Mapped[List["SiteIspAssignment"]] = relationship(
        "SiteIspAssignment", back_populates="isp"
    )


class IspContactEmail(Base):
    __tablename__ = "isp_contact_emails"

    email_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    isp_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("isps.isp_id", ondelete="CASCADE")
    )
    email_address: Mapped[str] = mapped_column(String(255), nullable=False)
    email_type: Mapped[str] = mapped_column(isp_email_role_enum, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=1)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    isp: Mapped[Optional["Isp"]] = relationship("Isp", back_populates="emails")


# =========================================================================
# 2. SITE TABLES
# =========================================================================

class Site(Base):
    __tablename__ = "sites"

    site_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    primary_ip: Mapped[Any] = mapped_column(INET, nullable=False)
    location: Mapped[Optional[str]] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("site_id BETWEEN 1000 AND 9999", name="chk_site_id_4_digit"),
    )

    assignments: Mapped[List["SiteIspAssignment"]] = relationship(
        "SiteIspAssignment", back_populates="site"
    )


class SiteIspAssignment(Base):
    __tablename__ = "site_isp_assignments"

    assignment_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    site_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("sites.site_id", ondelete="RESTRICT")
    )
    isp_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("isps.isp_id", ondelete="RESTRICT")
    )
    is_primary_isp: Mapped[bool] = mapped_column(Boolean, default=True)
    circuit_id: Mapped[str] = mapped_column(String(100), nullable=False)
    bandwidth_mbps: Mapped[Optional[int]] = mapped_column(Integer)

    site: Mapped[Optional["Site"]] = relationship("Site", back_populates="assignments")
    isp: Mapped[Optional["Isp"]] = relationship("Isp", back_populates="assignments")
    sensors: Mapped[List["Sensor"]] = relationship("Sensor", back_populates="assignment")


# =========================================================================
# 3. SENSOR TABLES
# =========================================================================

class Sensor(Base):
    __tablename__ = "sensors"

    sensor_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sensor_name: Mapped[str] = mapped_column(String(255), nullable=False)
    sensor_type: Mapped[str] = mapped_column(String(100), nullable=False)
    site_isp_assignment_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("site_isp_assignments.assignment_id", ondelete="CASCADE"), index=True
    )
    prtg_core_ip: Mapped[Optional[Any]] = mapped_column(INET)
    warning_threshold: Mapped[Optional[dict]] = mapped_column(JSONB)
    report_generation_time: Mapped[time] = mapped_column(Time, nullable=False)
    report_start_date: Mapped[Optional[date]] = mapped_column(Date)
    report_end_date: Mapped[Optional[date]] = mapped_column(Date)
    report_time_zone: Mapped[str] = mapped_column(String(100), nullable=False)

    __table_args__ = (
        CheckConstraint("sensor_id BETWEEN 1000 AND 9999", name="chk_sensor_id_4_digit"),
    )

    assignment: Mapped[Optional["SiteIspAssignment"]] = relationship(
        "SiteIspAssignment", back_populates="sensors"
    )
    alerts: Mapped[List["AlertHistory"]] = relationship("AlertHistory", back_populates="sensor")
    logs: Mapped[List["SensorLog"]] = relationship(
        "SensorLog", back_populates="sensor", cascade="all, delete-orphan"
    )


class SensorLog(Base):
    __tablename__ = "sensor_logs"

    log_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    sensor_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("sensors.sensor_id", ondelete="CASCADE"), index=True
    )
    log_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        server_default=func.now(),
        index=True  # Note: PostgreSQL default order is ASC. For DESC-specific indexing, use __table_args__ with Index()
    )
    log_level: Mapped[str] = mapped_column(String(20), nullable=False)
    log_message: Mapped[Optional[str]] = mapped_column(Text)
    log_details: Mapped[Optional[dict]] = mapped_column(JSONB)

    sensor: Mapped[Optional["Sensor"]] = relationship("Sensor", back_populates="logs")


# =========================================================================
# 4. ALERT TABLES
# =========================================================================

class AlertState(Base):
    __tablename__ = "alert_states"

    state_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    state_name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)

    alerts: Mapped[List["AlertHistory"]] = relationship("AlertHistory", back_populates="state")


class AlertHistory(Base):
    __tablename__ = "alert_history"

    alert_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sensor_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("sensors.sensor_id", ondelete="RESTRICT"), index=True
    )
    state_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("alert_states.state_id"), index=True
    )
    triggered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        server_default=func.now()
    )
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    alert_message: Mapped[Optional[str]] = mapped_column(Text)
    escalation_status: Mapped[Optional[str]] = mapped_column(String(50))

    sensor: Mapped[Optional["Sensor"]] = relationship("Sensor", back_populates="alerts")
    state: Mapped[Optional["AlertState"]] = relationship("AlertState", back_populates="alerts")
    diagnostics: Mapped[List["PingDiagnostic"]] = relationship(
        "PingDiagnostic", back_populates="alert", cascade="all, delete-orphan"
    )
    escalations: Mapped[List["EscalationRecord"]] = relationship(
        "EscalationRecord", back_populates="alert", cascade="all, delete-orphan"
    )


class PingDiagnostic(Base):
    __tablename__ = "ping_diagnostics"

    ping_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    alert_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("alert_history.alert_id", ondelete="CASCADE"), index=True
    )
    packet_count: Mapped[int] = mapped_column(Integer, nullable=False)
    packet_loss_percent: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    min_rtt_ms: Mapped[Optional[Decimal]] = mapped_column(Numeric(7, 2))
    avg_rtt_ms: Mapped[Optional[Decimal]] = mapped_column(Numeric(7, 2))
    max_rtt_ms: Mapped[Optional[Decimal]] = mapped_column(Numeric(7, 2))
    executed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        server_default=func.now()
    )
    technician_notes: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("packet_loss_percent BETWEEN 0.00 AND 100.00", name="chk_packet_loss"),
    )
    
    alert: Mapped[Optional["AlertHistory"]] = relationship("AlertHistory", back_populates="diagnostics")


class EscalationRecord(Base):
    __tablename__ = "escalation_records"

    escalation_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    alert_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("alert_history.alert_id", ondelete="CASCADE")
    )
    escalated_to: Mapped[str] = mapped_column(String(50), nullable=False)
    recipient_email: Mapped[str] = mapped_column(String(255), nullable=False)
    cc_emails: Mapped[Optional[List[str]]] = mapped_column(ARRAY(Text))
    email_subject: Mapped[Optional[str]] = mapped_column(Text)
    email_body: Mapped[Optional[str]] = mapped_column(Text)
    sent_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        server_default=func.now()
    )
    response_received: Mapped[bool] = mapped_column(Boolean, default=False)
    response_notes: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("escalated_to IN ('ISP','Tier-1')", name="chk_escalated_to"),
    )
    
    alert: Mapped[Optional["AlertHistory"]] = relationship("AlertHistory", back_populates="escalations")