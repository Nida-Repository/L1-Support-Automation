from __future__ import annotations

import contextlib
import datetime
import logging
from typing import Any, Generic, Iterable, Optional, Sequence, TypeVar

from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import (
    AlertHistory,
    AlertState,
    Base,
    EscalationRecord,
    Isp,
    IspContactEmail,
    LogLevelType,
    LogStatusType,
    PingDiagnostic,
    Sensor,
    SensorLog,
    Site,
    SiteIspAssignment,
)

# 1. Module Logger Definition
logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 500

ModelT = TypeVar("ModelT", bound=Base)


# ---------------------------------------------------------------------------
# Exceptions -- typed, so the API/service layer never has to grep DB error text
# ---------------------------------------------------------------------------

class RepositoryError(Exception):
    """Base class for all repository-layer errors."""


class NotFoundError(RepositoryError):
    def __init__(self, model: type, identifier: Any):
        self.model = model
        self.identifier = identifier
        message = f"{model.__name__} with id={identifier!r} not found"
        logger.warning(message)
        super().__init__(message)


class DuplicateError(RepositoryError):
    """Raised when a write violates a UNIQUE / PK constraint."""


class ConstraintViolationError(RepositoryError):
    """Raised for CHECK constraint or FK violations that aren't duplicates."""


def _reraise_integrity_error(exc: IntegrityError) -> None:
    """Translate a raw IntegrityError into a typed repository exception."""
    orig = str(getattr(exc, "orig", exc)).lower()
    if "unique" in orig or "duplicate key" in orig:
        err_msg = str(exc.orig) if exc.orig else str(exc)
        logger.error(f"Duplicate entry constraint violation: {err_msg}")
        raise DuplicateError(err_msg) from exc
    
    err_msg = str(exc.orig) if exc.orig else str(exc)
    logger.error(f"Database integrity constraint violation: {err_msg}")
    raise ConstraintViolationError(err_msg) from exc


def _clamp_page_size(limit: int) -> int:
    return max(1, min(limit, MAX_PAGE_SIZE))


@contextlib.contextmanager
def session_scope(session_factory):
    """
    Context manager for scripts/jobs that manage session lifecycles.
    """
    session: Session = session_factory()
    try:
        yield session
        session.commit()
        logger.debug("Database transaction committed successfully.")
    except Exception as exc:
        session.rollback()
        logger.error(f"Transaction failed, changes rolled back: {exc}")
        raise
    finally:
        session.close()
        logger.debug("Database session closed.")


# ---------------------------------------------------------------------------
# Generic base repository
# ---------------------------------------------------------------------------

class BaseRepository(Generic[ModelT]):

    model: type[ModelT]

    def __init__(self, session: Session):
        self.session = session

    # -- Create ---------------------------------------------------------

    def create(self, **fields: Any) -> ModelT:
        obj = self.model(**fields)
        self.session.add(obj)
        try:
            self.session.flush()
        except IntegrityError as exc:
            self.session.rollback()
            _reraise_integrity_error(exc)
        logger.info("Created %s: %s", self.model.__name__, obj)
        return obj

    def bulk_create(self, rows: Iterable[dict]) -> list[ModelT]:
        """Efficient multi-row insert for high-volume tables (logs, alerts)."""
        objs = [self.model(**row) for row in rows]
        self.session.add_all(objs)
        try:
            self.session.flush()
        except IntegrityError as exc:
            self.session.rollback()
            _reraise_integrity_error(exc)
        logger.info("Bulk created %d records for %s", len(objs), self.model.__name__)
        return objs

    # -- Read -------------------------------------------------------------

    def get(self, pk: Any) -> Optional[ModelT]:
        """PK lookup via the identity map -- no SQL if already loaded."""
        logger.debug("Fetching %s by PK: %s", self.model.__name__, pk)
        return self.session.get(self.model, pk)

    def get_or_404(self, pk: Any) -> ModelT:
        obj = self.get(pk)
        if obj is None:
            raise NotFoundError(self.model, pk)
        return obj

    def list(
        self,
        *,
        limit: int = DEFAULT_PAGE_SIZE,
        offset: int = 0,
        order_by: Any = None,
        **filters: Any,
    ) -> Sequence[ModelT]:
        stmt = select(self.model)
        for field, value in filters.items():
            stmt = stmt.where(getattr(self.model, field) == value)
        if order_by is not None:
            stmt = stmt.order_by(order_by)
        
        clamped_limit = _clamp_page_size(limit)
        stmt = stmt.offset(offset).limit(clamped_limit)
        
        logger.debug(
            "Listing %s (filters=%s, offset=%d, limit=%d)",
            self.model.__name__, filters, offset, clamped_limit
        )
        return self.session.execute(stmt).scalars().all()

    def count(self, **filters: Any) -> int:
        stmt = select(func.count()).select_from(self.model)
        for field, value in filters.items():
            stmt = stmt.where(getattr(self.model, field) == value)
        total = self.session.execute(stmt).scalar_one()
        logger.debug("Count for %s matching %s: %d", self.model.__name__, filters, total)
        return total

    def exists(self, **filters: Any) -> bool:
        return self.count(**filters) > 0

    # -- Update -------------------------------------------------------------

    def update(self, pk: Any, **fields: Any) -> ModelT:
        obj = self.get_or_404(pk)
        for key, value in fields.items():
            setattr(obj, key, value)
        try:
            self.session.flush()
        except IntegrityError as exc:
            self.session.rollback()
            _reraise_integrity_error(exc)
        logger.info("Updated %s(pk=%s) with fields: %s", self.model.__name__, pk, list(fields.keys()))
        return obj

    def bulk_update(self, filters: dict, values: dict) -> int:
        """Set-based UPDATE ... WHERE, no ORM objects loaded. Returns row count."""
        stmt = update(self.model).values(**values)
        for field, value in filters.items():
            stmt = stmt.where(getattr(self.model, field) == value)
        stmt = stmt.execution_options(synchronize_session="fetch")
        result = self.session.execute(stmt)
        logger.info("Bulk updated %d rows in %s matching %s", result.rowcount, self.model.__name__, filters)
        return result.rowcount

    # -- Delete -------------------------------------------------------------

    def delete(self, pk: Any) -> None:
        obj = self.get_or_404(pk)
        self.session.delete(obj)
        try:
            self.session.flush()
        except IntegrityError as exc:
            self.session.rollback()
            _reraise_integrity_error(exc)
        logger.info("Deleted %s(id=%r)", self.model.__name__, pk)

    def bulk_delete(self, **filters: Any) -> int:
        stmt = delete(self.model)
        for field, value in filters.items():
            stmt = stmt.where(getattr(self.model, field) == value)
        stmt = stmt.execution_options(synchronize_session="fetch")
        result = self.session.execute(stmt)
        logger.info("Bulk deleted %d rows from %s", result.rowcount, self.model.__name__)
        return result.rowcount


# ---------------------------------------------------------------------------
# Domain repositories
# ---------------------------------------------------------------------------

class SiteRepository(BaseRepository[Site]):
    model = Site

    def get_by_name(self, site_name: str) -> Optional[Site]:
        logger.debug("Querying Site by site_name: %s", site_name)
        stmt = select(Site).where(Site.site_name == site_name)
        return self.session.execute(stmt).scalar_one_or_none()


class IspRepository(BaseRepository[Isp]):
    model = Isp

    def search_by_name(self, name_fragment: str, *, limit: int = DEFAULT_PAGE_SIZE) -> Sequence[Isp]:
        logger.debug("Searching ISPs with query: %s", name_fragment)
        stmt = (
            select(Isp)
            .where(Isp.isp_name.ilike(f"%{name_fragment}%"))
            .order_by(Isp.isp_name)
            .limit(_clamp_page_size(limit))
        )
        return self.session.execute(stmt).scalars().all()


class IspContactEmailRepository(BaseRepository[IspContactEmail]):
    model = IspContactEmail

    def list_active_for_isp(self, isp_id: int) -> Sequence[IspContactEmail]:
        logger.debug("Fetching active contact emails for isp_id: %d", isp_id)
        stmt = (
            select(IspContactEmail)
            .where(
                IspContactEmail.isp_id == isp_id,
                IspContactEmail.is_active.is_(True),
            )
            .order_by(IspContactEmail.priority)
        )
        return self.session.execute(stmt).scalars().all()

    def deactivate(self, email_id: int) -> IspContactEmail:
        logger.info("Deactivating contact email_id: %d", email_id)
        return self.update(email_id, is_active=False)


class SiteIspAssignmentRepository(BaseRepository[SiteIspAssignment]):
    model = SiteIspAssignment

    def get_primary_for_site(self, site_id: int) -> Optional[SiteIspAssignment]:
        logger.debug("Querying primary ISP assignment for site_id: %d", site_id)
        stmt = select(SiteIspAssignment).where(
            SiteIspAssignment.site_id == site_id,
            SiteIspAssignment.is_primary_isp.is_(True),
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def list_for_site(self, site_id: int) -> Sequence[SiteIspAssignment]:
        logger.debug("Listing all ISP assignments for site_id: %d", site_id)
        stmt = select(SiteIspAssignment).where(SiteIspAssignment.site_id == site_id)
        return self.session.execute(stmt).scalars().all()


class SensorRepository(BaseRepository[Sensor]):
    model = Sensor

    def list_for_assignment(self, assignment_id: int) -> Sequence[Sensor]:
        logger.debug("Listing sensors for assignment_id: %d", assignment_id)
        stmt = select(Sensor).where(Sensor.site_isp_assignment_id == assignment_id)
        return self.session.execute(stmt).scalars().all()


class AlertStateRepository(BaseRepository[AlertState]):
    model = AlertState

    def get_by_name(self, state_name: str) -> Optional[AlertState]:
        stmt = select(AlertState).where(AlertState.state_name == state_name)
        return self.session.execute(stmt).scalar_one_or_none()


class AlertHistoryRepository(BaseRepository[AlertHistory]):
    model = AlertHistory

    def list_unresolved(
        self, *, limit: int = DEFAULT_PAGE_SIZE, offset: int = 0
    ) -> Sequence[AlertHistory]:
        logger.debug("Listing unresolved alerts (limit=%d, offset=%d)", limit, offset)
        stmt = (
            select(AlertHistory)
            .where(AlertHistory.resolved_at.is_(None))
            .order_by(AlertHistory.triggered_at.desc())
            .offset(offset)
            .limit(_clamp_page_size(limit))
        )
        return self.session.execute(stmt).scalars().all()

    def list_for_sensor(
        self,
        sensor_id: int,
        *,
        since: Optional[datetime.datetime] = None,
        limit: int = DEFAULT_PAGE_SIZE,
        offset: int = 0,
    ) -> Sequence[AlertHistory]:
        logger.debug("Listing alerts for sensor_id: %d (since=%s)", sensor_id, since)
        stmt = select(AlertHistory).where(AlertHistory.sensor_id == sensor_id)
        if since is not None:
            stmt = stmt.where(AlertHistory.triggered_at >= since)
        stmt = (
            stmt.order_by(AlertHistory.triggered_at.desc())
            .offset(offset)
            .limit(_clamp_page_size(limit))
        )
        return self.session.execute(stmt).scalars().all()

    def resolve(self, alert_id: int, *, resolved_at: Optional[datetime.datetime] = None) -> AlertHistory:
        timestamp = resolved_at or datetime.datetime.now(datetime.timezone.utc)
        logger.info("Resolving alert_id: %d at %s", alert_id, timestamp)
        return self.update(
            alert_id,
            resolved_at=timestamp,
        )


class SensorLogRepository(BaseRepository[SensorLog]):
    model = SensorLog

    def list_for_sensor(
        self,
        sensor_id: int,
        *,
        level: Optional[LogLevelType] = None,
        status: Optional[LogStatusType] = None,
        limit: int = DEFAULT_PAGE_SIZE,
        offset: int = 0,
    ) -> Sequence[SensorLog]:
        logger.debug("Fetching logs for sensor_id: %d (level=%s, status=%s)", sensor_id, level, status)
        stmt = select(SensorLog).where(SensorLog.sensor_id == sensor_id)
        if level is not None:
            stmt = stmt.where(SensorLog.log_level == level)
        if status is not None:
            stmt = stmt.where(SensorLog.log_status == status)
        stmt = (
            stmt.order_by(SensorLog.log_timestamp.desc())
            .offset(offset)
            .limit(_clamp_page_size(limit))
        )
        return self.session.execute(stmt).scalars().all()

    def close_open_logs(self, sensor_id: int) -> int:
        logger.info("Closing all open sensor logs for sensor_id: %d", sensor_id)
        return self.bulk_update(
            filters={"sensor_id": sensor_id, "log_status": LogStatusType.OPENED},
            values={"log_status": LogStatusType.CLOSED},
        )


class PingDiagnosticRepository(BaseRepository[PingDiagnostic]):
    model = PingDiagnostic

    def list_for_alert(self, alert_id: int) -> Sequence[PingDiagnostic]:
        logger.debug("Fetching ping diagnostics for alert_id: %d", alert_id)
        stmt = (
            select(PingDiagnostic)
            .where(PingDiagnostic.alert_id == alert_id)
            .order_by(PingDiagnostic.executed_at.desc())
        )
        return self.session.execute(stmt).scalars().all()


class EscalationRecordRepository(BaseRepository[EscalationRecord]):
    model = EscalationRecord

    def list_for_alert(self, alert_id: int) -> Sequence[EscalationRecord]:
        logger.debug("Fetching escalation records for alert_id: %d", alert_id)
        stmt = (
            select(EscalationRecord)
            .where(EscalationRecord.alert_id == alert_id)
            .order_by(EscalationRecord.sent_at.desc())
        )
        return self.session.execute(stmt).scalars().all()

    def mark_response_received(self, escalation_id: int, *, notes: Optional[str] = None) -> EscalationRecord:
        logger.info("Marking response received for escalation_id: %d", escalation_id)
        return self.update(escalation_id, response_received=True, response_notes=notes)


# ---------------------------------------------------------------------------
# Example usage (not executed on import)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from app.database import SessionLocal 
    from config.logging_config import setup_logging
    
    # Run central logging setup if executed directly
    setup_logging()

    logger.info("Executing repository script module...")

    with session_scope(SessionLocal) as session:
        sites = SiteRepository(session)
        alerts = AlertHistoryRepository(session)

        site = sites.get_by_name("Budapest")
        logger.info("Site query result: %s", site)
        
        if site is not None:
            open_alerts = alerts.list_unresolved(limit=20)
            for alert in open_alerts:
                logger.info("Open alert record: %s", alert)