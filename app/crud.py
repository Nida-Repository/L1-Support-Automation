
from __future__ import annotations

import contextlib
import datetime
import logging
from typing import Any, Generic, Iterable, Optional, Sequence, TypeVar

from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import  AlertHistory, AlertState, Base, EscalationRecord, Isp, IspContactEmail, LogLevelType, LogStatusType, PingDiagnostic, Sensor, SensorLog, Site, SiteIspAssignment

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
        super().__init__(f"{model.__name__} with id={identifier!r} not found")


class DuplicateError(RepositoryError):
    """Raised when a write violates a UNIQUE / PK constraint."""


class ConstraintViolationError(RepositoryError):
    """Raised for CHECK constraint or FK violations that aren't duplicates."""


def _reraise_integrity_error(exc: IntegrityError) -> None:
    """Translate a raw IntegrityError into a typed repository exception."""
    orig = str(getattr(exc, "orig", exc)).lower()
    if "unique" in orig or "duplicate key" in orig:
        raise DuplicateError(str(exc.orig) if exc.orig else str(exc)) from exc
    raise ConstraintViolationError(str(exc.orig) if exc.orig else str(exc)) from exc


def _clamp_page_size(limit: int) -> int:
    return max(1, min(limit, MAX_PAGE_SIZE))


@contextlib.contextmanager
def session_scope(session_factory):
    """
    context manager for scripts/jobs that just want:

        with session_scope(SessionLocal) as session:
            repo = SiteRepository(session)
            repo.create(site_id=1000, site_name="HQ", ...)

    Commits on success, rolls back and re-raises on any exception.
    Not used by web frameworks with their own request-scoped session/unit of
    work -- there, pass the request's `Session` directly into the repos.
    """
    session: Session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


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
        logger.info("Created %s", obj)
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
        return objs

    # -- Read -------------------------------------------------------------

    def get(self, pk: Any) -> Optional[ModelT]:
        """PK lookup via the identity map -- no SQL if already loaded."""
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
        stmt = stmt.offset(offset).limit(_clamp_page_size(limit))
        return self.session.execute(stmt).scalars().all()

    def count(self, **filters: Any) -> int:
        stmt = select(func.count()).select_from(self.model)
        for field, value in filters.items():
            stmt = stmt.where(getattr(self.model, field) == value)
        return self.session.execute(stmt).scalar_one()

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
        logger.info("Updated %s", obj)
        return obj

    def bulk_update(self, filters: dict, values: dict) -> int:
        """Set-based UPDATE ... WHERE, no ORM objects loaded. Returns row count."""
        stmt = update(self.model).values(**values)
        for field, value in filters.items():
            stmt = stmt.where(getattr(self.model, field) == value)
        stmt = stmt.execution_options(synchronize_session="fetch")
        result = self.session.execute(stmt)
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
        logger.info("Deleted %s(%r)", self.model.__name__, pk)

    def bulk_delete(self, **filters: Any) -> int:
        stmt = delete(self.model)
        for field, value in filters.items():
            stmt = stmt.where(getattr(self.model, field) == value)
        stmt = stmt.execution_options(synchronize_session="fetch")
        result = self.session.execute(stmt)
        return result.rowcount


# ---------------------------------------------------------------------------
# Domain repositories
# ---------------------------------------------------------------------------

class SiteRepository(BaseRepository[Site]):
    model = Site

    def get_by_name(self, site_name: str) -> Optional[Site]:
        stmt = select(Site).where(Site.site_name == site_name)
        return self.session.execute(stmt).scalar_one_or_none()


class IspRepository(BaseRepository[Isp]):
    model = Isp

    def search_by_name(self, name_fragment: str, *, limit: int = DEFAULT_PAGE_SIZE) -> Sequence[Isp]:
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
        return self.update(email_id, is_active=False)


class SiteIspAssignmentRepository(BaseRepository[SiteIspAssignment]):
    model = SiteIspAssignment

    def get_primary_for_site(self, site_id: int) -> Optional[SiteIspAssignment]:
        stmt = select(SiteIspAssignment).where(
            SiteIspAssignment.site_id == site_id,
            SiteIspAssignment.is_primary_isp.is_(True),
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def list_for_site(self, site_id: int) -> Sequence[SiteIspAssignment]:
        stmt = select(SiteIspAssignment).where(SiteIspAssignment.site_id == site_id)
        return self.session.execute(stmt).scalars().all()


class SensorRepository(BaseRepository[Sensor]):
    model = Sensor

    def list_for_assignment(self, assignment_id: int) -> Sequence[Sensor]:
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
        """
        Uses the partial index `idx_alert_history_unresolved`
        (state_id, triggered_at DESC) WHERE resolved_at IS NULL.
        """
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
        """Uses idx_alert_history_sensor_time (sensor_id, triggered_at DESC)."""
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
        return self.update(
            alert_id,
            resolved_at=resolved_at or datetime.datetime.now(datetime.timezone.utc),
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
        """Uses idx_sensor_logs_sensor_time (sensor_id, log_timestamp DESC)."""
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
        """Set-based close of every open log for a sensor. Returns rows affected."""
        return self.bulk_update(
            filters={"sensor_id": sensor_id, "log_status": LogStatusType.OPENED},
            values={"log_status": LogStatusType.CLOSED},
        )


class PingDiagnosticRepository(BaseRepository[PingDiagnostic]):
    model = PingDiagnostic

    def list_for_alert(self, alert_id: int) -> Sequence[PingDiagnostic]:
        """Index-only scan via idx_ping_diagnostics_alert_covered."""
        stmt = (
            select(PingDiagnostic)
            .where(PingDiagnostic.alert_id == alert_id)
            .order_by(PingDiagnostic.executed_at.desc())
        )
        return self.session.execute(stmt).scalars().all()


class EscalationRecordRepository(BaseRepository[EscalationRecord]):
    model = EscalationRecord

    def list_for_alert(self, alert_id: int) -> Sequence[EscalationRecord]:
        stmt = (
            select(EscalationRecord)
            .where(EscalationRecord.alert_id == alert_id)
            .order_by(EscalationRecord.sent_at.desc())
        )
        return self.session.execute(stmt).scalars().all()

    def mark_response_received(self, escalation_id: int, *, notes: Optional[str] = None) -> EscalationRecord:
        return self.update(escalation_id, response_received=True, response_notes=notes)


# ---------------------------------------------------------------------------
# Example usage (not executed on import)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from app.database import SessionLocal  
    logging.basicConfig(level=logging.INFO)

    with session_scope(SessionLocal) as session:
        sites = SiteRepository(session)
        alerts = AlertHistoryRepository(session)

        site = sites.get_by_name("Budapest")
        print(f"Site found: {site}")
        if site is not None:
            open_alerts = alerts.list_unresolved(limit=20)
            for alert in open_alerts:
                logger.info("Open alert: %s", alert)