"""Database Repository Layer.

Provides typed repositories for all domain models with centralized query building,
pagination, transaction management, and standard exception translation.
"""
from __future__ import annotations

import contextlib
import datetime
import logging
from dataclasses import dataclass
from typing import Any, Callable, Generator, Generic, Iterable, Optional, Sequence, TypeVar

from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.models import (
    AlertHistory,
    AlertState,
    Attachment,
    Base,
    EmailClassificationType,
    EmailDirectionType,
    EscalationRecord,
    Isp,
    IspContactEmail,
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
)
from utils.json_utils import to_jsonable_python

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 500

ModelT = TypeVar("ModelT", bound=Base)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class RepositoryError(Exception):
    """Base class for all repository-layer errors."""
    pass


class NotFoundError(RepositoryError):
    """Raised when an operation targets an entity that does not exist."""

    def __init__(self, model: type | str, identifier: Any):
        model_name = model.__name__ if hasattr(model, "__name__") else str(model)
        self.model_name = model_name
        self.identifier = identifier
        message = f"{model_name} with id={identifier!r} not found"
        logger.warning(message)
        super().__init__(message)


class DuplicateError(RepositoryError):
    """Raised when a write operation violates a UNIQUE or PRIMARY KEY constraint."""
    pass


class ConstraintViolationError(RepositoryError):
    """Raised for CHECK constraint or foreign key violations that are not duplicates."""
    pass


def _reraise_integrity_error(exc: IntegrityError) -> None:
    """Translate raw SQLAlchemy IntegrityError into a typed repository domain exception."""
    orig = str(getattr(exc, "orig", exc)).lower()
    err_msg = str(getattr(exc, "orig", exc))
    if "unique" in orig or "duplicate key" in orig:
        logger.error("Duplicate entry constraint violation: %s", err_msg)
        raise DuplicateError(err_msg) from exc

    logger.error("Database integrity constraint violation: %s", err_msg)
    raise ConstraintViolationError(err_msg) from exc


def _clamp_page_size(limit: int) -> int:
    return max(1, min(limit, MAX_PAGE_SIZE))


@dataclass
class Page(Generic[ModelT]):
    """Standard pagination envelope."""
    items: Sequence[ModelT]
    total: int
    limit: int
    offset: int

    @property
    def has_more(self) -> bool:
        return self.offset + len(self.items) < self.total


@contextlib.contextmanager
def session_scope(session_factory: Callable[[], Session]) -> Generator[Session, None, None]:
    """Context manager for scripts/jobs that manage independent session lifecycles."""
    session: Session = session_factory()
    try:
        yield session
        session.commit()
        logger.debug("Database transaction committed successfully.")
    except Exception as exc:
        session.rollback()
        logger.error("Transaction failed, changes rolled back: %s", exc)
        raise
    finally:
        session.close()
        logger.debug("Database session closed.")


# ---------------------------------------------------------------------------
# Generic Base Repository
# ---------------------------------------------------------------------------

class BaseRepository(Generic[ModelT]):
    """Generic repository providing standard CRUD operations."""

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

    def bulk_create(self, rows: Iterable[dict[str, Any]]) -> list[ModelT]:
        """Efficient multi-row insert for high-volume entities."""
        objs = [self.model(**row) for row in rows]
        self.session.add_all(objs)
        try:
            self.session.flush()
        except IntegrityError as exc:
            self.session.rollback()
            _reraise_integrity_error(exc)
        logger.info("Bulk created %d records for %s", len(objs), self.model.__name__)
        return objs

    # -- Read -----------------------------------------------------------

    def get(self, pk: Any) -> Optional[ModelT]:
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
            if hasattr(self.model, field):
                stmt = stmt.where(getattr(self.model, field) == value)
        if order_by is not None:
            stmt = stmt.order_by(order_by)

        clamped_limit = _clamp_page_size(limit)
        stmt = stmt.offset(offset).limit(clamped_limit)

        logger.debug(
            "Listing %s (filters=%s, offset=%d, limit=%d)",
            self.model.__name__,
            filters,
            offset,
            clamped_limit,
        )
        return self.session.execute(stmt).scalars().all()

    def count(self, **filters: Any) -> int:
        stmt = select(func.count()).select_from(self.model)
        for field, value in filters.items():
            if hasattr(self.model, field):
                stmt = stmt.where(getattr(self.model, field) == value)
        total = self.session.execute(stmt).scalar_one()
        logger.debug("Count for %s matching %s: %d", self.model.__name__, filters, total)
        return total

    def exists(self, **filters: Any) -> bool:
        return self.count(**filters) > 0

    # -- Update ---------------------------------------------------------

    def update(self, pk: Any, **fields: Any) -> ModelT:
        obj = self.get_or_404(pk)
        for key, value in fields.items():
            if hasattr(obj, key):
                setattr(obj, key, value)
        try:
            self.session.flush()
        except IntegrityError as exc:
            self.session.rollback()
            _reraise_integrity_error(exc)
        logger.info("Updated %s(pk=%s) fields: %s", self.model.__name__, pk, list(fields.keys()))
        return obj

    def bulk_update(self, filters: dict[str, Any], values: dict[str, Any]) -> int:
        stmt = update(self.model).values(**values)
        for field, value in filters.items():
            if hasattr(self.model, field):
                stmt = stmt.where(getattr(self.model, field) == value)
        stmt = stmt.execution_options(synchronize_session="fetch")
        result = self.session.execute(stmt)
        logger.info("Bulk updated %d rows in %s matching %s", result.rowcount, self.model.__name__, filters)
        return result.rowcount

    # -- Delete ---------------------------------------------------------

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
            if hasattr(self.model, field):
                stmt = stmt.where(getattr(self.model, field) == value)
        stmt = stmt.execution_options(synchronize_session="fetch")
        result = self.session.execute(stmt)
        logger.info("Bulk deleted %d rows from %s", result.rowcount, self.model.__name__)
        return result.rowcount


# ---------------------------------------------------------------------------
# Domain Repositories
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

    def create(self, **fields: Any) -> Sensor:
        if "warning_threshold" in fields and fields["warning_threshold"] is not None:
            fields["warning_threshold"] = to_jsonable_python(fields["warning_threshold"])
        return super().create(**fields)

    def update(self, pk: Any, **fields: Any) -> Sensor:
        if "warning_threshold" in fields and fields["warning_threshold"] is not None:
            fields["warning_threshold"] = to_jsonable_python(fields["warning_threshold"])
        return super().update(pk, **fields)

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

    def resolve(
        self, alert_id: int, *, resolved_at: Optional[datetime.datetime] = None
    ) -> AlertHistory:
        timestamp = resolved_at or datetime.datetime.now(datetime.timezone.utc)
        logger.info("Resolving alert_id: %d at %s", alert_id, timestamp)
        return self.update(alert_id, resolved_at=timestamp)


class SensorLogRepository(BaseRepository[SensorLog]):
    model = SensorLog

    def create(self, **fields: Any) -> SensorLog:
        if "log_details" in fields and fields["log_details"] is not None:
            fields["log_details"] = to_jsonable_python(fields["log_details"])
        return super().create(**fields)

    def bulk_create(self, rows: Iterable[dict[str, Any]]) -> list[SensorLog]:
        sanitized_rows = []
        for r in rows:
            row_dict = dict(r)
            if "log_details" in row_dict and row_dict["log_details"] is not None:
                row_dict["log_details"] = to_jsonable_python(row_dict["log_details"])
            sanitized_rows.append(row_dict)
        return super().bulk_create(sanitized_rows)

    def update(self, pk: Any, **fields: Any) -> SensorLog:
        if "log_details" in fields and fields["log_details"] is not None:
            fields["log_details"] = to_jsonable_python(fields["log_details"])
        return super().update(pk, **fields)

    def list_for_sensor(
        self,
        sensor_id: int,
        *,
        level: Optional[LogLevelType] = None,
        status: Optional[LogStatusType] = None,
        limit: int = DEFAULT_PAGE_SIZE,
        offset: int = 0,
    ) -> Sequence[SensorLog]:
        logger.debug(
            "Fetching logs for sensor_id: %d (level=%s, status=%s)", sensor_id, level, status
        )
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

    def mark_response_received(
        self, escalation_id: int, *, notes: Optional[str] = None
    ) -> EscalationRecord:
        logger.info("Marking response received for escalation_id: %d", escalation_id)
        return self.update(escalation_id, response_received=True, response_notes=notes)


class IspEmailThreadRepository(BaseRepository[IspEmailThread]):
    model = IspEmailThread

    def get_by_message_id(self, message_id: str) -> Optional[IspEmailThread]:
        stmt = (
            select(IspEmailThread)
            .options(selectinload(IspEmailThread.attachments))
            .where(IspEmailThread.message_id == message_id)
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_with_attachments(self, thread_id: int) -> Optional[IspEmailThread]:
        stmt = (
            select(IspEmailThread)
            .options(selectinload(IspEmailThread.attachments))
            .where(IspEmailThread.thread_id == thread_id)
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_reply_chain(self, in_reply_to: str) -> Sequence[IspEmailThread]:
        stmt = (
            select(IspEmailThread)
            .options(selectinload(IspEmailThread.attachments))
            .where(IspEmailThread.in_reply_to == in_reply_to)
            .order_by(IspEmailThread.sent_received_at.asc())
        )
        return self.session.execute(stmt).scalars().all()

    def list_for_alert(
        self,
        alert_id: int,
        *,
        direction: Optional[EmailDirectionType] = None,
        classification_type: Optional[EmailClassificationType] = None,
        limit: int = DEFAULT_PAGE_SIZE,
        offset: int = 0,
    ) -> Page[IspEmailThread]:
        stmt = (
            select(IspEmailThread)
            .options(selectinload(IspEmailThread.attachments))
            .where(IspEmailThread.alert_id == alert_id)
        )
        if direction is not None:
            stmt = stmt.where(IspEmailThread.direction == direction)
        if classification_type is not None:
            stmt = stmt.where(IspEmailThread.classification_type == classification_type)

        count_stmt = select(IspEmailThread.thread_id).where(IspEmailThread.alert_id == alert_id)
        if direction is not None:
            count_stmt = count_stmt.where(IspEmailThread.direction == direction)
        if classification_type is not None:
            count_stmt = count_stmt.where(IspEmailThread.classification_type == classification_type)

        total = self.session.execute(
            select(func.count()).select_from(count_stmt.subquery())
        ).scalar_one()

        clamped_limit = _clamp_page_size(limit)
        stmt = (
            stmt.order_by(IspEmailThread.sent_received_at.asc())
            .limit(clamped_limit)
            .offset(offset)
        )
        items = self.session.execute(stmt).scalars().all()
        return Page(items=items, total=total, limit=clamped_limit, offset=offset)

    def update_classification(
        self,
        thread_id: int,
        classification_type: EmailClassificationType,
    ) -> IspEmailThread:
        logger.info("Updating email thread classification for thread_id=%d to %s", thread_id, classification_type.value)
        return self.update(thread_id, classification_type=classification_type)


class ReminderHistoryRepository(BaseRepository[ReminderHistory]):
    model = ReminderHistory

    def list_for_alert(
        self, alert_id: int, *, limit: int = DEFAULT_PAGE_SIZE, offset: int = 0
    ) -> Page[ReminderHistory]:
        stmt = select(ReminderHistory).where(ReminderHistory.alert_id == alert_id)
        total = self.session.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()

        clamped_limit = _clamp_page_size(limit)
        stmt = (
            stmt.order_by(ReminderHistory.reminder_number.asc())
            .limit(clamped_limit)
            .offset(offset)
        )
        items = self.session.execute(stmt).scalars().all()
        return Page(items=items, total=total, limit=clamped_limit, offset=offset)

    def list_pending(
        self,
        *,
        status: ReminderStatusType = ReminderStatusType.SENT,
        limit: int = 100,
        offset: int = 0,
    ) -> Page[ReminderHistory]:
        stmt = select(ReminderHistory).where(
            ReminderHistory.status == status,
            ReminderHistory.response_received.is_(False),
        )
        total = self.session.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()

        clamped_limit = _clamp_page_size(limit)
        stmt = (
            stmt.order_by(ReminderHistory.sent_at.asc())
            .limit(clamped_limit)
            .offset(offset)
        )
        items = self.session.execute(stmt).scalars().all()
        return Page(items=items, total=total, limit=clamped_limit, offset=offset)

    def mark_responded(
        self,
        reminder_id: int,
        *,
        response_received_at: Optional[datetime.datetime] = None,
    ) -> ReminderHistory:
        timestamp = response_received_at or datetime.datetime.now(datetime.timezone.utc)
        return self.update(
            reminder_id,
            response_received=True,
            response_received_at=timestamp,
        )


class RootCauseRepository(BaseRepository[RootCause]):
    model = RootCause

    def get_by_alert(self, alert_id: int) -> Optional[RootCause]:
        stmt = select(RootCause).where(RootCause.alert_id == alert_id)
        return self.session.execute(stmt).scalar_one_or_none()

    def upsert_for_alert(
        self,
        *,
        alert_id: int,
        root_cause_name: str,
        category: str,
        identified_by: str,
        description: Optional[str] = None,
        customer_confirmed: bool = False,
        total_downtime: Optional[datetime.timedelta] = None,
    ) -> RootCause:
        existing = self.get_by_alert(alert_id)
        if existing is None:
            return self.create(
                alert_id=alert_id,
                root_cause_name=root_cause_name,
                category=category,
                identified_by=identified_by,
                description=description,
                customer_confirmed=customer_confirmed,
                total_downtime=total_downtime,
            )

        return self.update(
            existing.root_cause_id,
            root_cause_name=root_cause_name,
            category=category,
            identified_by=identified_by,
            description=description,
            customer_confirmed=customer_confirmed,
            total_downtime=total_downtime,
        )

    def confirm(self, root_cause_id: int) -> RootCause:
        return self.update(root_cause_id, customer_confirmed=True)


class AttachmentRepository(BaseRepository[Attachment]):
    model = Attachment

    def get_by_object_key(self, object_key: str) -> Optional[Attachment]:
        stmt = select(Attachment).where(Attachment.object_key == object_key)
        return self.session.execute(stmt).scalar_one_or_none()

    def list_for_alert(
        self, alert_id: int, *, limit: int = DEFAULT_PAGE_SIZE, offset: int = 0
    ) -> Page[Attachment]:
        stmt = select(Attachment).where(Attachment.alert_id == alert_id)
        total = self.session.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()

        clamped_limit = _clamp_page_size(limit)
        stmt = (
            stmt.order_by(Attachment.uploaded_at.desc())
            .limit(clamped_limit)
            .offset(offset)
        )
        items = self.session.execute(stmt).scalars().all()
        return Page(items=items, total=total, limit=clamped_limit, offset=offset)

    def list_for_thread(self, thread_id: int) -> Sequence[Attachment]:
        stmt = (
            select(Attachment)
            .where(Attachment.thread_id == thread_id)
            .order_by(Attachment.uploaded_at.asc())
        )
        return self.session.execute(stmt).scalars().all()