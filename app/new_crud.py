"""Backward compatibility bridge for new_crud functions.

Delegates all operations to the typed repositories in app.crud.
"""
from __future__ import annotations

import datetime
from typing import Iterable, Optional, Sequence

from sqlalchemy.orm import Session

from app.crud import (
    AttachmentRepository,
    ConstraintViolationError,
    DuplicateError,
    IspEmailThreadRepository,
    NotFoundError,
    Page,
    ReminderHistoryRepository,
    RepositoryError,
    RootCauseRepository,
)
from app.models import (
    Attachment,
    EmailClassificationType,
    EmailDirectionType,
    IspEmailThread,
    ReminderHistory,
    ReminderStatusType,
    RootCause,
)

# Exception aliases for legacy imports
CrudError = RepositoryError
EntityNotFoundError = NotFoundError
DuplicateEntityError = DuplicateError
ForeignKeyViolationError = ConstraintViolationError


# ===========================================================================
# ISP Email Threads
# ===========================================================================

def create_email_thread(
    db: Session,
    *,
    alert_id: int,
    message_id: str,
    sender: str,
    receiver: str,
    direction: EmailDirectionType,
    in_reply_to: Optional[str] = None,
    email_references: Optional[Iterable[str]] = None,
    subject: Optional[str] = None,
    cc: Optional[Iterable[str]] = None,
    body: Optional[str] = None,
    classification_type: EmailClassificationType = EmailClassificationType.UNKNOWN,
    sent_received_at: Optional[datetime.datetime] = None,
    commit: bool = True,
) -> IspEmailThread:
    repo = IspEmailThreadRepository(db)
    fields = {
        "alert_id": alert_id,
        "message_id": message_id,
        "sender": sender,
        "receiver": receiver,
        "direction": direction,
        "in_reply_to": in_reply_to,
        "email_references": list(email_references) if email_references else None,
        "subject": subject,
        "cc": list(cc) if cc else None,
        "body": body,
        "classification_type": classification_type,
    }
    if sent_received_at is not None:
        fields["sent_received_at"] = sent_received_at

    obj = repo.create(**fields)
    if commit:
        db.commit()
    return obj


def get_email_thread(db: Session, thread_id: int) -> Optional[IspEmailThread]:
    return IspEmailThreadRepository(db).get(thread_id)


def get_email_thread_by_message_id(db: Session, message_id: str) -> Optional[IspEmailThread]:
    return IspEmailThreadRepository(db).get_by_message_id(message_id)


def get_reply_chain(db: Session, in_reply_to: str) -> Sequence[IspEmailThread]:
    return IspEmailThreadRepository(db).get_reply_chain(in_reply_to)


def list_email_threads_by_alert(
    db: Session,
    alert_id: int,
    *,
    direction: Optional[EmailDirectionType] = None,
    classification_type: Optional[EmailClassificationType] = None,
    limit: int = 50,
    offset: int = 0,
) -> Page[IspEmailThread]:
    return IspEmailThreadRepository(db).list_for_alert(
        alert_id,
        direction=direction,
        classification_type=classification_type,
        limit=limit,
        offset=offset,
    )


def update_email_thread(
    db: Session,
    thread_id: int,
    *,
    subject: Optional[str] = None,
    body: Optional[str] = None,
    classification_type: Optional[EmailClassificationType] = None,
    cc: Optional[Iterable[str]] = None,
    commit: bool = True,
) -> IspEmailThread:
    repo = IspEmailThreadRepository(db)
    fields = {}
    if subject is not None:
        fields["subject"] = subject
    if body is not None:
        fields["body"] = body
    if classification_type is not None:
        fields["classification_type"] = classification_type
    if cc is not None:
        fields["cc"] = list(cc)

    obj = repo.update(thread_id, **fields)
    if commit:
        db.commit()
    return obj


def delete_email_thread(db: Session, thread_id: int, *, commit: bool = True) -> None:
    IspEmailThreadRepository(db).delete(thread_id)
    if commit:
        db.commit()


# ===========================================================================
# Reminder History
# ===========================================================================

def create_reminder(
    db: Session,
    *,
    alert_id: int,
    reminder_number: int,
    email_id: Optional[int] = None,
    status: ReminderStatusType = ReminderStatusType.SENT,
    sent_at: Optional[datetime.datetime] = None,
    commit: bool = True,
) -> ReminderHistory:
    if reminder_number <= 0:
        raise ValueError("reminder_number must be > 0")

    repo = ReminderHistoryRepository(db)
    fields = {
        "alert_id": alert_id,
        "reminder_number": reminder_number,
        "email_id": email_id,
        "status": status,
    }
    if sent_at is not None:
        fields["sent_at"] = sent_at

    obj = repo.create(**fields)
    if commit:
        db.commit()
    return obj


def get_reminder(db: Session, reminder_id: int) -> Optional[ReminderHistory]:
    return ReminderHistoryRepository(db).get(reminder_id)


def list_reminders_by_alert(
    db: Session, alert_id: int, *, limit: int = 50, offset: int = 0
) -> Page[ReminderHistory]:
    return ReminderHistoryRepository(db).list_for_alert(alert_id, limit=limit, offset=offset)


def list_pending_reminders(
    db: Session,
    *,
    status: ReminderStatusType = ReminderStatusType.SENT,
    limit: int = 100,
    offset: int = 0,
) -> Page[ReminderHistory]:
    return ReminderHistoryRepository(db).list_pending(status=status, limit=limit, offset=offset)


def mark_reminder_responded(
    db: Session,
    reminder_id: int,
    *,
    response_received_at: Optional[datetime.datetime] = None,
    commit: bool = True,
) -> ReminderHistory:
    repo = ReminderHistoryRepository(db)
    obj = repo.mark_responded(reminder_id, response_received_at=response_received_at)
    if commit:
        db.commit()
    return obj


def update_reminder_status(
    db: Session,
    reminder_id: int,
    status: ReminderStatusType,
    *,
    commit: bool = True,
) -> ReminderHistory:
    repo = ReminderHistoryRepository(db)
    obj = repo.update(reminder_id, status=status)
    if commit:
        db.commit()
    return obj


def delete_reminder(db: Session, reminder_id: int, *, commit: bool = True) -> None:
    ReminderHistoryRepository(db).delete(reminder_id)
    if commit:
        db.commit()


# ===========================================================================
# Root Cause
# ===========================================================================

def create_root_cause(
    db: Session,
    *,
    alert_id: int,
    root_cause_name: str,
    category: str,
    identified_by: str,
    description: Optional[str] = None,
    customer_confirmed: bool = False,
    total_downtime: Optional[datetime.timedelta] = None,
    commit: bool = True,
) -> RootCause:
    repo = RootCauseRepository(db)
    obj = repo.create(
        alert_id=alert_id,
        root_cause_name=root_cause_name,
        category=category,
        identified_by=identified_by,
        description=description,
        customer_confirmed=customer_confirmed,
        total_downtime=total_downtime,
    )
    if commit:
        db.commit()
    return obj


def get_root_cause(db: Session, root_cause_id: int) -> Optional[RootCause]:
    return RootCauseRepository(db).get(root_cause_id)


def get_root_cause_by_alert(db: Session, alert_id: int) -> Optional[RootCause]:
    return RootCauseRepository(db).get_by_alert(alert_id)


def upsert_root_cause(
    db: Session,
    *,
    alert_id: int,
    root_cause_name: str,
    category: str,
    identified_by: str,
    description: Optional[str] = None,
    customer_confirmed: bool = False,
    total_downtime: Optional[datetime.timedelta] = None,
    commit: bool = True,
) -> RootCause:
    repo = RootCauseRepository(db)
    obj = repo.upsert_for_alert(
        alert_id=alert_id,
        root_cause_name=root_cause_name,
        category=category,
        identified_by=identified_by,
        description=description,
        customer_confirmed=customer_confirmed,
        total_downtime=total_downtime,
    )
    if commit:
        db.commit()
    return obj


def confirm_root_cause(db: Session, root_cause_id: int, *, commit: bool = True) -> RootCause:
    repo = RootCauseRepository(db)
    obj = repo.confirm(root_cause_id)
    if commit:
        db.commit()
    return obj


def delete_root_cause(db: Session, root_cause_id: int, *, commit: bool = True) -> None:
    RootCauseRepository(db).delete(root_cause_id)
    if commit:
        db.commit()


# ===========================================================================
# Attachments
# ===========================================================================

def create_attachment(
    db: Session,
    *,
    alert_id: int,
    file_name: str,
    file_type: str,
    file_size: int,
    bucket_name: str,
    object_key: str,
    uploaded_by: str,
    thread_id: Optional[int] = None,
    etag: Optional[str] = None,
    commit: bool = True,
) -> Attachment:
    if file_size <= 0:
        raise ValueError("file_size must be > 0")

    repo = AttachmentRepository(db)
    obj = repo.create(
        alert_id=alert_id,
        thread_id=thread_id,
        file_name=file_name,
        file_type=file_type,
        file_size=file_size,
        bucket_name=bucket_name,
        object_key=object_key,
        etag=etag,
        uploaded_by=uploaded_by,
    )
    if commit:
        db.commit()
    return obj


def get_attachment(db: Session, attachment_id: int) -> Optional[Attachment]:
    return AttachmentRepository(db).get(attachment_id)


def get_attachment_by_object_key(db: Session, object_key: str) -> Optional[Attachment]:
    return AttachmentRepository(db).get_by_object_key(object_key)


def list_attachments_by_alert(
    db: Session, alert_id: int, *, limit: int = 50, offset: int = 0
) -> Page[Attachment]:
    return AttachmentRepository(db).list_for_alert(alert_id, limit=limit, offset=offset)


def list_attachments_by_thread(db: Session, thread_id: int) -> Sequence[Attachment]:
    return AttachmentRepository(db).list_for_thread(thread_id)


def delete_attachment(db: Session, attachment_id: int, *, commit: bool = True) -> None:
    AttachmentRepository(db).delete(attachment_id)
    if commit:
        db.commit()