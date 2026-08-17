import logging
from datetime import datetime, timezone
from app.database import SessionLocal, engine
from sqlalchemy import text

# Import existing and new models
from app.models import AlertHistory, IspContactEmail
from app.new_models import IspEmailThread, ReminderHistory, RootCause, Attachment
from app.new_crud import (
    create_email_thread,
    get_email_thread,
    create_reminder,
    mark_reminder_responded,
    upsert_root_cause,
    create_attachment,
    EmailDirectionType,
    EmailClassificationType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_tests():
    logger.info("--- STARTING SYSTEM INTEGRATION TEST ---")

    # 1. Test raw database connection
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            logger.info("✅ Database connectivity verified (SELECT 1 = %s)", result)
    except Exception as e:
        logger.error("❌ Database connection failed: %s", e)
        return

    # 2. Test ORM Session & Cross-Model Integration
    session = SessionLocal()
    try:
        # Fetch an existing alert_id from ALERT_HISTORY table
        existing_alert = session.query(AlertHistory).first()

        if not existing_alert:
            logger.warning(
                "⚠️ No existing records found in 'ALERT_HISTORY'. "
                "Insert at least one row into 'ALERT_HISTORY' manually to perform complete foreign key tests."
            )
            return

        target_alert_id = existing_alert.alert_id
        logger.info("Found existing Alert ID: %s", target_alert_id)

        # A. Test Email Thread Creation (new_crud)
        thread = create_email_thread(
            db=session,
            alert_id=target_alert_id,
            message_id=f"<test-{datetime.now(timezone.utc).timestamp()}@domain.com>",
            sender="noc@isp.com",
            receiver="support@company.com",
            direction=EmailDirectionType.INCOMING,
            subject="Integration Test Thread",
            classification_type=EmailClassificationType.TECHNICAL_ISSUE,
            commit=False,  # Flush only, we will rollback at the end
        )
        logger.info("✅ Created Email Thread (ID: %s)", thread.thread_id)

        # B. Test Dynamic Relationship Access (models.py -> new_models.py)
        # Verify AlertHistory model can access new email_threads relationship
        session.flush()
        assert thread in existing_alert.email_threads
        logger.info("✅ Dynamic Relationship Verified: AlertHistory.email_threads linked successfully.")

        # C. Test Reminder Creation & Response
        reminder = create_reminder(
            db=session,
            alert_id=target_alert_id,
            reminder_number=1,
            commit=False,
        )
        logger.info("✅ Created Reminder (ID: %s)", reminder.reminder_id)

        updated_reminder = mark_reminder_responded(
            db=session,
            reminder_id=reminder.reminder_id,
            response_received_at=datetime.now(timezone.utc),
            commit=False,
        )
        logger.info("✅ Reminder Responded Status: %s", updated_reminder.response_received)

        # D. Test Root Cause Upsert (1:1 relationship)
        root_cause = upsert_root_cause(
            db=session,
            alert_id=target_alert_id,
            root_cause_name="Fiber Cut",
            category="Physical Infrastructure",
            identified_by="ISP Field Tech",
            commit=False,
        )
        logger.info("✅ Created Root Cause (ID: %s)", root_cause.root_cause_id)

        # E. Test Attachment Creation
        attachment = create_attachment(
            db=session,
            alert_id=target_alert_id,
            thread_id=thread.thread_id,
            file_name="traceroute.log",
            file_type="text/plain",
            file_size=2048,
            bucket_name="isp-logs-bucket",
            object_key=f"logs/test_{datetime.now(timezone.utc).timestamp()}.log",
            uploaded_by="System Test",
            commit=False,
        )
        logger.info("✅ Created Attachment (ID: %s)", attachment.attachment_id)

        logger.info("🎉 ALL TESTS PASSED SUCCESSFULLY!")

    except Exception as exc:
        logger.error("❌ Integration Test Failed: %s", exc, exc_info=True)
    finally:
        # Roll back all dummy insertions so test data doesn't clutter your production database
        session.rollback()
        session.close()
        logger.info("Database session rolled back cleanly.")


if __name__ == "__main__":
    run_tests()