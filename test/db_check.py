"""Database Connectivity and Schema Verification Utility.

Verifies PostgreSQL database connection, runs reflection checks, and performs
ORM smoke test. Run manually by operators during deployment.
"""
from __future__ import annotations

import logging
import sys

from sqlalchemy import select, text

from app.database import Base, SessionLocal, engine
from app.models import AlertState, Isp, Sensor, Site
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger("db_check")


def verify_setup() -> None:
    logger.info("Starting Database and Model Verification...")

    # Test 1: Connection test
    try:
        logger.info("Testing database connection...")
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            logger.info("Database connection successful! PostgreSQL Version: %s", version)
    except Exception as e:
        logger.error("Connection failed! Check configuration settings and ensure PostgreSQL is running.")
        logger.debug("Error Details: %s", e)
        sys.exit(1)

    # Test 2: Reflect/Create Table Schemas
    try:
        logger.info("Verifying table metadata synchronization...")
        Base.metadata.create_all(bind=engine)
        logger.info("Models and table definitions successfully synchronized.")
    except Exception as e:
        logger.error("Schema synchronization failed! Inspect ORM models.")
        logger.debug("Error Details: %s", e)
        sys.exit(1)

    # Test 3: Session Operations Test
    try:
        logger.info("Testing basic query operations via ORM...")
        with SessionLocal() as session:
            states_count = session.execute(select(AlertState)).scalars().all()
            logger.info("ORM query successful. Current AlertState row count: %d", len(states_count))
        logger.info("Database setup verification completed successfully.")
    except Exception as e:
        logger.error("ORM Query Test failed.")
        logger.debug("Error Details: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    verify_setup()