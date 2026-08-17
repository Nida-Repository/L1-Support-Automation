"""Database Engine, Session Management, and Declarative Base.

Manages PostgreSQL connection pooling via SQLAlchemy 2.0 and provides
thread-safe session scopes and FastAPI dependency generators.
"""
from __future__ import annotations

import logging
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from config.settings import settings

from utils.json_utils import json_dumps, json_loads

logger = logging.getLogger(__name__)

logger.info("Initializing database connection engine: %s", settings.safe_database_url)

# Configure psycopg3 JSON/JSONB dumpers to seamlessly serialize Decimal, datetime, UUID, IP addresses, etc.
try:
    import psycopg.types.json

    psycopg.types.json.set_json_dumps(json_dumps)
    logger.debug("Successfully configured psycopg JSON/JSONB dumper with custom json_dumps.")
except (ImportError, AttributeError) as exc:
    logger.warning("Could not register psycopg custom JSON dumper: %s", exc)


class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base for all ORM models."""
    pass


engine = create_engine(
    settings.database_url,
    pool_pre_ping=settings.db_pool_pre_ping,
    future=True,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_recycle=settings.db_pool_recycle,
    json_serializer=json_dumps,
    json_deserializer=json_loads,
)

logger.info(
    "Database engine initialized (pool_size=%d, max_overflow=%d, pool_recycle=%ds).",
    settings.db_pool_size,
    settings.db_max_overflow,
    settings.db_pool_recycle,
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency-style database session generator.

    Yields an active database session and handles closing and error logging cleanly.
    """
    db: Session = SessionLocal()
    logger.debug("Database session opened [ID: %s]", id(db))
    try:
        yield db
    except Exception as exc:
        logger.error(
            "Unhandled exception during DB session lifecycle [ID: %s]: %s",
            id(db),
            exc,
            exc_info=True,
        )
        raise
    finally:
        db.close()
        logger.debug("Database session closed [ID: %s]", id(db))