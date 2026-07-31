import logging
import os
from urllib.parse import urlparse

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

# 1. Instantiate module-level logger
logger = logging.getLogger(__name__)

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    logger.critical("DATABASE_URL environment variable is missing!")
    raise ValueError("DATABASE_URL is not set in the environment.")


def _safe_db_url(url: str) -> str:
    """Helper to scrub passwords from the connection URL for safe log output."""
    try:
        parsed = urlparse(url)
        if parsed.password:
            return url.replace(parsed.password, "******")
        return url
    except Exception:
        return "DATABASE_URL [masked]"


logger.info("Initializing database connection engine: %s", _safe_db_url(DATABASE_URL))


class Base(DeclarativeBase):
    """
    Shared SQLAlchemy declarative base.
    All ORM models inherit from this class.
    """
    pass


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    future=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=1800,
)

logger.info(
    "Database engine created successfully (pool_size=10, max_overflow=20, pool_recycle=1800s)."
)


SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


def get_db():
    """
    FastAPI dependency style database session generator.
    Yields a session and handles closing/errors cleanly.
    """
    db = SessionLocal()
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