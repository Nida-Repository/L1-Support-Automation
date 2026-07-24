import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in the environment.")


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


SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


def get_db():
    """
    FastAPI dependency style database session generator.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()