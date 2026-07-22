# app/database.py
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in the environment.")
# SQLAlchemy Engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,   # Reconnects automatically if connection is lost
    future=True           # SQLAlchemy 2.x style
)
# Session Factory
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
)

class Base(DeclarativeBase):
    """
    Base class inherited by all SQLAlchemy models.
    Provides:
    - SQLAlchemy Declarative Base
    - Helpful object representation (__repr__) for debugging
    """
    # Fields commonly used to identify a record
    READABLE_ATTRS = (
        "site_name",
        "isp_name",
        "sensor_name",
        "state_name",
        "email_address",
        "circuit_id",
        "log_level",
        "name",
        "code",
    )
    def __repr__(self) -> str:
        """
        Returns a concise representation containing:
        - Primary Key(s)
        - Human-readable identifier (if present)
        Example:
            <Site site_id=1, site_name='Hyderabad'>
            <Sensor sensor_id=12, sensor_name='Ping Google'>
        """
        class_name = self.__class__.__name__

        try:
            # Get primary key column names
            pk_names = {column.name for column in self.__table__.primary_key.columns}
            items = []
            for column in self.__table__.columns:
                if (column.name in pk_names or column.name in self.READABLE_ATTRS):
                    value = getattr(self, column.name, None)

                    # Truncate very long strings
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:47] + "..."

                    items.append(f"{column.name}={value!r}")

            return f"<{class_name} {', '.join(items)}>"

        except Exception:
            # __repr__ should never raise an exception
            return f"<{class_name}>"