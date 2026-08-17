"""Centralized Application Configuration.

Loads configuration from environment variables and .env files, performs startup
validation, provides typed attributes with sensible defaults, and provides
utility methods for masking sensitive credentials in logs and output.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, field_validator

# Project root directory (parent of config/)
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / ".env"

if ENV_FILE.exists():
    load_dotenv(dotenv_path=ENV_FILE)
else:
    load_dotenv()


def mask_secret(secret: Optional[str], visible_chars: int = 2) -> str:
    """Safely mask a secret string for logging or debug display."""
    if not secret:
        return "[NOT SET]"
    if len(secret) <= visible_chars * 3:
        return "******"
    return f"{secret[:visible_chars]}******{secret[-visible_chars:]}"


def mask_url_password(url: Optional[str]) -> str:
    """Scrub passwords from connection URLs (DB, Redis, AMQP) before logging."""
    if not url:
        return "[NOT SET]"
    try:
        parsed = urlparse(url)
        if parsed.password:
            # Replace password portion safely
            masked_netloc = parsed.netloc.replace(f":{parsed.password}@", ":******@")
            return parsed._replace(netloc=masked_netloc).geturl()
        return url
    except Exception:
        return "[MASKED_URL]"


class Settings(BaseModel):
    """Application-wide settings with environment variable fallback and validation."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        str_strip_whitespace=True,
        extra="ignore",
    )

    # --- Project & Environment ---
    app_env: str = Field(default_factory=lambda: os.getenv("APP_ENV", "production"))
    debug: bool = Field(
        default_factory=lambda: os.getenv("DEBUG", "false").strip().lower() in ("true", "1", "yes")
    )
    project_root: Path = BASE_DIR
    log_dir: Path = BASE_DIR / "logs"

    # --- Database ---
    database_url: str = Field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/sensor_db"
        )
    )
    db_pool_size: int = Field(default_factory=lambda: int(os.getenv("DB_POOL_SIZE", "5")))
    db_max_overflow: int = Field(default_factory=lambda: int(os.getenv("DB_MAX_OVERFLOW", "5")))
    db_pool_recycle: int = Field(default_factory=lambda: int(os.getenv("DB_POOL_RECYCLE", "1800")))
    db_pool_pre_ping: bool = Field(
        default_factory=lambda: os.getenv("DB_POOL_PRE_PING", "true").strip().lower() in ("true", "1")
    )

    # --- Redis ---
    redis_url: str = Field(
        default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379/0")
    )
    redis_max_connections: int = Field(
        default_factory=lambda: int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))
    )
    redis_socket_timeout: float = Field(
        default_factory=lambda: float(os.getenv("REDIS_SOCKET_TIMEOUT", "3.0"))
    )
    redis_cache_ttl_seconds: int = Field(
        default_factory=lambda: int(os.getenv("REDIS_CACHE_TTL_SECONDS", "3600"))
    )
    redis_state_ttl_seconds: int = Field(
        default_factory=lambda: int(os.getenv("REDIS_STATE_TTL_SECONDS", "86400"))
    )

    # --- RabbitMQ / Celery Broker ---
    rabbitmq_url: str = Field(
        default_factory=lambda: os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672//")
    )
    celery_task_time_limit: int = Field(
        default_factory=lambda: int(os.getenv("CELERY_TASK_TIME_LIMIT", "120"))
    )
    celery_task_soft_time_limit: int = Field(
        default_factory=lambda: int(os.getenv("CELERY_TASK_SOFT_TIME_LIMIT", "90"))
    )

    # --- Webhook Security ---
    prtg_webhook_secret: Optional[str] = Field(
        default_factory=lambda: os.getenv("PRTG_WEBHOOK_SECRET")
    )

    # --- SMTP Configuration ---
    smtp_host: str = Field(default_factory=lambda: os.getenv("SMTP_HOST", "localhost"))
    smtp_port: int = Field(default_factory=lambda: int(os.getenv("SMTP_PORT", "587")))
    smtp_username: Optional[str] = Field(default_factory=lambda: os.getenv("SMTP_USERNAME"))
    smtp_password: Optional[str] = Field(default_factory=lambda: os.getenv("SMTP_PASSWORD"))
    smtp_use_tls: bool = Field(
        default_factory=lambda: os.getenv("SMTP_USE_TLS", "true").strip().lower() in ("true", "1")
    )
    smtp_use_ssl: bool = Field(
        default_factory=lambda: os.getenv("SMTP_USE_SSL", "false").strip().lower() in ("true", "1")
    )
    smtp_from_address: str = Field(
        default_factory=lambda: os.getenv("SMTP_FROM_ADDRESS", "noreply@example.com")
    )
    smtp_timeout_seconds: float = Field(
        default_factory=lambda: float(os.getenv("SMTP_TIMEOUT_SECONDS", "15.0"))
    )
    support_team_email: Optional[str] = Field(
        default_factory=lambda: os.getenv("SUPPORT_TEAM_EMAIL")
    )

    # --- IMAP Configuration ---
    imap_server: Optional[str] = Field(default_factory=lambda: os.getenv("IMAP_SERVER"))
    imap_port: int = Field(default_factory=lambda: int(os.getenv("IMAP_PORT", "993")))
    email_account: Optional[str] = Field(default_factory=lambda: os.getenv("EMAIL_ACCOUNT"))
    email_password: Optional[str] = Field(default_factory=lambda: os.getenv("EMAIL_PASSWORD"))

    # --- Diagnostic & Ping Service ---
    ping_diag_max_concurrent_jobs: int = Field(
        default_factory=lambda: int(os.getenv("PING_DIAG_MAX_CONCURRENT_JOBS", "5"))
    )
    ping_diag_max_concurrent_pings: int = Field(
        default_factory=lambda: int(os.getenv("PING_DIAG_MAX_CONCURRENT_PINGS", "20"))
    )
    ping_batch_count: int = Field(
        default_factory=lambda: int(os.getenv("PING_BATCH_COUNT", "10"))
    )
    pings_per_batch: int = Field(
        default_factory=lambda: int(os.getenv("PINGS_PER_BATCH", "10"))
    )
    ping_timeout_seconds: float = Field(
        default_factory=lambda: float(os.getenv("PING_TIMEOUT_SECONDS", "4.0"))
    )
    pause_between_batches_seconds: float = Field(
        default_factory=lambda: float(os.getenv("PAUSE_BETWEEN_BATCHES_SECONDS", "1.0"))
    )

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("DATABASE_URL must not be empty.")
        return v.strip()

    @property
    def safe_database_url(self) -> str:
        return mask_url_password(self.database_url)

    @property
    def safe_redis_url(self) -> str:
        return mask_url_password(self.redis_url)

    @property
    def safe_rabbitmq_url(self) -> str:
        return mask_url_password(self.rabbitmq_url)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton instance of application settings."""
    return Settings()


# Convenient global import
settings = get_settings()
