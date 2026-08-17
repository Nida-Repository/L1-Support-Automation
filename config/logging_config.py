"""Centralized Logging Configuration.

Configures root, application, Celery, and Uvicorn loggers with standard
formatting, console streaming, and rotating file handlers for general logs
and error-specific logs.
"""
from __future__ import annotations

import logging
import logging.config
from pathlib import Path

from config.settings import settings

LOG_DIR: Path = settings.log_dir
LOG_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_LOG_LEVEL = "DEBUG" if settings.debug else "INFO"

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": (
                "%(asctime)s | %(levelname)-8s | %(name)s | "
                "%(filename)s:%(lineno)d | %(message)s"
            ),
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": DEFAULT_LOG_LEVEL,
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": str(LOG_DIR / "app.log"),
            "maxBytes": 5 * 1024 * 1024,  # 5 MB
            "backupCount": 5,
            "formatter": "standard",
            "level": "INFO",
            "encoding": "utf-8",
        },
        "error_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": str(LOG_DIR / "error.log"),
            "maxBytes": 5 * 1024 * 1024,  # 5 MB
            "backupCount": 5,
            "formatter": "standard",
            "level": "ERROR",
            "encoding": "utf-8",
        },
    },
    "loggers": {
        "uvicorn": {
            "handlers": ["console", "file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.access": {
            "handlers": ["console", "file"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.error": {
            "handlers": ["console", "file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },
        "celery": {
            "handlers": ["console", "file", "error_file"],
            "level": DEFAULT_LOG_LEVEL,
            "propagate": False,
        },
        "celery.task": {
            "handlers": ["console", "file", "error_file"],
            "level": DEFAULT_LOG_LEVEL,
            "propagate": False,
        },
        "celery.worker": {
            "handlers": ["console", "file", "error_file"],
            "level": DEFAULT_LOG_LEVEL,
            "propagate": False,
        },
    },
    "root": {
        "handlers": ["console", "file", "error_file"],
        "level": DEFAULT_LOG_LEVEL,
    },
}

_is_logging_configured = False


def setup_logging() -> None:
    """Initialize dictConfig logging for the application.

    Idempotent: can be called multiple times without duplicate handler registration.
    """
    global _is_logging_configured
    logging.config.dictConfig(LOGGING_CONFIG)
    _is_logging_configured = True