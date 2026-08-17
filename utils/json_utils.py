"""Robust JSON Serialization and Sanitization Utilities.

Ensures seamless serialization across PostgreSQL JSONB, SQLAlchemy, Redis,
Celery, and REST payloads for non-standard JSON types including Decimal,
datetime, UUID, IPAddress, Enum, Pydantic models, and dataclasses.
"""
from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import ipaddress
import json
import pathlib
import uuid
from typing import Any, Callable, Dict, List, Union


def default_json_serializer(obj: Any) -> Any:
    """Fallback JSON serializer for types not natively handled by standard json module."""
    if isinstance(obj, decimal.Decimal):
        # Convert Decimal to float or int
        return float(obj)
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    if isinstance(obj, (uuid.UUID, pathlib.Path)):
        return str(obj)
    if isinstance(
        obj,
        (
            ipaddress.IPv4Address,
            ipaddress.IPv6Address,
            ipaddress.IPv4Network,
            ipaddress.IPv6Network,
            ipaddress.IPv4Interface,
            ipaddress.IPv6Interface,
        ),
    ):
        return str(obj)
    if isinstance(obj, enum.Enum):
        return obj.value
    if isinstance(obj, (bytes, bytearray)):
        try:
            return obj.decode("utf-8")
        except UnicodeDecodeError:
            return obj.decode("latin1", errors="replace")
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    if hasattr(obj, "model_dump"):
        return obj.model_dump(mode="json")
    if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
        return obj.dict()
    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    return str(obj)


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder handling Decimal, datetime, UUID, IP addresses, and Enums."""

    def default(self, obj: Any) -> Any:
        try:
            return default_json_serializer(obj)
        except Exception:
            return super().default(obj)


def json_dumps(obj: Any, **kwargs: Any) -> str:
    """Serializes a Python object to a JSON formatted string using CustomJSONEncoder."""
    if "default" not in kwargs:
        kwargs["default"] = default_json_serializer
    if "cls" not in kwargs:
        kwargs["cls"] = CustomJSONEncoder
    return json.dumps(obj, **kwargs)


def json_loads(s: Union[str, bytes, bytearray], **kwargs: Any) -> Any:
    """Deserializes a JSON formatted string/bytes to a Python object."""
    return json.loads(s, **kwargs)


def to_jsonable_python(obj: Any) -> Any:
    """Recursively converts any nested data structure into pure JSON-safe Python primitives.

    Converts Decimals to float, datetimes to ISO-8601 strings, enums to values,
    sets/tuples to lists, models to dicts, etc.
    """
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj

    if isinstance(obj, decimal.Decimal):
        return float(obj)

    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()

    if isinstance(
        obj,
        (
            uuid.UUID,
            pathlib.Path,
            ipaddress.IPv4Address,
            ipaddress.IPv6Address,
            ipaddress.IPv4Network,
            ipaddress.IPv6Network,
            ipaddress.IPv4Interface,
            ipaddress.IPv6Interface,
        ),
    ):
        return str(obj)

    if isinstance(obj, enum.Enum):
        return to_jsonable_python(obj.value)

    if isinstance(obj, (bytes, bytearray)):
        try:
            return obj.decode("utf-8")
        except UnicodeDecodeError:
            return obj.decode("latin1", errors="replace")

    if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
        return to_jsonable_python(obj.model_dump(mode="python"))

    if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
        return to_jsonable_python(obj.dict())

    if dataclasses.is_dataclass(obj):
        return to_jsonable_python(dataclasses.asdict(obj))

    if isinstance(obj, dict):
        return {str(k): to_jsonable_python(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set, frozenset)):
        return [to_jsonable_python(item) for item in obj]

    if hasattr(obj, "__dict__"):
        return {
            str(k): to_jsonable_python(v)
            for k, v in obj.__dict__.items()
            if not str(k).startswith("_")
        }

    return str(obj)
