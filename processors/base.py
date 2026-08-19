"""Shared Workflow Utilities for Sensor Alert Processors.

Contains payload extraction, normalization, and JSON serialization helpers
used across all status processor workflows.
"""
from __future__ import annotations

import json
from typing import Any, Optional
from utils.json_utils import json_dumps, json_loads, to_jsonable_python


def extract_sensor_id(payload: Any) -> Optional[int]:
    """Extract sensor_id as an integer from a Pydantic model, dictionary, or generic object."""
    raw_id = None
    if hasattr(payload, "sensor_id"):
        raw_id = payload.sensor_id
    elif isinstance(payload, dict):
        raw_id = payload.get("sensor_id") or payload.get("sensorid")
    else:
        raw_id = getattr(payload, "sensor_id", None)

    if raw_id is not None:
        try:
            return int(raw_id)
        except (ValueError, TypeError):
            return None
    return None


def extract_field(payload: Any, field_name: str, default: Any = None) -> Any:
    """Safely extract a named field value from a Pydantic model, dictionary, or object."""
    if isinstance(payload, dict):
        return payload.get(field_name, default)
    if hasattr(payload, field_name):
        val = getattr(payload, field_name)
        return val if val is not None else default
    return default


def sanitize_status(raw_status: Any, default: str = "Unknown") -> str:
    """Normalize status enum / string into a clean title-case string."""
    if raw_status is None:
        return default
    if hasattr(raw_status, "value"):
        return str(raw_status.value)
    clean_str = str(raw_status).replace("SensorStatus.", "").strip()
    return clean_str or default


def serialize_payload_for_json(payload: Any) -> dict[str, Any]:
    """Convert arbitrary payload objects (Pydantic v1/v2, dataclass, dict) into a JSON-safe dict."""
    safe_data = to_jsonable_python(payload)
    if isinstance(safe_data, dict):
        return safe_data
    return {"raw": safe_data}


def json_safe_dict(data: Any) -> Any:
    """Recursively convert any data structure into JSON-safe Python primitives."""
    return to_jsonable_python(data)

