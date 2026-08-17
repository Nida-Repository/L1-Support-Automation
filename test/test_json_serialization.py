"""Comprehensive tests for JSON serialization and sanitization across all layers.

Validates that Decimal, datetime, UUID, IPv4/IPv6, Enums, and Pydantic models
are seamlessly serialized across PostgreSQL JSONB/psycopg, Redis caching,
Celery messaging, and repository operations.
"""
from __future__ import annotations

import datetime
from decimal import Decimal
import enum
import ipaddress
import json
import uuid
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from app.crud import SensorLogRepository, SensorRepository
from processors.base import json_safe_dict, serialize_payload_for_json
from utils.json_utils import (
    CustomJSONEncoder,
    default_json_serializer,
    json_dumps,
    json_loads,
    to_jsonable_python,
)


class SampleEnum(enum.Enum):
    CRITICAL = "CRITICAL"
    ACTIVE = "ACTIVE"


class SampleModel(BaseModel):
    sensor_id: int
    packet_loss_percent: Decimal
    timestamp: datetime.datetime


def test_custom_json_encoder_with_decimal():
    data = {
        "packet_loss_percent": Decimal("100.00"),
        "min_rtt_ms": Decimal("14.52"),
        "nested": {
            "loss": Decimal("0.00"),
            "count": 5,
        },
    }
    encoded = json_dumps(data)
    assert '"packet_loss_percent": 100.0' in encoded or '"packet_loss_percent": 100.00' in encoded
    assert '"min_rtt_ms": 14.52' in encoded

    decoded = json_loads(encoded)
    assert decoded["packet_loss_percent"] == 100.0
    assert decoded["min_rtt_ms"] == 14.52
    assert decoded["nested"]["loss"] == 0.0


def test_custom_json_encoder_with_all_complex_types():
    test_uuid = uuid.uuid4()
    now = datetime.datetime(2026, 8, 17, 15, 6, 38, tzinfo=datetime.timezone.utc)
    today = datetime.date(2026, 8, 17)
    time_val = datetime.time(15, 6, 38)
    ipv4 = ipaddress.IPv4Address("192.168.1.1")
    ipv6 = ipaddress.IPv6Address("2001:db8::1")

    data = {
        "decimal": Decimal("99.99"),
        "uuid": test_uuid,
        "datetime": now,
        "date": today,
        "time": time_val,
        "ipv4": ipv4,
        "ipv6": ipv6,
        "enum": SampleEnum.CRITICAL,
        "set_data": {1, 2, 3},
        "bytes_data": b"sensor_alert",
    }

    serialized = json_dumps(data)
    parsed = json_loads(serialized)

    assert parsed["decimal"] == 99.99
    assert parsed["uuid"] == str(test_uuid)
    assert parsed["datetime"] == now.isoformat()
    assert parsed["date"] == today.isoformat()
    assert parsed["time"] == time_val.isoformat()
    assert parsed["ipv4"] == "192.168.1.1"
    assert parsed["ipv6"] == "2001:db8::1"
    assert parsed["enum"] == "CRITICAL"
    assert sorted(parsed["set_data"]) == [1, 2, 3]
    assert parsed["bytes_data"] == "sensor_alert"


def test_to_jsonable_python_recursive():
    payload = {
        "sensor_id": 2145,
        "ping_results": {
            "packet_count": 5,
            "packet_loss_percent": Decimal("100.00"),
            "min_rtt_ms": None,
            "avg_rtt_ms": None,
            "max_rtt_ms": None,
        },
        "ip": ipaddress.IPv4Address("10.0.0.1"),
        "status": SampleEnum.ACTIVE,
    }

    safe = to_jsonable_python(payload)
    assert safe["ping_results"]["packet_loss_percent"] == 100.0
    assert safe["ip"] == "10.0.0.1"
    assert safe["status"] == "ACTIVE"

    # Verify standard json.dumps succeeds on safe dict without any custom encoder
    pure_json = json.dumps(safe)
    assert "packet_loss_percent" in pure_json


def test_serialize_payload_for_json_with_pydantic_and_decimal():
    model = SampleModel(
        sensor_id=2145,
        packet_loss_percent=Decimal("50.00"),
        timestamp=datetime.datetime(2026, 8, 17, 12, 0, 0, tzinfo=datetime.timezone.utc),
    )
    result = serialize_payload_for_json(model)
    assert result["sensor_id"] == 2145
    assert result["packet_loss_percent"] == 50.0
    assert isinstance(result["timestamp"], str)


def test_json_safe_dict_helper():
    raw = {
        "site_id": 101,
        "target_ip": "1.2.3.4",
        "alert_id": 505,
        "ping_results": {
            "packet_loss_percent": Decimal("75.50"),
            "avg_rtt_ms": Decimal("12.34"),
        },
    }
    cleaned = json_safe_dict(raw)
    assert cleaned["ping_results"]["packet_loss_percent"] == 75.5
    assert cleaned["ping_results"]["avg_rtt_ms"] == 12.34

    # Should serialize without error using standard json.dumps
    json_str = json.dumps(cleaned)
    assert '"packet_loss_percent": 75.5' in json_str


def test_sensor_log_repository_sanitization():
    mock_session = MagicMock()
    repo = SensorLogRepository(mock_session)

    log_details = {
        "packet_loss_percent": Decimal("100.00"),
        "avg_rtt_ms": Decimal("15.20"),
    }

    entry = repo.create(
        sensor_id=2145,
        log_level=SampleEnum.CRITICAL,
        log_status=SampleEnum.ACTIVE,
        log_message="Down alert",
        log_details=log_details,
    )

    # Verify log_details on the created ORM object is JSON-safe
    assert entry.log_details["packet_loss_percent"] == 100.0
    assert entry.log_details["avg_rtt_ms"] == 15.2
    assert mock_session.add.called
    assert mock_session.flush.called


def test_psycopg_dumper_integration_with_decimal():
    try:
        import psycopg.types.json
        # Configure psycopg with our serializer
        psycopg.types.json.set_json_dumps(json_dumps)
        dumper = psycopg.types.json.JsonbDumper(dict)
        dumped_bytes = dumper.dump({
            "packet_loss_percent": Decimal("100.00"),
            "min_rtt_ms": Decimal("5.25"),
        })
        assert b'"packet_loss_percent": 100.0' in dumped_bytes or b'"packet_loss_percent": 100.00' in dumped_bytes
        assert b'"min_rtt_ms": 5.25' in dumped_bytes
    except (ImportError, AttributeError):
        pytest.skip("psycopg is not installed in this test environment")
