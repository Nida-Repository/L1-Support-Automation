
from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Optional

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    model_validator,
)

# ---------------------------------------------------------------------------
# Reusable, constrained field types (mirror the CHECK constraints in the DDL)
# ---------------------------------------------------------------------------

PacketCount = Annotated[
    int,
    Field(gt=0, description="Number of ICMP packets sent (packet_count > 0)"),
]

# DECIMAL(5,2), CHECK BETWEEN 0.00 AND 100.00
PacketLossPercent = Annotated[
    Decimal,
    Field(
        ge=Decimal("0.00"),
        le=Decimal("100.00"),
        max_digits=5,
        decimal_places=2,
        description="Packet loss percentage (0.00-100.00)",
    ),
]

# DECIMAL(7,2), CHECK >= 0
RttMs = Annotated[
    Decimal,
    Field(
        ge=Decimal("0.00"),
        max_digits=7,
        decimal_places=2,
        description="Round-trip time in milliseconds",
    ),
]


# ---------------------------------------------------------------------------
# Base: fields shared by create/read models
# ---------------------------------------------------------------------------

class PingDiagnosticBase(BaseModel):
    """Fields common to write and read representations of a ping diagnostic."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    alert_id: int = Field(..., gt=0, description="FK -> ALERT_HISTORY.alert_id")
    packet_count: PacketCount
    packet_loss_percent: Optional[PacketLossPercent] = None
    min_rtt_ms: Optional[RttMs] = None
    avg_rtt_ms: Optional[RttMs] = None
    max_rtt_ms: Optional[RttMs] = None
    technician_notes: Optional[str] = Field(default=None, max_length=10_000)

    @model_validator(mode="after")
    def _check_rtt_ordering(self) -> "PingDiagnosticBase":
        """DB has no cross-column CHECK for this; enforce sane ordering in the app layer."""
        vals = (self.min_rtt_ms, self.avg_rtt_ms, self.max_rtt_ms)
        if all(v is not None for v in vals):
            if not (self.min_rtt_ms <= self.avg_rtt_ms <= self.max_rtt_ms):
                raise ValueError(
                    "min_rtt_ms <= avg_rtt_ms <= max_rtt_ms must hold"
                )
        return self

    @field_serializer(
        "packet_loss_percent", "min_rtt_ms", "avg_rtt_ms", "max_rtt_ms", when_used="json"
    )
    def _serialize_decimal(self, value: Optional[Decimal]) -> Optional[float]:
        # Keep Decimal precision internally; emit plain numbers in JSON responses
        # instead of pydantic's default string representation.
        return float(value) if value is not None else None

class PingDiagnosticCreate(PingDiagnosticBase):
    """Payload for POST /ping-diagnostics."""


class PingDiagnosticUpdate(BaseModel):
    """Payload for PATCH /ping-diagnostics/{ping_id}. alert_id is intentionally omitted (immutable FK)."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    packet_count: Optional[PacketCount] = None
    packet_loss_percent: Optional[PacketLossPercent] = None
    min_rtt_ms: Optional[RttMs] = None
    avg_rtt_ms: Optional[RttMs] = None
    max_rtt_ms: Optional[RttMs] = None
    technician_notes: Optional[str] = Field(default=None, max_length=10_000)


# ---------------------------------------------------------------------------
# Read: full record as returned by the API, built directly from a
# SQLAlchemy ORM instance via from_attributes=True
# ---------------------------------------------------------------------------

class PingDiagnosticRead(PingDiagnosticBase):
    """Response model. Populates from a SQLAlchemy PingDiagnostic ORM object."""

    model_config = ConfigDict(from_attributes=True, str_strip_whitespace=True)

    ping_id: int
    executed_at: AwareDatetime  # TIMESTAMPTZ -> must be timezone-aware


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # FastAPI request validation
    payload = PingDiagnosticCreate(
        alert_id=104,
        packet_count=50,
        packet_loss_percent=Decimal("2.00"),
        min_rtt_ms=Decimal("10.50"),
        avg_rtt_ms=Decimal("15.25"),
        max_rtt_ms=Decimal("22.10"),
        technician_notes="Recheck during business hours",
    )
    print(payload.model_dump_json(indent=2))

    # Simulated ORM object (any object with matching attributes works with
    # from_attributes=True, e.g. a SQLAlchemy PingDiagnostic mapped instance)
    class _FakeORMRow:
        ping_id = 101
        alert_id = 104
        packet_count = 50
        packet_loss_percent = Decimal("2.00")
        min_rtt_ms = Decimal("10.50")
        avg_rtt_ms = Decimal("15.25")
        max_rtt_ms = Decimal("22.10")
        technician_notes = "Recheck during business hours"
        from datetime import datetime, timezone
        executed_at = datetime.now(timezone.utc)

    read_model = PingDiagnosticRead.model_validate(_FakeORMRow())
    print(read_model.model_dump_json(indent=2))