import sys
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.ping_service import PingIp, process


# ---------------------------------------------------------------------------
# Unit Tests: RTT Output Parsing
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw_output, expected",
    [
        ("64 bytes from 8.8.8.8: icmp_seq=1 ttl=117 time=14.2 ms", Decimal("14.2")),
        ("Reply from 8.8.8.8: bytes=32 time<1ms TTL=117", Decimal("1")),
        ("Reply from 8.8.8.8: bytes=32 time=123ms TTL=117", Decimal("123")),
        ("Request timed out.", Decimal("0.00")),
    ],
)
def test_parse_rtt_ms(raw_output: str, expected: Decimal):
    assert PingIp._parse_rtt_ms(raw_output) == expected


# ---------------------------------------------------------------------------
# Unit Tests: Ping Execution Logic
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_run_ping_batch_mixed_success():
    service = PingIp()

    # Mock single pings: 1 successful (10.0 ms), 1 failed (None)
    with patch.object(
        service, "_single_ping", side_effect=[Decimal("10.00"), None]
    ):
        result = await service._run_ping_batch(
            "8.8.8.8", count=2, timeout_per_ping=1.0
        )

        assert result["packet_count"] == 2
        assert result["packet_loss_percent"] == Decimal("50.00")
        assert result["min_rtt_ms"] == Decimal("10.00")
        assert result["avg_rtt_ms"] == Decimal("10.00")
        assert result["max_rtt_ms"] == Decimal("10.00")


@pytest.mark.asyncio
async def test_run_ping_batch_all_failed():
    service = PingIp()

    with patch.object(service, "_single_ping", return_value=None):
        result = await service._run_ping_batch(
            "8.8.8.8", count=3, timeout_per_ping=1.0
        )

        assert result["packet_count"] == 3
        assert result["packet_loss_percent"] == Decimal("100.00")
        assert result["min_rtt_ms"] is None
        assert result["avg_rtt_ms"] is None
        assert result["max_rtt_ms"] is None


# ---------------------------------------------------------------------------
# Unit Tests: High-Level Workflow (`execute`)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
@patch.object(PingIp, "_on_site_recovered")
@patch.object(PingIp, "_on_site_unreachable", new_callable=AsyncMock)
async def test_execute_recovers_early(
    mock_unreachable: AsyncMock,
    mock_recovered: MagicMock,
    mock_sleep: AsyncMock,
):
    service = PingIp()
    payload = {"site_id": 1, "sensor_id": 10, "target_ip": "10.0.0.1"}

    # Batch 1 fails (100% loss), Batch 2 succeeds (0% loss)
    batch_results = [
        {"packet_loss_percent": Decimal("100.00")},
        {"packet_loss_percent": Decimal("0.00")},
    ]

    with patch.object(service, "_run_ping_batch", side_effect=batch_results):
        await service.execute(payload)

    # Should exit loop early at batch 2
    mock_recovered.assert_called_once_with(10, "10.0.0.1")
    mock_unreachable.assert_not_called()
    assert mock_sleep.call_count == 1  # Paused only once between batch 1 & 2


@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
@patch.object(PingIp, "_on_site_recovered")
@patch.object(PingIp, "_on_site_unreachable", new_callable=AsyncMock)
async def test_execute_fails_all_batches(
    mock_unreachable: AsyncMock,
    mock_recovered: MagicMock,
    mock_sleep: AsyncMock,
):
    service = PingIp()
    payload = {
        "site_id": 1,
        "sensor_id": 10,
        "target_ip": "10.0.0.1",
        "alert_id": 99,
    }

    failed_batch = {"packet_loss_percent": Decimal("100.00")}

    with patch.object(service, "_run_ping_batch", return_value=failed_batch):
        await service.execute(payload)

    mock_recovered.assert_not_called()
    mock_unreachable.assert_called_once_with(
        site_id=1, alert_id=99, ping_result=failed_batch
    )
    # Pauses between 10 batches = 9 pauses
    assert mock_sleep.call_count == 9


# ---------------------------------------------------------------------------
# Unit Tests: Database & Escalation Hooks
# ---------------------------------------------------------------------------
@patch("services.ping_service.session_scope")
def test_on_site_recovered(mock_session_scope):
    mock_session = MagicMock()
    mock_session_scope.return_value.__enter__.return_value = mock_session

    with patch("services.ping_service.SensorLogRepository") as mock_repo_cls:
        mock_repo = mock_repo_cls.return_value
        mock_repo.close_open_logs.return_value = 2

        service = PingIp()
        service._on_site_recovered(sensor_id=10, target_ip="10.0.0.1")

        mock_repo_cls.assert_called_once_with(mock_session)
        mock_repo.close_open_logs.assert_called_once_with(10)


@pytest.mark.asyncio
@patch("services.ping_service.session_scope")
async def test_on_site_unreachable_with_alert_id(mock_session_scope):
    mock_session = MagicMock()
    mock_session_scope.return_value.__enter__.return_value = mock_session

    ping_result = {
        "packet_count": 10,
        "packet_loss_percent": Decimal("100.00"),
        "min_rtt_ms": None,
        "avg_rtt_ms": None,
        "max_rtt_ms": None,
    }

    mock_smtp = AsyncMock()

    with patch("clients.smtp_client.send_alert_notification", mock_smtp), \
         patch("services.ping_service.PingDiagnosticRepository") as mock_repo_cls:
        mock_repo = mock_repo_cls.return_value
        mock_repo.create.return_value = MagicMock(ping_id=101)

        service = PingIp()
        await service._on_site_unreachable(
            site_id=5, alert_id=42, ping_result=ping_result
        )

        # 1. DB Save check
        mock_repo.create.assert_called_once_with(
            alert_id=42,
            packet_count=10,
            packet_loss_percent=Decimal("100.00"),
            min_rtt_ms=None,
            avg_rtt_ms=None,
            max_rtt_ms=None,
            technician_notes="Automated diagnostic: Target remained unreachable after 10 full batches.",
        )

        # 2. SMTP dispatch check
        mock_smtp.assert_called_once_with(
            {
                "site_id": 5,
                "alert_id": 42,
                "ping_diagnostic_id": 101,
                "ping_results": {
                    "packet_count": 10,
                    "packet_loss_percent": "100.00",
                    "min_rtt_ms": None,
                    "avg_rtt_ms": None,
                    "max_rtt_ms": None,
                },
            }
        )


# ---------------------------------------------------------------------------
# Unit Tests: Subprocess Call & Celery Entry Point
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec")
async def test_single_ping_subprocess(mock_exec):
    mock_proc = AsyncMock()
    mock_proc.returncode = 0
    mock_proc.communicate.return_value = (
        b"64 bytes from 1.1.1.1: icmp_seq=1 ttl=58 time=12.5 ms",
        b"",
    )
    mock_exec.return_value = mock_proc

    service = PingIp()
    rtt = await service._single_ping("1.1.1.1", timeout=4.0)

    assert rtt == Decimal("12.5")


@patch("asyncio.run")
def test_process_celery_entrypoint(mock_asyncio_run):
    payload = {"site_id": 1, "sensor_id": 2, "target_ip": "1.1.1.1"}
    with patch.object(PingIp, "execute", new_callable=AsyncMock):
        process(payload)
        mock_asyncio_run.assert_called_once()