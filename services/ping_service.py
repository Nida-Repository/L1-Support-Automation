from __future__ import annotations

import inspect
import asyncio
import logging
import platform
import re
from decimal import Decimal
from typing import Any, Dict, Optional

from app.database import SessionLocal
from app.crud import (
    PingDiagnosticRepository,
    SensorLogRepository,
    session_scope,
)

logger = logging.getLogger(__name__)

BATCH_COUNT = 10
PINGS_PER_BATCH = 10
PING_TIMEOUT_SECONDS = 4.0
PAUSE_BETWEEN_BATCHES_SECONDS = 81.0


class PingIp:
    """Service handling multi-batch IP reachability diagnostics."""

    async def execute(self, payload: Dict[str, Any]) -> None:
        """
        Executes ping diagnostics in up to 10 batches of 10 pings.
        
        Payload keys expected:
            - site_id (int)
            - sensor_id (int)
            - target_ip (str)
            - alert_id (Optional[int]) - Optional / ignored if None
        """
        site_id: int = payload["site_id"]
        sensor_id: int = payload["sensor_id"]
        target_ip: str = payload["target_ip"]
        alert_id: Optional[int] = payload.get("alert_id")

        logger.info(
            "Starting ping diagnostic job for site_id=%s, target=%s",
            site_id,
            target_ip,
        )

        site_recovered = False
        last_ping_result: Optional[Dict[str, Any]] = None

        for batch_index in range(1, BATCH_COUNT + 1):
            logger.info("Executing ping batch %d/%d for %s", batch_index, BATCH_COUNT, target_ip)

            batch_result = await self._run_ping_batch(target_ip, PINGS_PER_BATCH, PING_TIMEOUT_SECONDS)
            last_ping_result = batch_result

            # Check if site is reachable (packet loss less than 100%)
            if batch_result["packet_loss_percent"] < Decimal("100.00"):
                logger.info("Site %s recovered on batch %d/%d!", target_ip, batch_index, BATCH_COUNT)
                site_recovered = True
                break

            # Pause for 81 seconds between batches if site is still unreachable
            if batch_index < BATCH_COUNT:
                logger.warning(
                    "Batch %d/%d failed for %s. Pausing for %s seconds...",
                    batch_index,
                    BATCH_COUNT,
                    target_ip,
                    PAUSE_BETWEEN_BATCHES_SECONDS,
                )
                await asyncio.sleep(PAUSE_BETWEEN_BATCHES_SECONDS)

        # Handle post-diagnostic actions based on reachability state
        if site_recovered:
            self._on_site_recovered(sensor_id)
        else:
            await self._on_site_unreachable(
                site_id=site_id,
                alert_id=alert_id,
                ping_result=last_ping_result,
            )

    async def _run_ping_batch(
        self, target_ip: str, count: int, timeout_per_ping: float
    ) -> Dict[str, Any]:
        """
        Executes pings sequentially so that each unreachable ping waits out its full 4-second timeout.
        Total batch runtime for 10 failed pings = 10 x 4s = 40 seconds.
        """
        successful_rtts: list[Decimal] = []
        packets_sent = 0

        for _ in range(count):
            packets_sent += 1
            rtt = await self._single_ping(target_ip, timeout_per_ping)
            if rtt is not None:
                successful_rtts.append(rtt)

        received = len(successful_rtts)
        loss_percent = Decimal((packets_sent - received) / packets_sent * 100).quantize(Decimal("0.01"))

        if successful_rtts:
            min_rtt = min(successful_rtts).quantize(Decimal("0.01"))
            max_rtt = max(successful_rtts).quantize(Decimal("0.01"))
            avg_rtt = (sum(successful_rtts) / Decimal(received)).quantize(Decimal("0.01"))
        else:
            min_rtt, avg_rtt, max_rtt = None, None, None

        return {
            "packet_count": packets_sent,
            "packet_loss_percent": loss_percent,
            "min_rtt_ms": min_rtt,
            "avg_rtt_ms": avg_rtt,
            "max_rtt_ms": max_rtt,
        }

    async def _single_ping(self, target_ip: str, timeout: float) -> Optional[Decimal]:
        """Runs a single ping command and parses RTT in milliseconds if successful."""
        system = platform.system().lower()
        if system == "windows":
            cmd = ["ping", "-n", "1", "-w", str(int(timeout * 1000)), target_ip]
        else:
            # Linux / macOS
            cmd = ["ping", "-c", "1", "-W", str(int(timeout)), target_ip]

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await process.communicate()

            if process.returncode == 0:
                return self._parse_rtt_ms(stdout.decode(errors="ignore"))
        except Exception as exc:
            logger.error("Error executing ping command against %s: %s", target_ip, exc)

        return None

    @staticmethod
    def _parse_rtt_ms(output: str) -> Optional[Decimal]:
        """Parses RTT output across standard OS ICMP ping formats."""
        match = re.search(r"time[=<]([\d\.]+)\s*ms", output, re.IGNORECASE)
        if match:
            return Decimal(match.group(1))
        return Decimal("0.00")

    def _on_site_recovered(self, sensor_id: int) -> None:
        """Closes all open sensor logs when site is found reachable."""
        with session_scope(SessionLocal) as session:
            repo = SensorLogRepository(session)
            closed_count = repo.close_open_logs(sensor_id)
            logger.info("Site recovered. Closed %d open log(s) for sensor_id=%s", closed_count, sensor_id)

    async def _on_site_unreachable(
        self, site_id: int, alert_id: Optional[int], ping_result: Optional[Dict[str, Any]]
    ) -> None:
        """Saves ping results (if alert_id is available) and triggers SMTP escalation."""
        if not ping_result:
            logger.error("No ping diagnostic available to process for site_id=%s", site_id)
            return

        # 1. Save to PingDiagnostic table ONLY if alert_id exists (due to NOT NULL foreign key constraint)
        if alert_id is not None:
            with session_scope(SessionLocal) as session:
                repo = PingDiagnosticRepository(session)
                diagnostic = repo.create(
                    alert_id=alert_id,
                    packet_count=ping_result["packet_count"],
                    packet_loss_percent=ping_result["packet_loss_percent"],
                    min_rtt_ms=ping_result["min_rtt_ms"],
                    avg_rtt_ms=ping_result["avg_rtt_ms"],
                    max_rtt_ms=ping_result["max_rtt_ms"],
                    technician_notes="Automated diagnostic: Target remained unreachable after 10 full batches.",
                )
                logger.info("Saved PingDiagnostic id=%s for alert_id=%s", diagnostic.ping_id, alert_id)
        else:
            logger.info("Skipping PingDiagnostic record creation as alert_id is not provided.")

        # 2. Invoke SMTP Client with site_id and ping_results
        smtp_payload = {
            "site_id": site_id,
            "ping_results": {
                "packet_count": ping_result["packet_count"],
                "packet_loss_percent": str(ping_result["packet_loss_percent"]),
                "min_rtt_ms": str(ping_result["min_rtt_ms"]) if ping_result["min_rtt_ms"] else None,
                "avg_rtt_ms": str(ping_result["avg_rtt_ms"]) if ping_result["avg_rtt_ms"] else None,
                "max_rtt_ms": str(ping_result["max_rtt_ms"]) if ping_result["max_rtt_ms"] else None,
            },
        }

        try:
            from client.smtp_client import send_alert_notification  # type: ignore

            if inspect.iscoroutinefunction(send_alert_notification):
                await send_alert_notification(smtp_payload)
            else:
                send_alert_notification(smtp_payload)
            logger.info("Successfully dispatched SMTP alert for site_id=%s", site_id)
        except Exception as exc:
            logger.exception("Failed to dispatch SMTP notification for site_id=%s: %s", site_id, exc)


# Entry point for Celery
def process(payload: Dict[str, Any]) -> None:
    """Celery task entrypoint executing the async workflow via asyncio runner."""
    workflow = PingIp()
    asyncio.run(workflow.execute(payload))