from __future__ import annotations

import asyncio
import inspect
import ipaddress
import logging
import platform
import re
import shutil
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from app.database import SessionLocal
from app.crud import (
    ConstraintViolationError,
    DuplicateError,
    NotFoundError,
    PingDiagnosticRepository,
    RepositoryError,
    SensorLogRepository,
    session_scope,
)

logger = logging.getLogger(__name__)

BATCH_COUNT = 10
PINGS_PER_BATCH = 10
PING_TIMEOUT_SECONDS = 4.0
PAUSE_BETWEEN_BATCHES_SECONDS = 81.0


SUBPROCESS_KILL_GRACE_SECONDS = 2.0

SUBPROCESS_WAIT_MARGIN_SECONDS = 1.0

SMTP_DISPATCH_TIMEOUT_SECONDS = 30.0

_HOSTNAME_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9\-.]*[A-Za-z0-9])?$")
_RTT_RE = re.compile(r"time[=<]\s*([\d.]+)\s*ms", re.IGNORECASE)


class PayloadValidationError(ValueError):
    """Raised when the incoming Celery payload is missing fields or malformed."""


class PingIp:
    """
    Service handling multi-batch IP reachability diagnostics.

    """

    def __init__(self) -> None:
        self._ping_binary = shutil.which("ping")

    # ------------------------------------------------------------------ #
    # Entry point
    # ------------------------------------------------------------------ #

    async def execute(self, payload: Dict[str, Any]) -> None:
        site_id, sensor_id, target_ip, alert_id = self._validate_payload(payload)

        logger.info(
            "Starting ping diagnostic job site_id=%s sensor_id=%s target=%s alert_id=%s",
            site_id, sensor_id, target_ip, alert_id,
        )

        if self._ping_binary is None:
            logger.error("No 'ping' executable found on PATH; cannot run diagnostics.")
            await self._on_site_unreachable(
                site_id=site_id,
                alert_id=alert_id,
                ping_result=self._empty_result("ping binary not found on host"),
            )
            return

        site_recovered = False
        last_ping_result: Optional[Dict[str, Any]] = None

        try:
            for batch_index in range(1, BATCH_COUNT + 1):
                logger.info("Executing ping batch %d/%d for %s", batch_index, BATCH_COUNT, target_ip)

                try:
                    batch_result = await self._run_ping_batch(
                        target_ip, PINGS_PER_BATCH, PING_TIMEOUT_SECONDS
                    )
                except Exception:
                    logger.exception(
                        "Unexpected error running batch %d/%d for %s; treating as 100%% loss",
                        batch_index, BATCH_COUNT, target_ip,
                    )
                    batch_result = self._empty_result("batch execution error")

                last_ping_result = batch_result

                if batch_result["packet_loss_percent"] < Decimal("100.00"):
                    logger.info("Site %s recovered on batch %d/%d", target_ip, batch_index, BATCH_COUNT)
                    site_recovered = True
                    break

                if batch_index < BATCH_COUNT:
                    logger.warning(
                        "Batch %d/%d failed for %s (100%% loss). Pausing %ss before next batch.",
                        batch_index, BATCH_COUNT, target_ip, PAUSE_BETWEEN_BATCHES_SECONDS,
                    )
                    await asyncio.sleep(PAUSE_BETWEEN_BATCHES_SECONDS)
        except asyncio.CancelledError:
            # Worker shutdown / Celery task revoked mid-flight. Don't swallow --
            # let the caller (Celery) know the task did not complete.
            logger.warning("Ping diagnostic job for site_id=%s was cancelled.", site_id)
            raise

        if site_recovered:
            self._on_site_recovered(sensor_id, target_ip)
        else:
            await self._on_site_unreachable(
                site_id=site_id, alert_id=alert_id, ping_result=last_ping_result,
            )

    # ------------------------------------------------------------------ #
    # Payload validation
    # ------------------------------------------------------------------ #

    @staticmethod
    def _validate_payload(payload: Dict[str, Any]) -> Tuple[int, int, str, Optional[int]]:
        if not isinstance(payload, dict):
            raise PayloadValidationError(f"payload must be a dict, got {type(payload)!r}")

        missing = [k for k in ("site_id", "sensor_id", "target_ip") if k not in payload]
        if missing:
            raise PayloadValidationError(f"payload missing required key(s): {missing}")

        try:
            site_id = int(payload["site_id"])
            sensor_id = int(payload["sensor_id"])
        except (TypeError, ValueError) as exc:
            raise PayloadValidationError(f"site_id/sensor_id must be integers: {exc}") from exc

        target_ip = str(payload["target_ip"]).strip()
        if not target_ip:
            raise PayloadValidationError("target_ip must be a non-empty string")

        try:
            ipaddress.ip_address(target_ip)
        except ValueError:
            # Not a literal IP -- allow a hostname/FQDN, but reject anything
            # that isn't a sane hostname to keep subprocess args safe.
            if not _HOSTNAME_RE.match(target_ip):
                raise PayloadValidationError(f"target_ip is not a valid IP or hostname: {target_ip!r}")

        alert_id_raw = payload.get("alert_id")
        alert_id: Optional[int] = None
        if alert_id_raw is not None:
            try:
                alert_id = int(alert_id_raw)
            except (TypeError, ValueError) as exc:
                raise PayloadValidationError(f"alert_id must be an integer or None: {exc}") from exc

        return site_id, sensor_id, target_ip, alert_id

    # ------------------------------------------------------------------ #
    # Batch execution
    # ------------------------------------------------------------------ #

    async def _run_ping_batch(
        self, target_ip: str, count: int, timeout_per_ping: float
    ) -> Dict[str, Any]:
        successful_rtts: List[Decimal] = []
        packets_sent = 0

        for _ in range(count):
            packets_sent += 1
            try:
                rtt = await self._single_ping(target_ip, timeout_per_ping)
            except Exception:
                logger.exception("Unhandled error pinging %s; counting as loss", target_ip)
                rtt = None
            if rtt is not None:
                successful_rtts.append(rtt)

        received = len(successful_rtts)
        loss_percent = (
            (Decimal(packets_sent - received) / Decimal(packets_sent) * Decimal(100))
            .quantize(Decimal("0.01"))
        )

        if successful_rtts:
            min_rtt = min(successful_rtts).quantize(Decimal("0.01"))
            max_rtt = max(successful_rtts).quantize(Decimal("0.01"))
            avg_rtt = (sum(successful_rtts) / Decimal(received)).quantize(Decimal("0.01"))
        else:
            min_rtt = avg_rtt = max_rtt = None

        return {
            "packet_count": packets_sent,
            "packet_loss_percent": loss_percent,
            "min_rtt_ms": min_rtt,
            "avg_rtt_ms": avg_rtt,
            "max_rtt_ms": max_rtt,
        }

    @staticmethod
    def _empty_result(reason: str) -> Dict[str, Any]:
        logger.debug("Producing 100%% loss placeholder result: %s", reason)
        return {
            "packet_count": PINGS_PER_BATCH,
            "packet_loss_percent": Decimal("100.00"),
            "min_rtt_ms": None,
            "avg_rtt_ms": None,
            "max_rtt_ms": None,
        }

    # ------------------------------------------------------------------ #
    # Single ping
    # ------------------------------------------------------------------ #

    async def _single_ping(self, target_ip: str, timeout: float) -> Optional[Decimal]:
        """
        Runs exactly one ping. Guarantees the call never exceeds
        `timeout + SUBPROCESS_WAIT_MARGIN_SECONDS`, regardless of whether the
        OS ping binary honors its own -W/-w flag, by wrapping the subprocess
        in asyncio.wait_for and forcibly killing it on timeout.
        """
        system = platform.system().lower()
        if system == "windows":
            cmd = [self._ping_binary, "-n", "1", "-w", str(int(timeout * 1000)), target_ip]
        else:
            cmd = [self._ping_binary, "-c", "1", "-W", str(int(timeout)), target_ip]

        process: Optional[asyncio.subprocess.Process] = None
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, _ = await asyncio.wait_for(
                    process.communicate(), timeout=timeout + SUBPROCESS_WAIT_MARGIN_SECONDS
                )
            except asyncio.TimeoutError:
                logger.warning("ping to %s exceeded %.1fs; killing stuck subprocess", target_ip, timeout)
                await self._kill_process(process)
                return None

            if process.returncode == 0:
                return self._parse_rtt_ms(stdout.decode(errors="ignore"))
            return None

        except FileNotFoundError:
            logger.error("ping executable not found: %s", self._ping_binary)
            return None
        except PermissionError:
            logger.error("permission denied executing ping binary: %s", self._ping_binary)
            return None
        except OSError as exc:
            logger.error("OS error launching ping subprocess for %s: %s", target_ip, exc)
            return None
        except asyncio.CancelledError:
            if process is not None:
                await self._kill_process(process)
            raise
        except Exception:
            logger.exception("Unexpected error pinging %s", target_ip)
            return None

    @staticmethod
    async def _kill_process(process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        try:
            process.kill()
            await asyncio.wait_for(process.wait(), timeout=SUBPROCESS_KILL_GRACE_SECONDS)
        except ProcessLookupError:
            pass
        except asyncio.TimeoutError:
            logger.error("ping subprocess did not die within %.1fs of being killed", SUBPROCESS_KILL_GRACE_SECONDS)
        except Exception:
            logger.exception("Unexpected error while killing stuck ping subprocess")

    @staticmethod
    def _parse_rtt_ms(output: str) -> Optional[Decimal]:
        match = _RTT_RE.search(output)
        if not match:
            return Decimal("0.00")
        try:
            return Decimal(match.group(1))
        except InvalidOperation:
            return Decimal("0.00")

    # ------------------------------------------------------------------ #
    # Recovery path
    # ------------------------------------------------------------------ #

    def _on_site_recovered(self, sensor_id: int, target_ip: str) -> None:
        try:
            with session_scope(SessionLocal) as session:
                repo = SensorLogRepository(session)
                closed_count = repo.close_open_logs(sensor_id)
                logger.info(
                    "Site %s recovered; closed %d open log(s) for sensor_id=%s",
                    target_ip, closed_count, sensor_id,
                )
        except NotFoundError:
            logger.warning("sensor_id=%s not found while closing logs after recovery", sensor_id)
        except (DuplicateError, ConstraintViolationError) as exc:
            logger.error("DB constraint error closing logs for sensor_id=%s: %s", sensor_id, exc)
        except RepositoryError:
            logger.exception("Repository error closing logs for sensor_id=%s", sensor_id)
        except Exception:
            logger.exception("Unexpected DB error closing logs for sensor_id=%s", sensor_id)

    # ------------------------------------------------------------------ #
    # Unreachable path
    # ------------------------------------------------------------------ #

    async def _on_site_unreachable(
        self, site_id: int, alert_id: Optional[int], ping_result: Optional[Dict[str, Any]]
    ) -> None:
        if not ping_result:
            logger.error("No ping diagnostic available for site_id=%s; using placeholder result", site_id)
            ping_result = self._empty_result("missing result")

        diagnostic_id: Optional[int] = None
        if alert_id is not None:
            diagnostic_id = self._persist_ping_diagnostic(alert_id, ping_result)
        else:
            logger.info("Skipping PingDiagnostic persistence: no alert_id for site_id=%s", site_id)

        await self._dispatch_smtp(
            site_id=site_id, alert_id=alert_id, diagnostic_id=diagnostic_id, ping_result=ping_result
        )

    def _persist_ping_diagnostic(self, alert_id: int, ping_result: Dict[str, Any]) -> Optional[int]:
        try:
            with session_scope(SessionLocal) as session:
                repo = PingDiagnosticRepository(session)
                diagnostic = repo.create(
                    alert_id=alert_id,
                    packet_count=ping_result["packet_count"],
                    packet_loss_percent=ping_result["packet_loss_percent"],
                    min_rtt_ms=ping_result["min_rtt_ms"],
                    avg_rtt_ms=ping_result["avg_rtt_ms"],
                    max_rtt_ms=ping_result["max_rtt_ms"],
                    technician_notes="Automated diagnostic: target remained unreachable after 10 full batches.",
                )
                logger.info("Saved PingDiagnostic id=%s for alert_id=%s", diagnostic.ping_id, alert_id)
                return diagnostic.ping_id
        except NotFoundError:
            logger.error("alert_id=%s not found; cannot persist ping diagnostic", alert_id)
        except DuplicateError as exc:
            logger.error("Duplicate ping diagnostic entry for alert_id=%s: %s", alert_id, exc)
        except ConstraintViolationError as exc:
            logger.error("Constraint violation persisting ping diagnostic for alert_id=%s: %s", alert_id, exc)
        except RepositoryError:
            logger.exception("Repository error persisting ping diagnostic for alert_id=%s", alert_id)
        except Exception:
            logger.exception("Unexpected DB error persisting ping diagnostic for alert_id=%s", alert_id)
        return None

    async def _dispatch_smtp(
        self,
        site_id: int,
        alert_id: Optional[int],
        diagnostic_id: Optional[int],
        ping_result: Dict[str, Any],
    ) -> None:
        smtp_payload = {
            "site_id": site_id,
            "alert_id": alert_id,
            "ping_diagnostic_id": diagnostic_id,
            "ping_results": {
                "packet_count": ping_result["packet_count"],
                "packet_loss_percent": str(ping_result["packet_loss_percent"]),
                "min_rtt_ms": str(ping_result["min_rtt_ms"]) if ping_result["min_rtt_ms"] is not None else None,
                "avg_rtt_ms": str(ping_result["avg_rtt_ms"]) if ping_result["avg_rtt_ms"] is not None else None,
                "max_rtt_ms": str(ping_result["max_rtt_ms"]) if ping_result["max_rtt_ms"] is not None else None,
            },
        }

        try:
            from client.smtp_client import send_alert_notification  # type: ignore
        except ImportError:
            logger.exception("Could not import send_alert_notification from client.smtp_client")
            return

        try:
            if inspect.iscoroutinefunction(send_alert_notification):
                await asyncio.wait_for(
                    send_alert_notification(smtp_payload), timeout=SMTP_DISPATCH_TIMEOUT_SECONDS
                )
            else:
                loop = asyncio.get_running_loop()
                await asyncio.wait_for(
                    loop.run_in_executor(None, send_alert_notification, smtp_payload),
                    timeout=SMTP_DISPATCH_TIMEOUT_SECONDS,
                )
            logger.info("Dispatched SMTP alert for site_id=%s", site_id)
        except asyncio.TimeoutError:
            logger.error(
                "Timed out (%ss) dispatching SMTP alert for site_id=%s",
                SMTP_DISPATCH_TIMEOUT_SECONDS, site_id,
            )
        except Exception:
            logger.exception("Failed to dispatch SMTP notification for site_id=%s", site_id)


# --------------------------------------------------------------------- #
# Celery entry point
# --------------------------------------------------------------------- #

def process(payload: Dict[str, Any]) -> None:

    try:
        workflow = PingIp()
        asyncio.run(workflow.execute(payload))
    except PayloadValidationError:
        logger.error("Rejected malformed ping diagnostic payload: %r", payload)
        raise
    except Exception:
        logger.exception("Unhandled exception in ping diagnostic task for payload=%r", payload)
        raise