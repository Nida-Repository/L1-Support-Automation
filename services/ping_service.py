"""Ping Diagnostic Service.

Handles multi-batch ICMP reachability diagnostics with concurrency bounding,
automatic subprocess lifecycle management, and escalation dispatch upon outage.
"""
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

from app.crud import (
    ConstraintViolationError,
    DuplicateError,
    NotFoundError,
    PingDiagnosticRepository,
    RepositoryError,
    SensorLogRepository,
    session_scope,
)
from app.database import SessionLocal
from config.settings import settings

logger = logging.getLogger(__name__)

SUBPROCESS_KILL_GRACE_SECONDS = 2.0
SUBPROCESS_WAIT_MARGIN_SECONDS = 1.0
SMTP_DISPATCH_TIMEOUT_SECONDS = 30.0

_HOSTNAME_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9\-.]*[A-Za-z0-9])?$")
_RTT_RE = re.compile(r"time[=<]\s*([\d.]+)\s*ms", re.IGNORECASE)

logger.info(
    "Initializing Ping Diagnostic Service [Batches: %d | Pings/Batch: %d | Timeout: %.1fs | Pause: %.1fs | "
    "MaxConcurrentJobs: %d | MaxConcurrentPings: %d]",
    settings.ping_batch_count,
    settings.pings_per_batch,
    settings.ping_timeout_seconds,
    settings.pause_between_batches_seconds,
    settings.ping_diag_max_concurrent_jobs,
    settings.ping_diag_max_concurrent_pings,
)


class PayloadValidationError(ValueError):
    """Raised when the incoming payload is missing required fields or malformed."""
    pass


def _get_job_semaphore() -> asyncio.Semaphore:
    """Lazily get or create a semaphore bound to the current running event loop."""
    loop = asyncio.get_running_loop()
    sem = getattr(loop, "_ping_diag_job_semaphore", None)
    if sem is None:
        sem = asyncio.Semaphore(settings.ping_diag_max_concurrent_jobs)
        loop._ping_diag_job_semaphore = sem  # type: ignore[attr-defined]
    return sem


def _get_ping_semaphore() -> asyncio.Semaphore:
    """Lazily get or create a subprocess semaphore bound to the current running event loop."""
    loop = asyncio.get_running_loop()
    sem = getattr(loop, "_ping_diag_ping_semaphore", None)
    if sem is None:
        sem = asyncio.Semaphore(settings.ping_diag_max_concurrent_pings)
        loop._ping_diag_ping_semaphore = sem  # type: ignore[attr-defined]
    return sem


class PingIp:
    """Service handling multi-batch IP reachability diagnostics."""

    def __init__(self) -> None:
        self._ping_binary = shutil.which("ping")
        if self._ping_binary:
            logger.debug("Resolved 'ping' executable path: %s", self._ping_binary)
        else:
            logger.warning("'ping' binary could not be found on PATH during initialization.")

    # ------------------------------------------------------------------ #
    # Entry Point
    # ------------------------------------------------------------------ #

    async def execute(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        site_id, sensor_id, target_ip, alert_id = self._validate_payload(payload)

        logger.info(
            "Starting ping diagnostic job | site_id=%s | sensor_id=%s | target=%s | alert_id=%s",
            site_id,
            sensor_id,
            target_ip,
            alert_id,
        )

        if self._ping_binary is None:
            logger.error("No 'ping' executable found on PATH; cannot run diagnostics for target=%s", target_ip)
            placeholder = self._empty_result("ping binary not found on host")
            await self._on_site_unreachable(
                site_id=site_id,
                alert_id=alert_id,
                ping_result=placeholder,
            )
            return placeholder

        job_semaphore = _get_job_semaphore()
        if job_semaphore.locked():
            logger.info(
                "Diagnostic job for site_id=%s (target=%s) is waiting for execution slot (max=%d)",
                site_id,
                target_ip,
                settings.ping_diag_max_concurrent_jobs,
            )

        async with job_semaphore:
            site_recovered = False
            last_ping_result: Optional[Dict[str, Any]] = None

            try:
                for batch_index in range(1, settings.ping_batch_count + 1):
                    logger.info(
                        "Executing ping batch %d/%d for target=%s (site_id=%s)",
                        batch_index,
                        settings.ping_batch_count,
                        target_ip,
                        site_id,
                    )

                    try:
                        batch_result = await self._run_ping_batch(
                            target_ip, settings.pings_per_batch, settings.ping_timeout_seconds
                        )
                    except Exception:
                        logger.exception(
                            "Unexpected error running batch %d/%d for target=%s; treating as 100%% loss",
                            batch_index,
                            settings.ping_batch_count,
                            target_ip,
                        )
                        batch_result = self._empty_result("batch execution error")

                    last_ping_result = batch_result
                    loss_pct = batch_result.get("packet_loss_percent", Decimal("100.00"))

                    logger.debug(
                        "Batch %d/%d result for %s: loss=%s%%, avg_rtt=%s ms",
                        batch_index,
                        settings.ping_batch_count,
                        target_ip,
                        loss_pct,
                        batch_result.get("avg_rtt_ms"),
                    )

                    if loss_pct < Decimal("100.00"):
                        logger.info(
                            "Site target=%s recovered on batch %d/%d (packet loss: %s%%)",
                            target_ip,
                            batch_index,
                            settings.ping_batch_count,
                            loss_pct,
                        )
                        site_recovered = True
                        break

                    if batch_index < settings.ping_batch_count:
                        logger.warning(
                            "Batch %d/%d failed for target=%s (100%% loss). Pausing %.1fs before next batch...",
                            batch_index,
                            settings.ping_batch_count,
                            target_ip,
                            settings.pause_between_batches_seconds,
                        )
                        await asyncio.sleep(settings.pause_between_batches_seconds)

            except asyncio.CancelledError:
                logger.warning(
                    "Ping diagnostic job for site_id=%s (target=%s) was cancelled mid-flight.",
                    site_id,
                    target_ip,
                )
                raise

            if site_recovered:
                self._on_site_recovered(sensor_id, target_ip)
            else:
                logger.warning(
                    "Target=%s remained unreachable across all %d batches for site_id=%s",
                    target_ip,
                    settings.ping_batch_count,
                    site_id,
                )
                await self._on_site_unreachable(
                    site_id=site_id,
                    alert_id=alert_id,
                    ping_result=last_ping_result,
                )

            return last_ping_result

    # ------------------------------------------------------------------ #
    # Payload Validation
    # ------------------------------------------------------------------ #

    @staticmethod
    def _validate_payload(payload: Dict[str, Any]) -> Tuple[int, int, str, Optional[int]]:
        logger.debug("Validating incoming payload: %r", payload)
        if not isinstance(payload, dict):
            logger.error("Payload validation failed: expected dict, got %s", type(payload).__name__)
            raise PayloadValidationError(f"payload must be a dict, got {type(payload)!r}")

        missing = [k for k in ("site_id", "sensor_id", "target_ip") if k not in payload]
        if missing:
            logger.error("Payload validation failed: missing required key(s) %s", missing)
            raise PayloadValidationError(f"payload missing required key(s): {missing}")

        try:
            site_id = int(payload["site_id"])
            sensor_id = int(payload["sensor_id"])
        except (TypeError, ValueError) as exc:
            logger.error("Payload validation failed: invalid site_id/sensor_id values: %s", exc)
            raise PayloadValidationError(f"site_id/sensor_id must be integers: {exc}") from exc

        target_ip = str(payload["target_ip"]).strip()
        if not target_ip:
            logger.error("Payload validation failed: target_ip is empty")
            raise PayloadValidationError("target_ip must be a non-empty string")

        try:
            ipaddress.ip_address(target_ip)
            logger.debug("Target IP '%s' validated as a valid IP address.", target_ip)
        except ValueError:
            logger.debug("Target '%s' is not a literal IP, checking hostname format...", target_ip)
            if not _HOSTNAME_RE.match(target_ip):
                logger.error("Payload validation failed: target '%s' is neither a valid IP nor hostname", target_ip)
                raise PayloadValidationError(f"target_ip is not a valid IP or hostname: {target_ip!r}")

        alert_id_raw = payload.get("alert_id")
        alert_id: Optional[int] = None
        if alert_id_raw is not None:
            try:
                alert_id = int(alert_id_raw)
            except (TypeError, ValueError) as exc:
                logger.error("Payload validation failed: alert_id '%s' is not an integer: %s", alert_id_raw, exc)
                raise PayloadValidationError(f"alert_id must be an integer or None: {exc}") from exc

        return site_id, sensor_id, target_ip, alert_id

    # ------------------------------------------------------------------ #
    # Batch Execution
    # ------------------------------------------------------------------ #

    async def _run_ping_batch(
        self, target_ip: str, count: int, timeout_per_ping: float
    ) -> Dict[str, Any]:
        logger.debug(
            "Starting ping execution for target=%s | count=%d | timeout=%.1fs",
            target_ip,
            count,
            timeout_per_ping,
        )

        ping_semaphore = _get_ping_semaphore()

        async def _bounded_single_ping(attempt_idx: int) -> Optional[Decimal]:
            async with ping_semaphore:
                try:
                    return await self._single_ping(target_ip, timeout_per_ping)
                except Exception:
                    logger.exception(
                        "Unhandled error on ping attempt %d/%d to %s; counting as loss",
                        attempt_idx,
                        count,
                        target_ip,
                    )
                    return None

        results = await asyncio.gather(
            *(_bounded_single_ping(idx) for idx in range(1, count + 1))
        )

        packets_sent = count
        successful_rtts: List[Decimal] = [rtt for rtt in results if rtt is not None]
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
        logger.debug("Producing 100%% loss placeholder result reason: '%s'", reason)
        return {
            "packet_count": settings.pings_per_batch,
            "packet_loss_percent": Decimal("100.00"),
            "min_rtt_ms": None,
            "avg_rtt_ms": None,
            "max_rtt_ms": None,
        }

    # ------------------------------------------------------------------ #
    # Single Ping Subprocess
    # ------------------------------------------------------------------ #

    async def _single_ping(self, target_ip: str, timeout: float) -> Optional[Decimal]:
        system = platform.system().lower()
        if system == "windows":
            cmd = [self._ping_binary, "-n", "1", "-w", str(int(timeout * 1000)), target_ip]
        else:
            cmd = [self._ping_binary, "-c", "1", "-W", str(int(timeout)), target_ip]

        logger.debug("Executing ping command: %s", " ".join(cmd))
        process: Optional[asyncio.subprocess.Process] = None
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(), timeout=timeout + SUBPROCESS_WAIT_MARGIN_SECONDS
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Ping process to %s timed out after %.1fs; killing PID=%s",
                    target_ip,
                    timeout + SUBPROCESS_WAIT_MARGIN_SECONDS,
                    getattr(process, "pid", "N/A"),
                )
                await self._kill_process(process)
                return None

            if process.returncode == 0:
                out_str = stdout.decode(errors="ignore")
                rtt = self._parse_rtt_ms(out_str)
                logger.debug("Ping succeeded to %s | parsed RTT: %s ms", target_ip, rtt)
                return rtt

            err_str = stderr.decode(errors="ignore").strip()
            logger.debug(
                "Ping returned exit code %d for %s | stderr: %s",
                process.returncode,
                target_ip,
                err_str,
            )
            return None

        except (FileNotFoundError, PermissionError) as exc:
            logger.error("Ping execution error (%s): %s", self._ping_binary, exc)
            return None
        except OSError as exc:
            logger.error("OS error during ping subprocess for target=%s: %s", target_ip, exc)
            return None
        except asyncio.CancelledError:
            logger.warning("Single ping execution was cancelled for target=%s", target_ip)
            if process is not None:
                await self._kill_process(process)
            raise
        except Exception:
            logger.exception("Unexpected error pinging target=%s", target_ip)
            return None

    @staticmethod
    async def _kill_process(process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        pid = getattr(process, "pid", "N/A")
        logger.debug("Terminating subprocess PID=%s...", pid)
        try:
            process.kill()
            await asyncio.wait_for(process.wait(), timeout=SUBPROCESS_KILL_GRACE_SECONDS)
            logger.debug("Subprocess PID=%s terminated successfully.", pid)
        except ProcessLookupError:
            logger.debug("Subprocess PID=%s had already exited.", pid)
        except asyncio.TimeoutError:
            logger.error(
                "Ping subprocess PID=%s refused to terminate within %.1fs grace period",
                pid,
                SUBPROCESS_KILL_GRACE_SECONDS,
            )
        except Exception:
            logger.exception("Unexpected error killing subprocess PID=%s", pid)

    @staticmethod
    def _parse_rtt_ms(output: str) -> Optional[Decimal]:
        match = _RTT_RE.search(output)
        if not match:
            logger.debug("Failed to extract RTT from stdout; regex search returned no match.")
            return Decimal("0.00")
        try:
            return Decimal(match.group(1))
        except InvalidOperation:
            logger.warning("Could not convert extracted RTT string '%s' to Decimal.", match.group(1))
            return Decimal("0.00")

    # ------------------------------------------------------------------ #
    # Recovery & Unreachable Handlers
    # ------------------------------------------------------------------ #

    def _on_site_recovered(self, sensor_id: int, target_ip: Optional[str] = None) -> None:
        ip_display = target_ip or "N/A"
        logger.info("Handling site recovery flow for target=%s (sensor_id=%s)", ip_display, sensor_id)
        try:
            with session_scope(SessionLocal) as session:
                repo = SensorLogRepository(session)
                closed_count = repo.close_open_logs(sensor_id)
                logger.info(
                    "Site %s recovered successfully; closed %d open log(s) for sensor_id=%s",
                    ip_display,
                    closed_count,
                    sensor_id,
                )
        except NotFoundError:
            logger.warning("sensor_id=%s not found in DB while closing open logs", sensor_id)
        except (DuplicateError, ConstraintViolationError) as exc:
            logger.error("DB constraint error closing logs for sensor_id=%s: %s", sensor_id, exc)
        except RepositoryError:
            logger.exception("Repository error closing open logs for sensor_id=%s", sensor_id)
        except Exception:
            logger.exception("Unexpected DB error closing open logs for sensor_id=%s", sensor_id)

    async def _on_site_unreachable(
        self, site_id: int, alert_id: Optional[int], ping_result: Optional[Dict[str, Any]]
    ) -> None:
        logger.info("Handling site unreachable flow for site_id=%s | alert_id=%s", site_id, alert_id)
        if not ping_result:
            logger.error("No ping diagnostic available for site_id=%s; generating placeholder 100%% loss", site_id)
            ping_result = self._empty_result("missing result")

        diagnostic_id: Optional[int] = None
        if alert_id is not None:
            diagnostic_id = self._persist_ping_diagnostic(alert_id, ping_result)
        else:
            logger.info("Skipping PingDiagnostic persistence: alert_id is None for site_id=%s", site_id)

        await self._dispatch_smtp(
            site_id=site_id,
            alert_id=alert_id,
            diagnostic_id=diagnostic_id,
            ping_result=ping_result,
        )

    def _persist_ping_diagnostic(self, alert_id: int, ping_result: Dict[str, Any]) -> Optional[int]:
        logger.debug("Persisting PingDiagnostic record for alert_id=%s...", alert_id)
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
                    technician_notes="Automated diagnostic: Target remained unreachable after 10 full batches.",
                )
                logger.info("Successfully persisted PingDiagnostic [ID: %s] for alert_id=%s", diagnostic.ping_id, alert_id)
                return diagnostic.ping_id
        except (NotFoundError, DuplicateError, ConstraintViolationError, RepositoryError) as exc:
            logger.error("Error persisting ping diagnostic for alert_id=%s: %s", alert_id, exc)
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

        logger.debug("Attempting SMTP notification dispatch for site_id=%s...", site_id)
        try:
            from clients.smtp_client import send_alert_notification
        except ImportError:
            logger.exception("Could not import send_alert_notification from clients.smtp_client")
            return

        try:
            if inspect.iscoroutinefunction(send_alert_notification):
                logger.debug("Invoking send_alert_notification as coroutine...")
                await asyncio.wait_for(
                    send_alert_notification(smtp_payload), timeout=SMTP_DISPATCH_TIMEOUT_SECONDS
                )
            else:
                logger.debug("Invoking send_alert_notification in thread executor...")
                loop = asyncio.get_running_loop()
                await asyncio.wait_for(
                    loop.run_in_executor(None, send_alert_notification, smtp_payload),
                    timeout=SMTP_DISPATCH_TIMEOUT_SECONDS,
                )
            logger.info("Successfully dispatched SMTP alert for site_id=%s (alert_id=%s)", site_id, alert_id)
        except asyncio.TimeoutError:
            logger.error(
                "Timed out after %.1fs while dispatching SMTP alert for site_id=%s",
                SMTP_DISPATCH_TIMEOUT_SECONDS,
                site_id,
            )
        except Exception:
            logger.exception("Failed to dispatch SMTP notification for site_id=%s", site_id)


def process(payload: Dict[str, Any]) -> None:
    """Synchronous entry point for Celery task worker."""
    logger.info("Celery process worker received ping diagnostic task payload")
    try:
        workflow = PingIp()
        asyncio.run(workflow.execute(payload))
        logger.info("Ping diagnostic task completed successfully for site_id=%s", payload.get("site_id"))
    except PayloadValidationError:
        logger.error("Rejected malformed ping diagnostic payload: %r", payload)
        raise
    except Exception:
        logger.exception("Unhandled exception in ping diagnostic task execution for payload=%r", payload)
        raise