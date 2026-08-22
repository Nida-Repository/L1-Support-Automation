"""Redis Caching and Incident State Tracking.

Provides a read-through cache for site and sensor metadata, and a fail-open
deduplication tracker for incoming PRTG sensor alerts.
"""
from __future__ import annotations

import datetime
import json
import logging
from typing import Any, Optional

import redis

from config.settings import settings

from utils.json_utils import json_dumps, json_loads

logger = logging.getLogger(__name__)

logger.info("Initializing Redis connection pool: %s", settings.safe_redis_url)

redis_pool = redis.ConnectionPool.from_url(
    settings.redis_url,
    decode_responses=True,
    socket_timeout=settings.redis_socket_timeout,
    socket_connect_timeout=settings.redis_socket_timeout,
    retry_on_timeout=True,
    health_check_interval=30,
    max_connections=settings.redis_max_connections,
)
redis_client = redis.Redis(connection_pool=redis_pool)


class CacheService:
    """Read-through cache for site metadata.

    Degrades gracefully on Redis failures rather than blocking the main workflow.
    """

    @staticmethod
    def get_sensor_site_info(sensor_id: int) -> Optional[dict[str, Any]]:
        cache_key = f"cache:sensor:{sensor_id}"
        try:
            cached_data = redis_client.get(cache_key)
        except redis.RedisError as exc:
            logger.warning("Redis GET failed for %s: %s", cache_key, exc)
            return None

        if not cached_data:
            logger.debug("Cache miss for %s", cache_key)
            return None

        try:
            site_info = json_loads(cached_data)
            logger.debug("Cache hit for %s", cache_key)
            return site_info
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error("Corrupt cache value at %s: %s", cache_key, exc)
            return None

    @staticmethod
    def set_sensor_site_info(
        sensor_id: int,
        site_info: dict[str, Any],
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        ttl = ttl_seconds if ttl_seconds is not None else settings.redis_cache_ttl_seconds
        cache_key = f"cache:sensor:{sensor_id}"
        try:
            redis_client.setex(cache_key, ttl, json_dumps(site_info))
            logger.info("Successfully cached site info for sensor_id %s (TTL: %ds)", sensor_id, ttl)
            return True
        except (redis.RedisError, Exception) as exc:
            logger.warning("Redis SETEX failed for %s: %s", cache_key, exc)
            return False


class IncidentStateTracker:
    """Tracks active sensor states for deduplicating repeated PRTG alert events.

    Fails open (treats alerts as not-a-duplicate on Redis failure) to ensure outages
    are never silently dropped.
    """

    @staticmethod
    def get_sensor_state(sensor_id: int) -> Optional[str]:
        cache_key = f"state:sensor:{sensor_id}"
        try:
            state = redis_client.get(cache_key)
            logger.debug("Retrieved state '%s' for %s", state, cache_key)
            return state
        except redis.RedisError as exc:
            logger.warning("Redis GET failed for %s: %s", cache_key, exc)
            return None

    @staticmethod
    def set_sensor_state(
        sensor_id: int,
        status: str,
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        ttl = ttl_seconds if ttl_seconds is not None else settings.redis_state_ttl_seconds
        cache_key = f"state:sensor:{sensor_id}"
        try:
            redis_client.setex(cache_key, ttl, status)
            logger.info("Updated active state for sensor_id %s -> '%s' (TTL: %ds)", sensor_id, status, ttl)
            return True
        except redis.RedisError as exc:
            logger.warning("Redis SETEX failed for %s: %s", cache_key, exc)
            return False

    @staticmethod
    def is_duplicate_alert(sensor_id: int, current_status: str) -> bool:
        try:
            last_status = IncidentStateTracker.get_sensor_state(sensor_id)
            is_dup = (last_status == current_status)
            if is_dup:
                logger.debug("Duplicate detected for sensor_id %s with status '%s'", sensor_id, current_status)
            return is_dup
        except redis.RedisError as exc:
            logger.warning(
                "Dedup check failed for sensor_id %s (%s) — failing open to allow alert",
                sensor_id,
                exc,
            )
            return False

    @staticmethod
    def ping() -> bool:
        try:
            is_alive = bool(redis_client.ping())
            logger.debug("Redis ping status: %s", is_alive)
            return is_alive
        except redis.RedisError as exc:
            logger.warning("Redis ping failed: %s", exc)
            return False


class EmailThreadCache:
    """Redis cache for mapping outgoing and incoming Message-IDs to thread/alert context.

    Provides the first-line lookup for In-Reply-To and References headers to avoid
    unnecessary PostgreSQL queries on every incoming email.
    """

    @staticmethod
    def clean_id(message_id: Optional[str]) -> str:
        """Strip enclosing angle brackets and whitespace from Message-ID."""
        if not message_id:
            return ""
        return message_id.strip().strip("<>").strip()

    @classmethod
    def get_thread_by_message_id(cls, message_id: str) -> Optional[dict[str, Any]]:
        """Lookup alert/thread mapping by Message-ID.

        Returns:
            dict containing alert_id, thread_id, and escalation_id if cached, else None.
        """
        clean_msg_id = cls.clean_id(message_id)
        if not clean_msg_id:
            return None

        cache_key = f"msgid:{clean_msg_id}"
        try:
            cached_data = redis_client.get(cache_key)
        except redis.RedisError as exc:
            logger.warning("Redis GET failed for %s: %s", cache_key, exc)
            return None

        if not cached_data:
            logger.debug("Redis cache miss for Message-ID: %s", clean_msg_id)
            return None

        try:
            thread_info = json_loads(cached_data)
            logger.info("Redis cache hit for Message-ID: %s -> %s", clean_msg_id, thread_info)
            return thread_info
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error("Corrupt cache value at %s: %s", cache_key, exc)
            return None

    @classmethod
    def set_message_id_mapping(
        cls,
        message_id: str,
        data: dict[str, Any],
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        """Store Message-ID mapping in Redis with configurable TTL."""
        clean_msg_id = cls.clean_id(message_id)
        if not clean_msg_id or not data:
            return False

        ttl = ttl_seconds if ttl_seconds is not None else settings.redis_state_ttl_seconds
        cache_key = f"msgid:{clean_msg_id}"
        payload = {
            "alert_id": data.get("alert_id"),
            "thread_id": data.get("thread_id"),
            "escalation_id": data.get("escalation_id"),
        }
        try:
            redis_client.setex(cache_key, ttl, json_dumps(payload))
            logger.info("Cached Message-ID mapping in Redis: %s -> %s (TTL: %ds)", clean_msg_id, payload, ttl)
            return True
        except (redis.RedisError, Exception) as exc:
            logger.warning("Redis SETEX failed for %s: %s", cache_key, exc)
            return False


class IspReplyMonitor:
    """Manages temporary event-driven monitoring state for ISP email replies in Redis.

    Keeps minimal in-memory state in Redis to avoid hitting PostgreSQL on every Celery Beat tick.
    When incoming replies arrive, response_received is set immediately in Redis, allowing
    Celery Beat to inspect Redis state first without querying the database for all monitors.
    """

    PREFIX = "isp:monitor:"
    ALERT_INDEX_PREFIX = "isp:monitor:alert:"
    INDEX_KEY = "isp:monitor:index"
    LOCK_KEY = "isp:monitor:lock:scan"

    @classmethod
    def clean_id(cls, message_id: Optional[str]) -> str:
        """Strip enclosing angle brackets and whitespace from Message-ID."""
        if not message_id:
            return ""
        return message_id.strip().strip("<>").strip()

    @classmethod
    def register_monitor(
        cls,
        *,
        alert_id: int,
        escalation_id: int,
        message_id: str,
        isp_email: str,
        isp_email_id: Optional[int],
        sensor_name: str,
        site_name: str,
        isp_name: str,
        circuit_id: str,
        original_subject: str,
        original_references: Optional[list[str]] = None,
        to_addresses: Optional[list[str]] = None,
        cc_addresses: Optional[list[str]] = None,
        timeout_minutes: Optional[int] = None,
    ) -> bool:
        """Register an outbound ISP email for automated reply monitoring."""
        clean_msg_id = cls.clean_id(message_id)
        if not clean_msg_id:
            logger.error("Cannot register ISP monitor with empty Message-ID")
            return False

        timeout = timeout_minutes or settings.isp_reply_timeout_minutes
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        next_reminder = now_utc + datetime.timedelta(minutes=timeout)

        monitor_data = {
            "alert_id": alert_id,
            "escalation_id": escalation_id,
            "message_id": clean_msg_id,
            "isp_email": isp_email,
            "isp_email_id": isp_email_id,
            "sensor_name": sensor_name,
            "site_name": site_name,
            "isp_name": isp_name,
            "circuit_id": circuit_id,
            "original_subject": original_subject,
            "original_references": original_references or [],
            "to_addresses": to_addresses or [isp_email],
            "cc_addresses": cc_addresses or [],
            "reminder_count": 0,
            "response_received": False,
            "monitoring_active": True,
            "sent_at": now_utc.isoformat(),
            "next_reminder_time": next_reminder.isoformat(),
            "last_reminder_sent_at": None,
        }

        key = f"{cls.PREFIX}{clean_msg_id}"
        alert_key = f"{cls.ALERT_INDEX_PREFIX}{alert_id}"
        ttl = settings.isp_monitor_redis_ttl_seconds

        try:
            pipe = redis_client.pipeline()
            pipe.setex(key, ttl, json_dumps(monitor_data))
            pipe.setex(alert_key, ttl, clean_msg_id)
            pipe.sadd(cls.INDEX_KEY, clean_msg_id)
            pipe.execute()
            logger.info(
                "Registered ISP reply monitor [Alert: %s | MsgID: %s | NextReminder: %s]",
                alert_id,
                clean_msg_id,
                next_reminder.isoformat(),
            )
            return True
        except (redis.RedisError, Exception) as exc:
            logger.error("Failed to register ISP reply monitor for MsgID %s: %s", clean_msg_id, exc)
            return False

    @classmethod
    def get_monitor(cls, message_id: str) -> Optional[dict[str, Any]]:
        """Retrieve monitor state for a given Message-ID."""
        clean_msg_id = cls.clean_id(message_id)
        if not clean_msg_id:
            return None
        key = f"{cls.PREFIX}{clean_msg_id}"
        try:
            data = redis_client.get(key)
            if data:
                return json_loads(data)
        except Exception as exc:
            logger.warning("Redis GET failed for %s: %s", key, exc)
        return None

    @classmethod
    def get_monitor_by_alert_id(cls, alert_id: int) -> Optional[dict[str, Any]]:
        """Retrieve monitor state by alert_id."""
        alert_key = f"{cls.ALERT_INDEX_PREFIX}{alert_id}"
        try:
            clean_msg_id = redis_client.get(alert_key)
            if clean_msg_id:
                return cls.get_monitor(clean_msg_id)
        except Exception as exc:
            logger.warning("Redis GET failed for %s: %s", alert_key, exc)
        return None

    @classmethod
    def mark_response_received(
        cls,
        *,
        message_id: Optional[str] = None,
        in_reply_to: Optional[str] = None,
        references: Optional[list[str]] = None,
        alert_id: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        """Mark monitor as response_received=True and stop active monitoring in Redis.

        Matches against candidate Message-IDs (direct message_id, in_reply_to, references)
        or alert_id to find and cancel the active monitor immediately upon reply ingestion.
        """
        candidates: list[str] = []
        if in_reply_to:
            clean_irt = cls.clean_id(in_reply_to)
            if clean_irt:
                candidates.append(clean_irt)
        if references:
            for ref in references:
                clean_ref = cls.clean_id(ref)
                if clean_ref and clean_ref not in candidates:
                    candidates.append(clean_ref)
        if message_id:
            clean_m = cls.clean_id(message_id)
            if clean_m and clean_m not in candidates:
                candidates.append(clean_m)

        target_msg_id: Optional[str] = None
        monitor_state: Optional[dict[str, Any]] = None

        # 1. Search candidate message IDs
        for cid in candidates:
            m = cls.get_monitor(cid)
            if m:
                target_msg_id = cid
                monitor_state = m
                break

        # 2. Fallback to alert_id lookup
        if not monitor_state and alert_id:
            alert_key = f"{cls.ALERT_INDEX_PREFIX}{alert_id}"
            try:
                msg_id_from_alert = redis_client.get(alert_key)
                if msg_id_from_alert:
                    m = cls.get_monitor(msg_id_from_alert)
                    if m:
                        target_msg_id = msg_id_from_alert
                        monitor_state = m
            except Exception as exc:
                logger.warning("Error fetching alert key %s: %s", alert_key, exc)

        if not target_msg_id or not monitor_state:
            logger.debug("No active ISP reply monitor matched candidate IDs %s / alert_id %s", candidates, alert_id)
            return None

        # Update Redis state
        monitor_state["response_received"] = True
        monitor_state["monitoring_active"] = False
        monitor_state["response_received_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

        root_msg_id = monitor_state.get("message_id") or target_msg_id
        key = f"{cls.PREFIX}{root_msg_id}"
        ttl = settings.isp_monitor_redis_ttl_seconds
        try:
            pipe = redis_client.pipeline()
            pipe.setex(key, ttl, json_dumps(monitor_state))
            pipe.srem(cls.INDEX_KEY, root_msg_id)
            if alert_id:
                pipe.delete(f"{cls.ALERT_INDEX_PREFIX}{alert_id}")
            pipe.execute()
            logger.info(
                "Marked ISP monitor response_received=True in Redis [MsgID: %s | Alert: %s]",
                root_msg_id,
                monitor_state.get("alert_id"),
            )
            return monitor_state
        except Exception as exc:
            logger.error("Failed to update response_received for monitor %s: %s", target_msg_id, exc)
            return monitor_state

    @classmethod
    def update_monitor(cls, message_id: str, updates: dict[str, Any]) -> bool:
        """Update fields in an existing monitor state, preserving TTL."""
        clean_msg_id = cls.clean_id(message_id)
        if not clean_msg_id:
            return False
        key = f"{cls.PREFIX}{clean_msg_id}"
        try:
            current = cls.get_monitor(clean_msg_id)
            if not current:
                return False
            current.update(updates)
            ttl = redis_client.ttl(key)
            if ttl is None or ttl <= 0:
                ttl = settings.isp_monitor_redis_ttl_seconds
            redis_client.setex(key, ttl, json_dumps(current))
            return True
        except Exception as exc:
            logger.error("Failed to update monitor %s: %s", clean_msg_id, exc)
            return False

    @classmethod
    def stop_monitoring(cls, message_id: str, alert_id: Optional[int] = None) -> None:
        """Stop monitoring for a given Message-ID and remove from active index."""
        clean_msg_id = cls.clean_id(message_id)
        if not clean_msg_id:
            return
        key = f"{cls.PREFIX}{clean_msg_id}"
        try:
            current = cls.get_monitor(clean_msg_id)
            if current:
                current["monitoring_active"] = False
                ttl = redis_client.ttl(key)
                if ttl is None or ttl <= 0:
                    ttl = settings.isp_monitor_redis_ttl_seconds
                redis_client.setex(key, ttl, json_dumps(current))
            redis_client.srem(cls.INDEX_KEY, clean_msg_id)
            if alert_id:
                redis_client.delete(f"{cls.ALERT_INDEX_PREFIX}{alert_id}")
            logger.info("Stopped monitoring for Message-ID: %s", clean_msg_id)
        except Exception as exc:
            logger.warning("Error stopping monitor for %s: %s", clean_msg_id, exc)

    @classmethod
    def get_all_active_monitors(cls) -> list[dict[str, Any]]:
        """Retrieve all currently active monitor states from Redis in a single batch."""
        active_monitors: list[dict[str, Any]] = []
        try:
            members = redis_client.smembers(cls.INDEX_KEY)
            if not members:
                return []

            pipe = redis_client.pipeline()
            member_list = list(members)
            for msg_id in member_list:
                pipe.get(f"{cls.PREFIX}{msg_id}")
            results = pipe.execute()

            stale_members: list[str] = []
            for msg_id, raw_data in zip(member_list, results):
                if not raw_data:
                    stale_members.append(msg_id)
                    continue
                try:
                    state = json_loads(raw_data)
                    if state.get("monitoring_active", False) and not state.get("response_received", False):
                        active_monitors.append(state)
                    else:
                        stale_members.append(msg_id)
                except Exception:
                    stale_members.append(msg_id)

            if stale_members:
                redis_client.srem(cls.INDEX_KEY, *stale_members)

            return active_monitors
        except Exception as exc:
            logger.error("Failed to retrieve active monitors from Redis: %s", exc)
            return []

    @classmethod
    def acquire_scan_lock(cls) -> bool:
        """Acquire distributed lock to prevent overlapping Celery Beat executions."""
        try:
            return bool(
                redis_client.set(
                    cls.LOCK_KEY,
                    "locked",
                    nx=True,
                    ex=settings.isp_monitor_lock_ttl_seconds,
                )
            )
        except Exception as exc:
            logger.warning("Failed to acquire scan lock: %s", exc)
            return False

    @classmethod
    def release_scan_lock(cls) -> None:
        """Release the distributed scan lock."""
        try:
            redis_client.delete(cls.LOCK_KEY)
        except Exception as exc:
            logger.warning("Failed to release scan lock: %s", exc)