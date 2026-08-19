"""Redis Caching and Incident State Tracking.

Provides a read-through cache for site and sensor metadata, and a fail-open
deduplication tracker for incoming PRTG sensor alerts.
"""
from __future__ import annotations

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