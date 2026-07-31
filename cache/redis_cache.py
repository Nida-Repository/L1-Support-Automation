import json
import logging
import os
from typing import Any, Optional
from urllib.parse import urlparse

import redis
from dotenv import load_dotenv

load_dotenv()

# 1. Instantiate module-level logger
logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL")


def _safe_url(url: str) -> str:
    """Scrub passwords from Redis URLs before logging."""
    try:
        parsed = urlparse(url)
        if parsed.password:
            return url.replace(parsed.password, "******")
        return url
    except Exception:
        return "[masked_redis_url]"


if not REDIS_URL:
    logger.critical("REDIS_URL environment variable is missing!")
    raise RuntimeError("REDIS_URL environment variable is not set")

logger.info("Initializing Redis connection pool: %s", _safe_url(REDIS_URL))

redis_pool = redis.ConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=3,
    socket_connect_timeout=3,
    retry_on_timeout=True,
    health_check_interval=30,
    max_connections=50,
)
redis_client = redis.Redis(connection_pool=redis_pool)


class CacheService:
    """
    Read-through cache for site mappings. Non-critical path:
    on any Redis failure we log and degrade gracefully rather than
    blocking webhook processing.
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
            site_info = json.loads(cached_data)
            logger.debug("Cache hit for %s", cache_key)
            return site_info
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error("Corrupt cache value at %s: %s", cache_key, exc)
            return None

    @staticmethod
    def set_sensor_site_info(
        sensor_id: int, site_info: dict[str, Any], ttl_seconds: int = 3600
    ) -> bool:
        cache_key = f"cache:sensor:{sensor_id}"
        try:
            redis_client.setex(cache_key, ttl_seconds, json.dumps(site_info))
            logger.info("Successfully cached site info for sensor_id %s (TTL: %ds)", sensor_id, ttl_seconds)
            return True
        except redis.RedisError as exc:
            logger.warning("Redis SETEX failed for %s: %s", cache_key, exc)
            return False


class IncidentStateTracker:
    """
    On Redis failure we fail OPEN (treat as not-a-duplicate) so an outage 
    never silently drops a real alert.
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
    def set_sensor_state(sensor_id: int, status: str, ttl_seconds: int = 86400) -> bool:
        cache_key = f"state:sensor:{sensor_id}"
        try:
            redis_client.setex(cache_key, ttl_seconds, status)
            logger.info("Updated active state for sensor_id %s -> '%s' (TTL: %ds)", sensor_id, status, ttl_seconds)
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
            logger.warning("Dedup check failed for sensor_id %s (%s) — failing open to allow alert", sensor_id, exc)
            return False  # fail open — never silently swallow an alert

    @staticmethod
    def ping() -> bool:
        try:
            is_alive = redis_client.ping()
            logger.debug("Redis ping status: %s", is_alive)
            return is_alive
        except redis.RedisError as exc:
            logger.warning("Redis ping failed: %s", exc)
            return False