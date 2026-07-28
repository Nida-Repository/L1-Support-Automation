import json
import logging
import os
from typing import Any, Optional

import redis
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL")

if not REDIS_URL:
    raise RuntimeError("REDIS_URL environment variable is not set")

# Pooled client with sane timeouts — a hung Redis connection should never
# be allowed to hang the webhook or the worker indefinitely.
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
    """Read-through cache for site mappings. Non-critical path:
    on any Redis failure we log and degrade gracefully rather than
    blocking webhook processing."""

    @staticmethod
    def get_sensor_site_info(sensor_id: int) -> Optional[dict[str, Any]]:
        cache_key = f"cache:sensor:{sensor_id}"
        try:
            cached_data = redis_client.get(cache_key)
        except redis.RedisError as exc:
            logger.warning(f"Redis GET failed for {cache_key}: {exc}")
            return None

        if not cached_data:
            return None

        try:
            return json.loads(cached_data)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error(f"Corrupt cache value at {cache_key}: {exc}")
            return None

    @staticmethod
    def set_sensor_site_info(
        sensor_id: int, site_info: dict[str, Any], ttl_seconds: int = 3600
    ) -> bool:
        cache_key = f"cache:sensor:{sensor_id}"
        try:
            redis_client.setex(cache_key, ttl_seconds, json.dumps(site_info))
            return True
        except redis.RedisError as exc:
            logger.warning(f"Redis SETEX failed for {cache_key}: {exc}")
            return False


class IncidentStateTracker:
    """Tracks active sensor states for dedup. On Redis failure we fail
    OPEN (treat as not-a-duplicate) so an outage never silently drops
    a real alert."""

    @staticmethod
    def get_sensor_state(sensor_id: int) -> Optional[str]:
        try:
            return redis_client.get(f"state:sensor:{sensor_id}")
        except redis.RedisError as exc:
            logger.warning(f"Redis GET failed for state:sensor:{sensor_id}: {exc}")
            return None

    @staticmethod
    def set_sensor_state(sensor_id: int, status: str, ttl_seconds: int = 86400) -> bool:
        try:
            redis_client.setex(f"state:sensor:{sensor_id}", ttl_seconds, status)
            return True
        except redis.RedisError as exc:
            logger.warning(f"Redis SETEX failed for state:sensor:{sensor_id}: {exc}")
            return False

    @staticmethod
    def is_duplicate_alert(sensor_id: int, current_status: str) -> bool:
        try:
            last_status = IncidentStateTracker.get_sensor_state(sensor_id)
            return last_status == current_status
        except redis.RedisError as exc:
            logger.warning(f"Dedup check failed for sensor {sensor_id}: {exc}")
            return False  # fail open — never silently swallow an alert

    @staticmethod
    def ping() -> bool:
        try:
            return redis_client.ping()
        except redis.RedisError:
            return False