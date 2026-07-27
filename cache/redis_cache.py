import json
import os
from typing import Any, Optional
import redis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")

# Connect to Redis
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, protocol=2)


class CacheService:
    """Read-Through Cache service for site mappings, devices, and email lists."""

    @staticmethod
    def get_sensor_site_info(sensor_id: int) -> Optional[dict[str, Any]]:
        cache_key = f"cache:sensor:{sensor_id}"
        cached_data = redis_client.get(cache_key)

        if cached_data:
            return json.loads(cached_data)

        return None

    @staticmethod
    def set_sensor_site_info(sensor_id: int, site_info: dict[str, Any], ttl_seconds: int = 3600) -> None:
        cache_key = f"cache:sensor:{sensor_id}"
        redis_client.setex(cache_key, ttl_seconds, json.dumps(site_info))


class IncidentStateTracker:
    """Tracks active sensor states to handle incident deduplication."""

    @staticmethod
    def get_sensor_state(sensor_id: int) -> Optional[str]:
        return redis_client.get(f"state:sensor:{sensor_id}")

    @staticmethod
    def set_sensor_state(sensor_id: int, status: str, ttl_seconds: int = 86400) -> None:
        redis_client.setex(f"state:sensor:{sensor_id}", ttl_seconds, status)

    @staticmethod
    def is_duplicate_alert(sensor_id: int, current_status: str) -> bool:
        last_status = IncidentStateTracker.get_sensor_state(sensor_id)
        return last_status == current_status