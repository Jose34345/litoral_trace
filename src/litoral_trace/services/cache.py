"""Estrategia de cache deterministica en Redis para telemetria satelital."""
from __future__ import annotations

import json
import time
from typing import Any

from litoral_trace.config import get_settings

_redis_client: Any | None = None
_redis_available: bool = False


def get_redis_client() -> Any | None:
    """Obtiene o inicializa la conexion singleton a Redis con manejo gracioso de fallos."""
    global _redis_client, _redis_available
    if _redis_client is not None:
        return _redis_client if _redis_available else None

    redis_url = get_settings().cache.redis_url
    try:
        import redis

        client = redis.Redis.from_url(
            redis_url,
            socket_timeout=1.5,
            socket_connect_timeout=1.5,
        )
        client.ping()
        _redis_client = client
        _redis_available = True
        return _redis_client
    except Exception:
        _redis_client = None
        _redis_available = False
        return None


def build_ndvi_cache_key(
    org_id: int,
    lote_id: int,
    geometry_hash: str,
    start_date: str,
    end_date: str,
    cloud_threshold: float,
    algorithm_version: str = "2.4.0-gee-sentinel2-scl-v2",
) -> str:
    """Genera una clave de cache deterministica para consultas satelitales."""
    return (
        "ndvi:v1:"
        f"{org_id}:{lote_id}:{geometry_hash}:{start_date}:{end_date}:"
        f"{cloud_threshold:.1f}:{algorithm_version}"
    )


def get_cached_satellite_data(
    cache_key: str,
) -> tuple[dict[str, Any] | None, int]:
    """Intenta recuperar datos desde Redis y devuelve (data, redis_read_ms)."""
    t0 = time.time()
    client = get_redis_client()
    if not client:
        return None, int((time.time() - t0) * 1000)
    try:
        raw_val = client.get(cache_key)
        read_ms = int((time.time() - t0) * 1000)
        if raw_val:
            data = json.loads(raw_val)
            return data, read_ms
    except Exception:
        pass
    return None, int((time.time() - t0) * 1000)


def set_cached_satellite_data(
    cache_key: str,
    data: dict[str, Any],
    ttl_seconds: int = 86400,
) -> tuple[bool, int]:
    """Guarda datos procesados en Redis con TTL configurable."""
    t0 = time.time()
    client = get_redis_client()
    if not client:
        return False, int((time.time() - t0) * 1000)
    try:
        client.setex(cache_key, ttl_seconds, json.dumps(data))
        write_ms = int((time.time() - t0) * 1000)
        return True, write_ms
    except Exception:
        return False, int((time.time() - t0) * 1000)
