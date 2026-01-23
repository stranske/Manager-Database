"""Cache helpers with Redis/in-memory backends and Prometheus metrics."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from threading import Lock
from typing import Any

from cachetools import TTLCache  # type: ignore[import-untyped]
from prometheus_client import Counter, Gauge

CACHE_HITS = Counter("cache_hits_total", "Cache hits by namespace.", ("namespace",))
CACHE_MISSES = Counter("cache_misses_total", "Cache misses by namespace.", ("namespace",))
CACHE_HIT_RATIO = Gauge("cache_hit_ratio", "Cache hit ratio by namespace.", ("namespace",))

_CACHE_LOCK = Lock()
_CACHE_BACKEND: _CacheBackend | None = None
_METRICS_LOCK = Lock()
_CACHE_METRICS: dict[str, dict[str, int]] = {}


def _cache_ttl_seconds() -> int:
    return max(int(os.getenv("CACHE_TTL_SECONDS", "60")), 1)


def _cache_max_items() -> int:
    return max(int(os.getenv("CACHE_MAX_ITEMS", "512")), 1)


def _build_redis_client(redis_url: str):
    # Import lazily so Redis remains optional in environments without it.
    import redis

    return redis.Redis.from_url(redis_url)


@dataclass(frozen=True)
class _CacheBackend:
    get: Callable[[str], Any]
    set: Callable[[str, str, int], None]
    delete_prefix: Callable[[str], None]


def _get_backend() -> _CacheBackend:
    global _CACHE_BACKEND
    if _CACHE_BACKEND is not None:
        return _CACHE_BACKEND

    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        try:
            client = _build_redis_client(redis_url)
        except Exception:
            client = None
        if client is not None:

            def _redis_get(key: str) -> Any:
                return client.get(key)

            def _redis_set(key: str, payload: str, ttl: int) -> None:
                client.setex(key, ttl, payload)

            def _redis_delete_prefix(prefix: str) -> None:
                keys = list(client.scan_iter(f"{prefix}*"))
                if keys:
                    client.delete(*keys)

            _CACHE_BACKEND = _CacheBackend(
                get=_redis_get, set=_redis_set, delete_prefix=_redis_delete_prefix
            )
            return _CACHE_BACKEND

    cache = TTLCache(maxsize=_cache_max_items(), ttl=_cache_ttl_seconds())
    cache_lock = Lock()

    def _memory_get(key: str) -> Any:
        with cache_lock:
            return cache.get(key)

    def _memory_set(key: str, payload: str, ttl: int) -> None:
        # TTLCache uses the ttl configured at initialization.
        with cache_lock:
            cache[key] = payload

    def _memory_delete_prefix(prefix: str) -> None:
        with cache_lock:
            keys = [key for key in cache.keys() if str(key).startswith(prefix)]
            for key in keys:
                cache.pop(key, None)

    _CACHE_BACKEND = _CacheBackend(
        get=_memory_get, set=_memory_set, delete_prefix=_memory_delete_prefix
    )
    return _CACHE_BACKEND


def reset_cache_backend() -> None:
    """Reset the cached backend (useful for tests)."""
    global _CACHE_BACKEND
    _CACHE_BACKEND = None


def reset_cache_stats() -> None:
    """Reset in-process cache statistics (useful for tests)."""
    with _METRICS_LOCK:
        _CACHE_METRICS.clear()


def _make_cache_key(namespace: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    raw = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{namespace}:{digest}"


def _record_cache_metric(namespace: str, hit: bool) -> None:
    with _METRICS_LOCK:
        stats = _CACHE_METRICS.setdefault(namespace, {"hits": 0, "misses": 0})
        if hit:
            stats["hits"] += 1
            CACHE_HITS.labels(namespace=namespace).inc()
        else:
            stats["misses"] += 1
            CACHE_MISSES.labels(namespace=namespace).inc()
        total = stats["hits"] + stats["misses"]
        if total:
            CACHE_HIT_RATIO.labels(namespace=namespace).set(stats["hits"] / total)


def get_cache_stats(namespace: str) -> dict[str, int | float]:
    """Return cache hit/miss counts and ratio for a namespace."""
    with _METRICS_LOCK:
        stats = _CACHE_METRICS.get(namespace, {"hits": 0, "misses": 0}).copy()
    total = stats["hits"] + stats["misses"]
    ratio = (stats["hits"] / total) if total else 0.0
    return {"hits": stats["hits"], "misses": stats["misses"], "hit_ratio": ratio}


def cache_get(namespace: str, key: str) -> Any | None:
    backend = _get_backend()
    payload = backend.get(key)
    if payload is None:
        _record_cache_metric(namespace, hit=False)
        return None
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    try:
        value = json.loads(payload)
    except (TypeError, json.JSONDecodeError):
        value = payload
    _record_cache_metric(namespace, hit=True)
    return value


def cache_set(key: str, value: Any, *, ttl: int | None = None) -> None:
    backend = _get_backend()
    payload = json.dumps(value, default=str)
    backend.set(key, payload, ttl or _cache_ttl_seconds())


def invalidate_cache_prefix(prefix: str) -> None:
    backend = _get_backend()
    backend.delete_prefix(prefix)


def cache_query(
    namespace: str,
    *,
    ttl: int | None = None,
    skip_args: int = 0,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Cache results for query-like functions."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            cache_key = _make_cache_key(namespace, args[skip_args:], kwargs)
            cached = cache_get(namespace, cache_key)
            if cached is not None:
                return cached
            result = func(*args, **kwargs)
            if result is not None:
                cache_set(cache_key, result, ttl=ttl)
            return result

        return wrapper

    return decorator
