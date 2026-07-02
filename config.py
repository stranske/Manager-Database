"""Central runtime configuration defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_DB_PATH = "manager_database.db"
DEFAULT_CACHE_TTL_SECONDS = 60
DEFAULT_CACHE_MAX_ITEMS = 512


def _positive_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return value if value > 0 else default


@dataclass(frozen=True)
class RuntimeConfig:
    db_url: str | None
    db_path: str
    cache_ttl_seconds: int
    cache_max_items: int
    redis_url: str | None


def load_runtime_config() -> RuntimeConfig:
    return RuntimeConfig(
        db_url=os.getenv("DB_URL"),
        db_path=os.getenv("DB_PATH", DEFAULT_DB_PATH),
        cache_ttl_seconds=_positive_int_env("CACHE_TTL_SECONDS", DEFAULT_CACHE_TTL_SECONDS),
        cache_max_items=_positive_int_env("CACHE_MAX_ITEMS", DEFAULT_CACHE_MAX_ITEMS),
        redis_url=os.getenv("REDIS_URL"),
    )
