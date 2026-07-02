"""Central runtime configuration defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_DB_PATH = "manager_database.db"
DEFAULT_CACHE_TTL_SECONDS = 60
DEFAULT_CACHE_MAX_ITEMS = 512
DEFAULT_CHAT_RATE_LIMIT_PER_MINUTE = 10
DEFAULT_CHAT_RATE_LIMIT_WINDOW_SECONDS = 60.0
DEFAULT_CHAT_SESSION_COOKIE_NAME = "session_id"


def _positive_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return value if value > 0 else default


def _positive_float_env(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = float(raw_value)
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
    chat_chain_fallback_mode: str
    chat_rate_limit_per_minute: int
    chat_rate_limit_window_seconds: float
    chat_session_cookie_name: str
    chat_session_cookie_secret: str | None
    llm_zone: str


def load_runtime_config() -> RuntimeConfig:
    return RuntimeConfig(
        db_url=os.getenv("DB_URL"),
        db_path=os.getenv("DB_PATH", DEFAULT_DB_PATH),
        cache_ttl_seconds=_positive_int_env("CACHE_TTL_SECONDS", DEFAULT_CACHE_TTL_SECONDS),
        cache_max_items=_positive_int_env("CACHE_MAX_ITEMS", DEFAULT_CACHE_MAX_ITEMS),
        redis_url=os.getenv("REDIS_URL"),
        chat_chain_fallback_mode=os.getenv("CHAT_CHAIN_FALLBACK_MODE", ""),
        chat_rate_limit_per_minute=_positive_int_env(
            "CHAT_RATE_LIMIT_PER_MINUTE", DEFAULT_CHAT_RATE_LIMIT_PER_MINUTE
        ),
        chat_rate_limit_window_seconds=_positive_float_env(
            "CHAT_RATE_LIMIT_WINDOW_SECONDS", DEFAULT_CHAT_RATE_LIMIT_WINDOW_SECONDS
        ),
        chat_session_cookie_name=os.getenv(
            "CHAT_SESSION_COOKIE_NAME", DEFAULT_CHAT_SESSION_COOKIE_NAME
        ),
        chat_session_cookie_secret=os.getenv("CHAT_SESSION_COOKIE_SECRET"),
        llm_zone=os.getenv("LLM_ZONE", ""),
    )
