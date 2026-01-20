import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api import chat


def _payload_from_response(response):
    # Decode JSONResponse bodies without spinning up an ASGI client.
    return json.loads(response.body)


@pytest.fixture(autouse=True)
def _fast_health_timeouts(monkeypatch):
    # Keep health checks fast in tests without changing production defaults.
    monkeypatch.setenv("DB_HEALTH_TIMEOUT_S", "0.1")
    monkeypatch.setenv("MINIO_HEALTH_TIMEOUT_S", "0.1")
    monkeypatch.setenv("REDIS_HEALTH_TIMEOUT_S", "0.1")


def test_health_detailed_ok(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    # Force SQLite for predictable health check behavior in tests.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(chat, "_ping_minio", lambda _timeout_seconds: None)
    resp = asyncio.run(chat.health_detailed())
    assert resp.status_code == 200
    payload = _payload_from_response(resp)
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0
    assert payload["components"]["app"]["healthy"] is True
    assert payload["components"]["database"]["healthy"] is True
    assert payload["components"]["database"]["latency_ms"] >= 0
    assert payload["components"]["minio"]["healthy"] is True
    assert payload["components"]["minio"]["latency_ms"] >= 0
    assert payload["components"]["redis"]["enabled"] is False
    assert payload["components"]["redis"]["healthy"] is True
    assert payload["components"]["redis"]["latency_ms"] == 0


def test_health_detailed_db_unreachable(tmp_path, monkeypatch):
    bad_path = tmp_path / "missing" / "dev.db"
    # Ensure we do not attempt a Postgres connection during tests.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(bad_path))
    monkeypatch.setattr(chat, "_ping_minio", lambda _timeout_seconds: None)
    resp = asyncio.run(chat.health_detailed())
    assert resp.status_code == 503
    payload = _payload_from_response(resp)
    assert payload["healthy"] is False
    assert payload["components"]["database"]["healthy"] is False


def test_health_detailed_minio_unreachable(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))

    def _raise_minio(_timeout_seconds):
        raise RuntimeError("minio down")

    monkeypatch.setattr(chat, "_ping_minio", _raise_minio)
    resp = asyncio.run(chat.health_detailed())
    assert resp.status_code == 503
    payload = _payload_from_response(resp)
    assert payload["healthy"] is False
    assert payload["components"]["minio"]["healthy"] is False


def test_health_detailed_minio_retries_with_backoff(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("MINIO_HEALTH_TIMEOUT_S", "1")
    monkeypatch.setattr(
        chat, "_MINIO_CIRCUIT", chat.CircuitBreaker(failure_threshold=3, reset_timeout_s=60.0)
    )

    async def _healthy_db():
        return chat.JSONResponse(status_code=200, content={"healthy": True, "latency_ms": 0})

    monkeypatch.setattr(chat, "health_db", _healthy_db)

    calls = []

    def _flaky_minio(_timeout_seconds):
        calls.append("call")
        if len(calls) < 4:
            raise RuntimeError("minio flaky")

    sleep_calls = []

    def _fake_sleep(duration):
        sleep_calls.append(duration)

    monkeypatch.setattr(chat, "_ping_minio", _flaky_minio)
    monkeypatch.setattr(chat.time, "sleep", _fake_sleep)
    resp = asyncio.run(chat.health_detailed())
    payload = _payload_from_response(resp)
    assert resp.status_code == 200
    assert payload["components"]["minio"]["healthy"] is True
    assert calls == ["call", "call", "call", "call"]
    assert sleep_calls == [0.1, 0.2, 0.4]


def test_health_detailed_redis_unreachable(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    # Enable Redis checks so failures surface in the detailed payload.
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(chat, "_ping_minio", lambda _timeout_seconds: None)

    def _raise_redis(_redis_url, _timeout_seconds):
        raise RuntimeError("redis down")

    monkeypatch.setattr(chat, "_ping_redis", _raise_redis)
    resp = asyncio.run(chat.health_detailed())
    assert resp.status_code == 503
    payload = _payload_from_response(resp)
    assert payload["healthy"] is False
    assert payload["components"]["redis"]["enabled"] is True
    assert payload["components"]["redis"]["healthy"] is False


def test_health_detailed_redis_ok(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    # Enable Redis checks to exercise the healthy cache path.
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(chat, "_ping_minio", lambda _timeout_seconds: None)
    monkeypatch.setattr(chat, "_ping_redis", lambda _redis_url, _timeout_seconds: None)
    resp = asyncio.run(chat.health_detailed())
    assert resp.status_code == 200
    payload = _payload_from_response(resp)
    assert payload["healthy"] is True
    assert payload["components"]["redis"]["enabled"] is True
    assert payload["components"]["redis"]["healthy"] is True


def test_health_detailed_minio_circuit_breaker_opens(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(
        chat, "_MINIO_CIRCUIT", chat.CircuitBreaker(failure_threshold=3, reset_timeout_s=60.0)
    )

    def _raise_minio(_timeout_seconds):
        raise RuntimeError("minio down")

    monkeypatch.setattr(chat, "_ping_minio", _raise_minio)
    for _ in range(3):
        resp = asyncio.run(chat.health_detailed())
        assert resp.status_code == 503

    def _unexpected_minio(_timeout_seconds):
        raise AssertionError("minio should not be called when circuit is open")

    monkeypatch.setattr(chat, "_ping_minio", _unexpected_minio)
    resp = asyncio.run(chat.health_detailed())
    payload = _payload_from_response(resp)
    assert payload["components"]["minio"]["healthy"] is False
    assert payload["components"]["minio"]["circuit_open"] is True


def test_health_detailed_redis_circuit_breaker_opens(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(chat, "_ping_minio", lambda _timeout_seconds: None)
    monkeypatch.setattr(
        chat, "_REDIS_CIRCUIT", chat.CircuitBreaker(failure_threshold=3, reset_timeout_s=60.0)
    )

    def _raise_redis(_redis_url, _timeout_seconds):
        raise RuntimeError("redis down")

    monkeypatch.setattr(chat, "_ping_redis", _raise_redis)
    for _ in range(3):
        resp = asyncio.run(chat.health_detailed())
        assert resp.status_code == 503

    def _unexpected_redis(_redis_url, _timeout_seconds):
        raise AssertionError("redis should not be called when circuit is open")

    monkeypatch.setattr(chat, "_ping_redis", _unexpected_redis)
    resp = asyncio.run(chat.health_detailed())
    payload = _payload_from_response(resp)
    assert payload["components"]["redis"]["healthy"] is False
    assert payload["components"]["redis"]["circuit_open"] is True


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (health)
# - [ ] summary is concise and imperative
