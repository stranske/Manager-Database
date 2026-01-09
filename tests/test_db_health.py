import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import health_db


def _payload_from_response(response):
    # Decode JSONResponse bodies without spinning up an ASGI client.
    return json.loads(response.body)


def test_health_db_ok(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    # Force SQLite for predictable health check behavior in tests.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(health_db())
    assert resp.status_code == 200
    payload = _payload_from_response(resp)
    assert payload["healthy"] is True
    assert payload["latency_ms"] >= 0


def test_health_db_unreachable(tmp_path, monkeypatch):
    bad_path = tmp_path / "missing" / "dev.db"
    # Ensure we do not attempt a Postgres connection during tests.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(bad_path))
    resp = asyncio.run(health_db())
    assert resp.status_code == 503
    payload = _payload_from_response(resp)
    assert payload["healthy"] is False
    assert payload["latency_ms"] >= 0


def test_health_db_timeout(monkeypatch):
    def slow_ping(_timeout_seconds: float) -> None:
        # Sleep long enough to trip the timeout without touching the DB.
        time.sleep(0.2)

    # Avoid accidental Postgres connections if DB_URL is set in the environment.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_HEALTH_TIMEOUT_S", "0.05")
    monkeypatch.setattr("api.chat._ping_db", slow_ping)
    start = time.perf_counter()
    resp = asyncio.run(health_db())
    elapsed = time.perf_counter() - start
    assert elapsed < 0.5
    assert resp.status_code == 503


def test_health_db_timeout_cap(monkeypatch):
    # Ensure the helper never exceeds the 5s cap, even if env is larger.
    # Strip DB_URL to keep the health path on SQLite.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_HEALTH_TIMEOUT_S", "10")
    from api.chat import _db_timeout_seconds

    assert _db_timeout_seconds() == 5.0
