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
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(health_db())
    assert resp.status_code == 200
    payload = _payload_from_response(resp)
    assert payload["healthy"] is True
    assert payload["latency_ms"] >= 0


def test_health_db_failure(monkeypatch):
    def fail_ping(_timeout_seconds: float) -> None:
        raise RuntimeError("boom")

    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setattr("api.chat._ping_db", fail_ping)
    resp = asyncio.run(health_db())
    assert resp.status_code == 503
    payload = _payload_from_response(resp)
    assert payload["healthy"] is False


def test_health_db_timeout(monkeypatch):
    def slow_ping(_timeout_seconds: float) -> None:
        time.sleep(0.2)

    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_HEALTH_TIMEOUT_S", "0.05")
    monkeypatch.setattr("api.chat._ping_db", slow_ping)
    resp = asyncio.run(health_db())
    assert resp.status_code == 503
    payload = _payload_from_response(resp)
    assert payload["healthy"] is False
