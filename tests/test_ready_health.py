import asyncio
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import health_ready, health_readyz


def _payload_from_response(response):
    # Decode JSONResponse bodies without spinning up an ASGI client.
    return json.loads(response.body)


def test_health_ready_ok(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    # Force SQLite for predictable health check behavior in tests.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(health_ready())
    assert resp.status_code == 200
    payload = _payload_from_response(resp)
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0
    assert payload["db_latency_ms"] >= 0


def test_health_ready_db_unreachable(tmp_path, monkeypatch):
    bad_path = tmp_path / "missing" / "dev.db"
    # Ensure we do not attempt a Postgres connection during tests.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(bad_path))
    resp = asyncio.run(health_ready())
    assert resp.status_code == 503
    payload = _payload_from_response(resp)
    assert payload["healthy"] is False


def test_health_readyz_ok(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    # Keep the readyz alias consistent with the main readiness behavior.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(health_readyz())
    assert resp.status_code == 200
    payload = _payload_from_response(resp)
    assert payload["healthy"] is True
