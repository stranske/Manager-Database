import asyncio
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api import chat


def _payload_from_response(response):
    # Decode JSONResponse bodies without spinning up an ASGI client.
    return json.loads(response.body)


def test_health_detailed_ok(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    # Force SQLite for predictable health check behavior in tests.
    monkeypatch.delenv("DB_URL", raising=False)
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
    monkeypatch.setenv("DB_PATH", str(db_path))

    def _raise_minio(_timeout_seconds):
        raise RuntimeError("minio down")

    monkeypatch.setattr(chat, "_ping_minio", _raise_minio)
    resp = asyncio.run(chat.health_detailed())
    assert resp.status_code == 503
    payload = _payload_from_response(resp)
    assert payload["healthy"] is False
    assert payload["components"]["minio"]["healthy"] is False


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (health)
# - [ ] summary is concise and imperative
