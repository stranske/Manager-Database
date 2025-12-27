import sys
import time
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import app


def test_health_db_ok(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    client = TestClient(app)
    resp = client.get("/health/db")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["healthy"] is True
    assert payload["latency_ms"] >= 0


def test_health_db_unreachable(tmp_path, monkeypatch):
    bad_path = tmp_path / "missing" / "dev.db"
    monkeypatch.setenv("DB_PATH", str(bad_path))
    client = TestClient(app)
    resp = client.get("/health/db")
    assert resp.status_code == 503
    payload = resp.json()
    assert payload["healthy"] is False
    assert payload["latency_ms"] >= 0


def test_health_db_timeout(monkeypatch):
    def slow_ping() -> None:
        # Sleep long enough to trip the timeout without touching the DB.
        time.sleep(0.2)

    monkeypatch.setenv("DB_HEALTH_TIMEOUT_S", "0.05")
    monkeypatch.setattr("api.chat._ping_db", slow_ping)
    client = TestClient(app)
    start = time.perf_counter()
    resp = client.get("/health/db")
    elapsed = time.perf_counter() - start
    assert elapsed < 0.5
    assert resp.status_code == 503
