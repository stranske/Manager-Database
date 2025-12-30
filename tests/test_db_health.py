import asyncio
import sys
import time
from pathlib import Path

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import app


async def _get_db_health():
    # Use the ASGI transport to avoid hanging threadpool-based clients.
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/health/db")
    finally:
        await app.router.shutdown()


def test_health_db_ok(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_db_health())
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["healthy"] is True
    assert payload["latency_ms"] >= 0


def test_health_db_unreachable(tmp_path, monkeypatch):
    bad_path = tmp_path / "missing" / "dev.db"
    monkeypatch.setenv("DB_PATH", str(bad_path))
    resp = asyncio.run(_get_db_health())
    assert resp.status_code == 503
    payload = resp.json()
    assert payload["healthy"] is False
    assert payload["latency_ms"] >= 0


def test_health_db_timeout(monkeypatch):
    def slow_ping(_timeout_seconds: float) -> None:
        # Sleep long enough to trip the timeout without touching the DB.
        time.sleep(0.2)

    monkeypatch.setenv("DB_HEALTH_TIMEOUT_S", "0.05")
    monkeypatch.setattr("api.chat._ping_db", slow_ping)
    start = time.perf_counter()
    resp = asyncio.run(_get_db_health())
    elapsed = time.perf_counter() - start
    assert elapsed < 0.5
    assert resp.status_code == 503


def test_health_db_timeout_cap(monkeypatch):
    # Ensure the helper never exceeds the 5s cap, even if env is larger.
    monkeypatch.setenv("DB_HEALTH_TIMEOUT_S", "10")
    from api.chat import _db_timeout_seconds

    assert _db_timeout_seconds() == 5.0
