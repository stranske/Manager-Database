import asyncio
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api import chat
from api.chat import health_ready, health_readyz


def _payload_from_response(response):
    # Decode JSONResponse bodies without spinning up an ASGI client.
    return json.loads(response.body)


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def perf_counter(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _install_health_clock(monkeypatch, fake_clock: _FakeClock) -> chat.HealthClock:
    # Keep readiness uptime deterministic without patching the time module.
    clock = chat.HealthClock(
        perf_counter=fake_clock.perf_counter,
        monotonic=fake_clock.monotonic,
        sleep=fake_clock.sleep,
    )
    monkeypatch.setattr(chat, "HEALTH_CLOCK", clock)
    monkeypatch.setattr(chat, "APP_START_TIME", clock.monotonic())
    return clock


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


def test_health_ready_uptime_uses_injected_clock(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    # Force SQLite so readiness stays fast and deterministic.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    fake_clock = _FakeClock()
    _install_health_clock(monkeypatch, fake_clock)
    fake_clock.advance(6.7)
    resp = asyncio.run(health_ready())
    payload = _payload_from_response(resp)
    assert payload["uptime_s"] == 6


def test_health_ready_db_unreachable(tmp_path, monkeypatch):
    bad_path = tmp_path / "missing" / "dev.db"
    # Ensure we do not attempt a Postgres connection during tests.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(bad_path))
    resp = asyncio.run(health_ready())
    assert resp.status_code == 503
    payload = _payload_from_response(resp)
    assert payload["healthy"] is False


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (health)
# - [ ] summary is concise and imperative


def test_health_readyz_ok(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    # Keep the readyz alias consistent with the main readiness behavior.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(health_readyz())
    assert resp.status_code == 200
    payload = _payload_from_response(resp)
    assert payload["healthy"] is True


def test_health_ready_timeout(monkeypatch):
    def timeout_ping(_timeout_seconds: float) -> None:
        raise TimeoutError("db timeout")

    # Avoid accidental Postgres connections if DB_URL is set in the environment.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_HEALTH_TIMEOUT_S", "0.05")
    monkeypatch.setattr("api.chat._HEALTH_RETRY_BACKOFFS", ())
    monkeypatch.setattr("api.chat._ping_db", timeout_ping)
    resp = asyncio.run(health_ready())
    assert resp.status_code == 503
    payload = _payload_from_response(resp)
    assert payload["healthy"] is False
