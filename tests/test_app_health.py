import json
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest

from api import chat
from api.chat import health_app, health_live, health_livez, healthz


def _configure_health_env(monkeypatch, tmp_path):
    # Keep health checks local and fast for unit tests.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))
    monkeypatch.setenv("HEALTH_SUMMARY_TIMEOUT_S", "0.2")
    monkeypatch.setattr(chat, "_ping_minio", lambda _timeout_seconds: None)


def _shutdown_health_executor():
    # Avoid lingering worker threads after async health checks.
    chat._HEALTH_EXECUTOR.shutdown(wait=False, cancel_futures=True)


@pytest.mark.asyncio
async def test_health_app_ok(tmp_path, monkeypatch):
    _configure_health_env(monkeypatch, tmp_path)
    resp = await health_app()
    _shutdown_health_executor()
    payload = json.loads(resp.body)
    assert resp.status_code == 200
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0
    assert payload["failed_checks"] == {}


def test_health_live_ok():
    payload = health_live()
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0


def test_healthz_ok():
    # Probe aliases should mirror the base liveness payload.
    payload = healthz()
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0


def test_health_livez_ok():
    payload = health_livez()
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0


@pytest.mark.asyncio
async def test_health_app_reports_failed_dependencies(tmp_path, monkeypatch):
    _configure_health_env(monkeypatch, tmp_path)

    def _raise_minio(_timeout_seconds):
        raise RuntimeError("minio down")

    monkeypatch.setattr(chat, "_ping_minio", _raise_minio)
    resp = await health_app()
    _shutdown_health_executor()
    payload = json.loads(resp.body)
    assert resp.status_code == 503
    assert payload["failed_checks"]["minio"] == "minio down"


@pytest.mark.parametrize("budget_s", ["0.1"])
@pytest.mark.asyncio
async def test_health_app_responds_within_budget(tmp_path, monkeypatch, budget_s):
    _configure_health_env(monkeypatch, tmp_path)
    monkeypatch.setenv("HEALTH_SUMMARY_TIMEOUT_S", budget_s)
    start = time.perf_counter()
    resp = await health_app()
    _shutdown_health_executor()
    elapsed = time.perf_counter() - start
    assert resp.status_code == 200
    assert elapsed < 0.2


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (health)
# - [ ] summary is concise and imperative
