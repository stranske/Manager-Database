from __future__ import annotations

import asyncio
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest

from adapters import base, edgar
from alerts.channels import SlackChannel, resolve_slack_webhook_url

ROOT = Path(__file__).resolve().parents[1]


def _source(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_connect_db_and_alembic_share_default_sqlite_path(monkeypatch):
    captured: dict[str, object] = {}

    class DummyConnection:
        pass

    def fake_connect(path, **kwargs):
        captured["path"] = path
        captured["kwargs"] = kwargs
        return DummyConnection()

    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("DB_PATH", raising=False)
    monkeypatch.setattr(sqlite3, "connect", fake_connect)

    assert base.connect_db(connect_timeout=2, retries=0).__class__ is DummyConnection
    assert captured == {"path": base.DEFAULT_SQLITE_DB_PATH, "kwargs": {"timeout": 2}}
    assert base.DEFAULT_SQLITE_DB_PATH == "manager_database.db"
    assert "sqlite:///./{DEFAULT_SQLITE_DB_PATH}" in _source("alembic/env.py")


def test_migration_postgres_ddl_is_idempotent():
    canonical = _source("alembic/versions/001_canonical_schema.py")
    activism = _source("alembic/versions/006_activism_events.py")

    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS monthly_usage AS" in canonical
    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_report AS" in canonical
    assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_activism_events_unique_base" in activism
    assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_activism_events_unique_threshold" in activism


def test_env_defaults_are_shared_for_region_slack_and_timezone(monkeypatch):
    monkeypatch.delenv("ALERT_SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/legacy")

    assert resolve_slack_webhook_url() == "https://hooks.slack.test/legacy"
    assert SlackChannel().webhook_url == "https://hooks.slack.test/legacy"

    ingest_source = _source("etl/ingest_flow.py")
    activism_source = _source("etl/activism_flow.py")
    assert 'region_name=os.getenv("MINIO_REGION", "us-east-1")' in ingest_source
    assert 'os.getenv("ACTIVISM_FLOW_TIMEZONE", os.getenv("TZ", "UTC"))' in activism_source


def test_slack_webhook_falls_back_when_alert_specific_value_is_blank(monkeypatch):
    monkeypatch.setenv("ALERT_SLACK_WEBHOOK_URL", "   ")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", " https://hooks.slack.test/legacy ")

    assert resolve_slack_webhook_url() == "https://hooks.slack.test/legacy"
    assert SlackChannel().webhook_url == "https://hooks.slack.test/legacy"


@pytest.mark.asyncio
async def test_edgar_request_spacing_lock_prevents_overlapping_sleeps(monkeypatch):
    sleep_calls: list[float] = []
    active_sleeps = 0
    max_active_sleeps = 0
    real_sleep = asyncio.sleep

    @asynccontextmanager
    async def fake_tracked_call(source, url):
        _ = (source, url)

        def log(response):
            _ = response

        yield log

    class DummyClient:
        async def get(self, url, headers=None, params=None):
            _ = (headers, params)
            return httpx.Response(200, request=httpx.Request("GET", url), text="ok")

    async def fake_sleep(delay):
        nonlocal active_sleeps, max_active_sleeps
        sleep_calls.append(delay)
        active_sleeps += 1
        max_active_sleeps = max(max_active_sleeps, active_sleeps)
        await real_sleep(0)
        active_sleeps -= 1

    monkeypatch.setattr(edgar, "tracked_call", fake_tracked_call)
    monkeypatch.setattr(edgar.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(edgar, "EDGAR_MIN_REQUEST_INTERVAL", 0.5)
    monkeypatch.setattr(edgar, "_last_edgar_request_at", 100.0)
    monkeypatch.setattr(edgar, "_edgar_request_lock", asyncio.Lock())

    times = iter([100.0, 100.5, 100.5, 101.0])
    monkeypatch.setattr(edgar, "monotonic", lambda: next(times))

    await asyncio.gather(
        edgar._request_with_retry(DummyClient(), "https://example.test/a", {}, source="test"),
        edgar._request_with_retry(DummyClient(), "https://example.test/b", {}, source="test"),
    )

    assert sleep_calls == [0.5, 0.5]
    assert max_active_sleeps == 1
