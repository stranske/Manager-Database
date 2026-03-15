from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from alerts.channels import (
    DeliveryResult,
    EmailChannel,
    NotificationChannel,
    SlackChannel,
    StreamlitChannel,
    build_configured_channels,
)
from alerts.db import deserialize_json_array, deserialize_json_object, ensure_alert_tables
from alerts.dispatch import AlertDispatcher
from alerts.models import AlertEvent, AlertRule, FiredAlert


def _setup_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'Elliott')")
    ensure_alert_tables(conn)
    return conn


def _build_rule(*, channels: list[str]) -> AlertRule:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    return AlertRule(
        rule_id=1,
        name="Important alert",
        description=None,
        event_type="new_filing",
        condition_json={},
        channels=channels,
        enabled=True,
        manager_id=1,
        created_by="test",
        created_at=timestamp,
        updated_at=timestamp,
    )


class _StaticChannel(NotificationChannel):
    def __init__(self, result: DeliveryResult) -> None:
        self.channel_name = result.channel
        self._result = result

    async def deliver(self, alert: FiredAlert) -> DeliveryResult:
        _ = alert
        return self._result


@pytest.mark.asyncio
async def test_streamlit_channel_always_succeeds():
    result = await StreamlitChannel().deliver(
        FiredAlert(
            rule=_build_rule(channels=["streamlit"]),
            event=AlertEvent(event_type="new_filing", manager_id=1, payload={"type": "13F-HR"}),
            channels=["streamlit"],
        )
    )

    assert result == DeliveryResult(channel="streamlit", success=True)


@pytest.mark.asyncio
async def test_email_channel_smtp_delivery(monkeypatch):
    captured: dict[str, object] = {}

    def fake_send(message, **kwargs):
        captured["subject"] = message["Subject"]
        captured["to"] = message["To"]
        captured["kwargs"] = kwargs

    monkeypatch.setattr("alerts.channels._send_via_smtp", fake_send)

    channel = EmailChannel(
        sender="alerts@example.com",
        recipients=["ops@example.com"],
        smtp_host="smtp.example.com",
    )
    result = await channel.deliver(
        FiredAlert(
            rule=_build_rule(channels=["email"]),
            event=AlertEvent(
                event_type="new_filing",
                manager_id=1,
                payload={"type": "13F-HR", "manager_name": "Elliott"},
            ),
            channels=["email"],
        )
    )

    assert result.success is True
    assert captured["subject"] == "Important alert"
    assert captured["to"] == "ops@example.com"
    assert captured["kwargs"] == {
        "host": "smtp.example.com",
        "port": 587,
        "username": None,
        "password": None,
        "use_tls": True,
        "timeout_seconds": 10.0,
    }


@pytest.mark.asyncio
async def test_email_channel_skips_when_unconfigured():
    channel = EmailChannel(sender="", recipients=[])
    result = await channel.deliver(
        FiredAlert(
            rule=_build_rule(channels=["email"]),
            event=AlertEvent(event_type="new_filing", manager_id=1, payload={"type": "13F-HR"}),
            channels=["email"],
        )
    )

    assert result.success is True
    assert result.skipped is True
    assert "ALERT_EMAIL_FROM" in (result.error_message or "")


@pytest.mark.asyncio
async def test_slack_channel_posts_blocks(monkeypatch):
    calls: list[tuple[str, dict[str, object]]] = []

    class FakeResponse:
        status_code = 200

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            _ = (args, kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            _ = (exc_type, exc, tb)

        async def post(self, url, json):
            calls.append((url, json))
            return FakeResponse()

    monkeypatch.setattr("alerts.channels.httpx.AsyncClient", FakeAsyncClient)

    channel = SlackChannel(webhook_url="https://hooks.slack.test/services/abc")
    result = await channel.deliver(
        FiredAlert(
            rule=_build_rule(channels=["slack"]),
            event=AlertEvent(
                event_type="new_filing",
                manager_id=1,
                payload={"type": "13F-HR", "manager_name": "Elliott"},
            ),
            channels=["slack"],
        )
    )

    assert result.success is True
    assert calls[0][0] == "https://hooks.slack.test/services/abc"
    assert "blocks" in calls[0][1]


@pytest.mark.asyncio
async def test_dispatcher_records_successes_and_failures(tmp_path):
    conn = _setup_db(tmp_path / "alerts.db")
    try:
        dispatcher = AlertDispatcher(
            conn,
            {
                "streamlit": _StaticChannel(DeliveryResult("streamlit", success=True)),
                "slack": _StaticChannel(
                    DeliveryResult("slack", success=False, error_message="HTTP 500")
                ),
                "email": _StaticChannel(DeliveryResult("email", success=True, skipped=True)),
            },
        )
        fired = FiredAlert(
            rule=_build_rule(channels=["streamlit", "slack", "email"]),
            event=AlertEvent(
                event_type="new_filing",
                manager_id=1,
                payload={"type": "13F-HR", "manager_name": "Elliott"},
            ),
            channels=["streamlit", "slack", "email"],
        )

        alert_id = await dispatcher.dispatch_single(fired)
        row = conn.execute(
            "SELECT delivered_channels, delivery_errors FROM alert_history WHERE alert_id = ?",
            (alert_id,),
        ).fetchone()
    finally:
        conn.close()

    assert deserialize_json_array(row[0]) == ["streamlit"]
    assert deserialize_json_object(row[1]) == {"slack": "HTTP 500"}


def test_build_configured_channels_reads_env(monkeypatch):
    monkeypatch.setenv("ALERT_EMAIL_FROM", "alerts@example.com")
    monkeypatch.setenv("ALERT_EMAIL_TO", "ops@example.com")
    monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("ALERT_SLACK_WEBHOOK_URL", "https://hooks.slack.test/services/abc")

    channels = build_configured_channels()

    assert sorted(channels) == ["email", "slack", "streamlit"]
    assert isinstance(channels["email"], EmailChannel)
    assert isinstance(channels["slack"], SlackChannel)
    assert isinstance(channels["streamlit"], StreamlitChannel)
