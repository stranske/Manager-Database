from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

import httpx
import pytest

from alerts.db import ensure_alert_tables, serialize_json
from etl import digest_flow


def _seed_digest_db(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    conn.execute("""CREATE TABLE filings (
            filing_id INTEGER PRIMARY KEY,
            manager_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            filed_date TEXT,
            source TEXT NOT NULL,
            url TEXT,
            created_at TEXT
        )""")
    conn.execute("""CREATE TABLE news_items (
            news_id INTEGER PRIMARY KEY,
            manager_id INTEGER,
            published_at TEXT NOT NULL,
            source TEXT NOT NULL,
            headline TEXT NOT NULL,
            url TEXT,
            body_snippet TEXT,
            topics TEXT DEFAULT '[]',
            confidence REAL
        )""")
    conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'Alpha Capital')")
    conn.execute(
        """INSERT INTO filings(filing_id, manager_id, type, filed_date, source, url, created_at)
           VALUES (10, 1, '13F-HR', '2026-05-08', 'edgar', 'https://filing.test/10',
                   '2026-05-08T20:00:00+00:00')"""
    )
    conn.execute("""INSERT INTO news_items(news_id, manager_id, published_at, source, headline, url)
           VALUES (20, 1, '2026-05-08T21:00:00+00:00', 'rss',
                   'Alpha Capital opens new office', 'https://news.test/20')""")
    ensure_alert_tables(conn)
    conn.execute("""INSERT INTO alert_rules(rule_id, name, event_type, condition_json, channels,
                                   enabled, manager_id, created_by)
           VALUES (30, 'Large filing alert', 'new_filing', '{}', '["streamlit"]', 1, 1, 'test')""")
    conn.execute(
        """INSERT INTO alert_history(alert_id, rule_id, fired_at, event_type, payload_json,
                                     delivered_channels, acknowledged)
           VALUES (40, 30, '2026-05-08T22:00:00+00:00', 'new_filing', ?, '[]', 0)""",
        (serialize_json({"summary": "New 13F received", "type": "13F-HR"}),),
    )
    conn.commit()


def test_build_digest_collects_recent_filings_news_and_alerts():
    conn = sqlite3.connect(":memory:")
    try:
        _seed_digest_db(conn)
        digest = digest_flow.build_digest.fn(
            conn,
            lookback_hours=24,
            now=datetime(2026, 5, 9, 12, 0, tzinfo=UTC),
        )
    finally:
        conn.close()

    assert digest.is_empty is False
    assert digest.filings[0].manager_name == "Alpha Capital"
    assert digest.filings[0].filing_type == "13F-HR"
    assert digest.news[0].headline == "Alpha Capital opens new office"
    assert digest.news[0].source == "rss"
    assert digest.alerts[0].rule_name == "Large filing alert"
    assert digest.alerts[0].summary == "New 13F received"

    text = digest_flow.render_digest_plain_text(digest)
    assert "Alpha Capital: 13F-HR filed 2026-05-08 via edgar" in text
    assert "Alpha Capital opens new office" in text
    assert "New 13F received" in text


def test_build_digest_empty_when_window_has_no_activity():
    conn = sqlite3.connect(":memory:")
    try:
        _seed_digest_db(conn)
        digest = digest_flow.build_digest.fn(
            conn,
            lookback_hours=24,
            now=datetime(2026, 5, 12, 0, 0, tzinfo=UTC),
        )
    finally:
        conn.close()

    assert digest.is_empty is True
    assert "No filings" in digest_flow.render_digest_plain_text(digest)
    assert "No filings" in digest_flow.render_digest_html(digest)


def test_build_digest_filters_created_at_without_date_truncation():
    conn = sqlite3.connect(":memory:")
    try:
        _seed_digest_db(conn)
        conn.execute(
            """INSERT INTO filings(filing_id, manager_id, type, filed_date, source, url, created_at)
               VALUES (11, 99, '13D', '2026-05-08', 'edgar', NULL,
                       '2026-05-08T00:30:00+00:00')"""
        )
        digest = digest_flow.build_digest.fn(
            conn,
            lookback_hours=24,
            now=datetime(2026, 5, 9, 12, 0, tzinfo=UTC),
        )
    finally:
        conn.close()

    assert [filing.filing_type for filing in digest.filings] == ["13F-HR"]


@pytest.mark.asyncio
async def test_digest_flow_dry_run_skips_delivery_without_credentials(monkeypatch, tmp_path):
    db_path = tmp_path / "digest.db"
    conn = sqlite3.connect(db_path)
    try:
        _seed_digest_db(conn)
    finally:
        conn.close()

    monkeypatch.delenv("ALERT_EMAIL_FROM", raising=False)
    monkeypatch.delenv("ALERT_EMAIL_TO", raising=False)
    output_path = tmp_path / "digest.txt"

    result = await digest_flow.digest_flow.fn(
        db_path=str(db_path),
        lookback_hours=24,
        dry_run=True,
        output_path=str(output_path),
        now=datetime(2026, 5, 9, 0, 0, tzinfo=UTC),
    )

    assert result["filings"] == 1
    assert result["news"] == 1
    assert result["alerts"] == 1
    assert result["delivery"] == {
        "channel": "email",
        "success": True,
        "skipped": True,
        "error_message": "dry-run",
    }
    assert "Alpha Capital" in output_path.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_sendgrid_http_errors_return_failed_delivery(monkeypatch):
    digest = digest_flow.DigestDocument(
        generated_at=datetime(2026, 5, 9, 0, 0, tzinfo=UTC),
        lookback_hours=24,
    )

    class FailingClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, *args, **kwargs):
            raise httpx.ConnectError("offline")

    monkeypatch.setenv("DIGEST_EMAIL_FROM", "alerts@example.test")
    monkeypatch.setenv("DIGEST_EMAIL_TO", "user@example.test")
    monkeypatch.setenv("DIGEST_EMAIL_PROVIDER", "sendgrid")
    monkeypatch.setenv("SENDGRID_API_KEY", "token")
    monkeypatch.setattr(digest_flow.httpx, "AsyncClient", FailingClient)

    result = await digest_flow.deliver_digest_email(digest)

    assert result.success is False
    assert result.error_message == "SendGrid digest delivery failed: offline"
