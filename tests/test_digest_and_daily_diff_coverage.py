"""Focused regression coverage for digest and daily-diff boundary behavior."""

import asyncio
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from alerts.db import ensure_alert_tables, serialize_json
from etl import daily_diff_flow, digest_flow


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"summary": "Summary", "headline": "Headline", "news_count": 2}, "Summary"),
        ({"summary": None, "headline": "Headline", "type": "13F"}, "Headline"),
        ({"summary": "", "headline": [], "type": "13F", "filing_type": "13D"}, "13F"),
        ({"type": "", "filing_type": "13D", "delta_type": "buy"}, "13D"),
        ({"filing_type": None, "delta_type": "buy", "news_count": 2}, "buy"),
        ({"headline": "Manager update"}, "Manager update"),
        ({"news_count": 2, "threshold_crossed": "5%"}, "2 manager-linked news items"),
        ({"news_count": 0, "threshold_crossed": "5%"}, "Threshold crossed: 5%"),
        ({"threshold_crossed": "5%"}, "Threshold crossed: 5%"),
        ({"news_count": 0, "threshold_crossed": 0}, "Important alert event"),
        ({}, "Important alert event"),
    ],
)
def test_payload_summary_uses_the_most_specific_available_value(payload, expected):
    assert digest_flow._payload_summary(payload) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (datetime(2026, 5, 9, 12), datetime(2026, 5, 9, 12, tzinfo=UTC)),
        (datetime(2026, 5, 9, 12, tzinfo=UTC), datetime(2026, 5, 9, 12, tzinfo=UTC)),
        (
            datetime(2026, 5, 9, 1, 30, tzinfo=timezone(timedelta(hours=5, minutes=30))),
            datetime(2026, 5, 8, 20, tzinfo=UTC),
        ),
        (
            datetime(2026, 5, 8, 23, tzinfo=timezone(timedelta(hours=-4))),
            datetime(2026, 5, 9, 3, tzinfo=UTC),
        ),
    ],
)
def test_coerce_utc_normalizes_naive_and_offset_datetimes(value, expected):
    result = digest_flow._coerce_utc(value)

    assert result == expected
    assert result.tzinfo is UTC


@pytest.mark.parametrize("row_factory", [None, sqlite3.Row], ids=["tuple-rows", "mapping-rows"])
def test_digest_cutoff_is_inclusive_and_excludes_acknowledged_alerts(row_factory):
    with closing(sqlite3.connect(":memory:")) as conn:
        conn.row_factory = row_factory
        conn.executescript("""
            CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO managers VALUES (1, 'Alpha Capital');
            CREATE TABLE filings (
                filing_id INTEGER PRIMARY KEY, manager_id INTEGER, type TEXT,
                filed_date TEXT, created_at TEXT, source TEXT, url TEXT
            );
            CREATE TABLE news_items (
                news_id INTEGER PRIMARY KEY, manager_id INTEGER, published_at TEXT,
                source TEXT, headline TEXT, url TEXT
            );
        """)
        ensure_alert_tables(conn)
        conn.execute("""INSERT INTO alert_rules (rule_id, name, event_type, manager_id)
                        VALUES (1, 'Filing alert', 'new_filing', 1)""")
        # Two rows at the cutoff also exercise the descending ID tie-breaker.
        for row_id, timestamp in enumerate(
            (
                "2026-05-08T11:59:59+00:00",
                "2026-05-08T12:00:00+00:00",
                "2026-05-08T12:00:00+00:00",
                "2026-05-08T12:00:01+00:00",
            ),
            start=1,
        ):
            conn.execute(
                """INSERT INTO filings VALUES (?, 1, ?, '2026-05-07', ?, 'edgar', NULL)""",
                (row_id, f"Filing {row_id}", timestamp),
            )
            conn.execute(
                "INSERT INTO news_items VALUES (?, 1, ?, 'rss', ?, NULL)",
                (row_id, timestamp, f"Headline {row_id}"),
            )
            conn.execute(
                """INSERT INTO alert_history
                       (alert_id, rule_id, fired_at, event_type, payload_json)
                   VALUES (?, 1, ?, 'new_filing', ?)""",
                (row_id, timestamp, serialize_json({"summary": f"Alert {row_id}"})),
            )
        conn.execute("""INSERT INTO alert_history
                            (rule_id, fired_at, event_type, payload_json, acknowledged)
                        VALUES (1, '2026-05-09T12:00:00+00:00', 'new_filing',
                                '{"summary":"Acknowledged"}', 1)""")

        digest = digest_flow.build_digest.fn(
            conn, lookback_hours=24, now=datetime(2026, 5, 9, 12, tzinfo=UTC)
        )

    assert digest.is_empty is False
    assert [item.filing_type for item in digest.filings] == ["Filing 4", "Filing 3", "Filing 2"]
    assert [item.headline for item in digest.news] == ["Headline 4", "Headline 3", "Headline 2"]
    assert [item.summary for item in digest.alerts] == ["Alert 4", "Alert 3", "Alert 2"]


def test_digest_legacy_filings_use_the_cutoff_date_when_created_at_is_absent():
    with closing(sqlite3.connect(":memory:")) as conn:
        conn.executescript("""
            CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE filings (
                filing_id INTEGER PRIMARY KEY, manager_id INTEGER, type TEXT,
                filed_date TEXT, source TEXT, url TEXT
            );
            INSERT INTO filings VALUES
                (1, 7, '13F', '2026-05-07', 'edgar', NULL),
                (2, 7, '13D', '2026-05-08', 'edgar', NULL),
                (3, 7, '13G', '2026-05-09', 'edgar', NULL);
        """)

        digest = digest_flow.build_digest.fn(
            conn, lookback_hours=24, now=datetime(2026, 5, 9, 12, tzinfo=UTC)
        )

    assert [(item.filing_type, item.filed_date, item.manager_name) for item in digest.filings] == [
        ("13G", "2026-05-09", "Manager 7"),
        ("13D", "2026-05-08", "Manager 7"),
    ]
    assert digest.news == []
    assert digest.alerts == []


def test_digest_email_configuration_skips_missing_sendgrid_credentials(monkeypatch):
    digest = digest_flow.DigestDocument(
        generated_at=datetime(2026, 5, 9, tzinfo=UTC), lookback_hours=24
    )
    monkeypatch.setenv("DIGEST_EMAIL_FROM", "alerts@example.test")
    monkeypatch.setenv("DIGEST_EMAIL_TO", "first@example.test, second@example.test")
    monkeypatch.setenv("DIGEST_EMAIL_PROVIDER", "sendgrid")
    monkeypatch.delenv("SENDGRID_API_KEY", raising=False)

    result = asyncio.run(digest_flow.deliver_digest_email(digest))

    assert result.skipped is True
    assert result.error_message == "SENDGRID_API_KEY is not configured"


@pytest.mark.parametrize(
    ("environment", "expected_message"),
    [
        ({}, "DIGEST_EMAIL_FROM is not configured"),
        ({"DIGEST_EMAIL_FROM": "alerts@example.test"}, "DIGEST_EMAIL_TO is not configured"),
        (
            {
                "DIGEST_EMAIL_FROM": "alerts@example.test",
                "DIGEST_EMAIL_TO": "reader@example.test",
            },
            "SMTP_HOST is not configured",
        ),
    ],
)
def test_digest_email_skips_incomplete_smtp_configuration(
    monkeypatch, environment, expected_message
):
    for name in (
        "DIGEST_EMAIL_FROM",
        "DIGEST_EMAIL_TO",
        "DIGEST_EMAIL_PROVIDER",
        "ALERT_EMAIL_FROM",
        "ALERT_EMAIL_TO",
        "ALERT_EMAIL_PROVIDER",
        "SENDGRID_API_KEY",
        "SMTP_HOST",
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    digest = digest_flow.DigestDocument(
        generated_at=datetime(2026, 5, 9, tzinfo=UTC), lookback_hours=24
    )
    result = asyncio.run(digest_flow.deliver_digest_email(digest))

    assert result.success is True
    assert result.skipped is True
    assert result.error_message == expected_message


def test_rendered_digest_escapes_html_and_preserves_plaintext():
    digest = digest_flow.DigestDocument(
        generated_at=datetime(2026, 5, 9, tzinfo=UTC),
        lookback_hours=24,
        filings=[
            digest_flow.DigestFiling(
                manager_name="A < B",
                filing_type="13F",
                filed_date="2026-05-09",
                source="SEC & EDGAR",
            )
        ],
    )

    assert "A < B" in digest_flow.render_digest_plain_text(digest)
    assert "A &lt; B" in digest_flow.render_digest_html(digest)
    assert "SEC &amp; EDGAR" in digest_flow.render_digest_html(digest)


@pytest.mark.parametrize("lookback_hours", [0, -1])
def test_digest_flow_rejects_nonpositive_lookback_before_opening_database(
    monkeypatch, lookback_hours
):
    connect = Mock(side_effect=AssertionError("invalid lookback must not open the database"))
    monkeypatch.setattr(digest_flow, "connect_db", connect)

    with pytest.raises(ValueError, match="lookback_hours must be greater than 0"):
        asyncio.run(digest_flow.digest_flow.fn(lookback_hours=lookback_hours))

    connect.assert_not_called()


@pytest.mark.parametrize(
    ("delta_type", "value_prev", "value_curr", "expected_value", "expected_type"),
    [
        ("add", None, -25, 25.0, "buy"),
        ("Increase", "10.5", "25.75", 15.25, "buy"),
        ("EXIT", -25, None, 25.0, "sell"),
        ("decrease", 25, 10, 15.0, "sell"),
        ("ADD", None, 0, 0.0, "buy"),
        ("EXIT", 0, None, 0.0, "sell"),
        ("DECREASE", 25, 0, 25.0, "sell"),
        ("INCREASE", 0, 25, 25.0, "buy"),
        ("UNCHANGED", 25, 25, 0.0, "unchanged"),
        ("CUSTOM", None, None, None, "custom"),
    ],
)
def test_large_delta_event_preserves_amount_and_normalizes_known_types(
    delta_type, value_prev, value_curr, expected_value, expected_type
):
    row = {
        "manager_id": 7,
        "report_date": "2026-05-09",
        "cusip": "012345678",
        "name_of_issuer": "Example issuer",
        "delta_type": delta_type,
        "shares_prev": 10,
        "shares_curr": 25,
        "value_prev": value_prev,
        "value_curr": value_curr,
    }
    original = row.copy()
    event = daily_diff_flow._large_delta_event_from_diff(row)

    assert row == original
    assert event.event_type == "large_delta"
    assert event.manager_id == 7
    assert event.payload == {
        **original,
        "raw_delta_type": delta_type.upper(),
        "delta_type": expected_type,
        "value_usd": expected_value,
    }
