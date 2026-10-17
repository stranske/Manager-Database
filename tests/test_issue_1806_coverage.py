"""Focused regression coverage for the ranked #1806 coverage targets."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime

import pytest

from adapters import asic
from api import search, signals
from etl import digest_flow
from ui import daily_report


def test_digest_helpers_escape_and_split_recipients():
    digest = digest_flow.DigestDocument(
        generated_at=datetime(2026, 1, 1, tzinfo=UTC), lookback_hours=24
    )
    assert digest_flow._split_csv(" a@example.test, , b@example.test ") == [
        "a@example.test",
        "b@example.test",
    ]
    assert digest_flow._split_csv(None) == []
    message = digest_flow.build_digest_email_message(
        digest, sender="sender@example.test", recipients=["a@example.test", "b@example.test"]
    )
    assert message["To"] == "a@example.test, b@example.test"
    assert "No filings" in message.get_body(preferencelist=("plain",)).get_content()


@pytest.mark.asyncio
async def test_digest_delivery_configuration_guards(monkeypatch):
    digest = digest_flow.DigestDocument(datetime.now(UTC), 24)
    monkeypatch.delenv("DIGEST_EMAIL_FROM", raising=False)
    monkeypatch.delenv("ALERT_EMAIL_FROM", raising=False)
    assert (await digest_flow.deliver_digest_email(digest)).skipped
    monkeypatch.setenv("DIGEST_EMAIL_FROM", "sender@example.test")
    monkeypatch.delenv("DIGEST_EMAIL_TO", raising=False)
    monkeypatch.delenv("ALERT_EMAIL_TO", raising=False)
    assert "TO is not configured" in (await digest_flow.deliver_digest_email(digest)).error_message
    monkeypatch.setenv("DIGEST_EMAIL_TO", "recipient@example.test")
    monkeypatch.setenv("DIGEST_EMAIL_PROVIDER", "sendgrid")
    monkeypatch.delenv("SENDGRID_API_KEY", raising=False)
    assert "SENDGRID_API_KEY" in (await digest_flow.deliver_digest_email(digest)).error_message


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, []),
        (["1", "x", 2], [1, 2]),
        ("{1, 2, x}", [1, 2]),
        ('[1, "2", null]', [1, 2]),
        ("bad", []),
    ],
)
def test_signal_manager_id_parser_rejects_invalid_values(value, expected):
    assert signals._parse_manager_ids(value) == expected


def test_signal_date_helpers_and_empty_schema_guards():
    assert signals._to_date("2024-01-02T03:04:05Z").isoformat() == "2024-01-02"
    assert signals._to_date(datetime(2024, 1, 2, tzinfo=UTC)).isoformat() == "2024-01-02"
    conn = sqlite3.connect(":memory:")
    try:
        assert signals._resolve_latest_report_date(conn, "missing") is None
        assert signals._resolve_latest_manager_filing_id(conn, 1) is None
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("rank", "distance", "expected"),
    [(None, None, (0.0, 0.0)), (-1, -1, (0.0, 0.0)), (1, 2, (0.5, 0.0))],
)
def test_search_score_components_clamp_invalid_inputs(rank, distance, expected):
    assert search._normalize_rank(rank) == expected[0]
    assert search._vector_relevance(distance) == expected[1]
    assert search.universal_search(" ", object()) == []
    assert search.universal_search("query", object(), limit=0) == []


def test_search_activism_headline_has_safe_fallbacks():
    assert search._format_activism_filing_headline(None, "", None) == (
        "13D Filing: Unknown manager -> Unknown subject (n/a)"
    )
    assert "5.0%" in search._format_activism_filing_headline(" A ", " B ", 5)


def test_daily_report_formatters_cover_empty_and_directional_values():
    assert daily_report.parse_topics("['alpha', beta]") == ["alpha", "beta"]
    assert daily_report.topic_choices(daily_report.pd.DataFrame({"topics": ["beta,alpha"]})) == [
        "All topics",
        "alpha",
        "beta",
    ]
    assert "↑ +2" in daily_report.format_shares_delta(1, 3)
    assert "↓ -2" in daily_report.format_shares_delta(3, 1)
    assert "n/a" in daily_report.format_percent_change(0, 2)
    assert "+$2.00" in daily_report.format_value_delta(1, 3)
    assert "<x>" not in daily_report.headline_markdown("<x>", None)
    assert "Contrarian: BUY vs SELL" in daily_report.format_signal_badge("BUY", "SELL")


def test_asic_csv_resource_and_parse_paths():
    assert (
        asic._csv_resource_url(
            {"result": {"results": [{"resources": [{"format": "CSV", "url": "x"}]}]}}
        )
        == "x"
    )
    assert asic._csv_resource_url({"result": {"results": []}}) is None
    parsed = asyncio.run(asic.parse(b"name,value\nA,1\n"))
    assert parsed[0]["record_count"] == 1
    assert parsed[0]["status"] == "unsupported"
