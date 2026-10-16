"""Focused regression coverage for digest and daily-diff boundary behavior."""

import asyncio
from datetime import UTC, datetime

import pytest

from etl import daily_diff_flow, digest_flow


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"headline": "Manager update"}, "Manager update"),
        ({"news_count": 2}, "2 manager-linked news items"),
        ({"threshold_crossed": "5%"}, "Threshold crossed: 5%"),
        ({}, "Important alert event"),
    ],
)
def test_payload_summary_uses_the_most_specific_available_value(payload, expected):
    assert digest_flow._payload_summary(payload) == expected


def test_coerce_utc_normalizes_naive_and_offset_datetimes():
    naive = datetime(2026, 5, 9, 12, 0)
    offset = datetime(2026, 5, 9, 7, 0, tzinfo=UTC)

    assert digest_flow._coerce_utc(naive) == naive.replace(tzinfo=UTC)
    assert digest_flow._coerce_utc(offset) == datetime(2026, 5, 9, 7, 0, tzinfo=UTC)


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


def test_digest_flow_rejects_nonpositive_lookback_before_opening_database():
    with pytest.raises(ValueError, match="lookback_hours must be greater than 0"):
        asyncio.run(digest_flow.digest_flow.fn(lookback_hours=0))


@pytest.mark.parametrize(
    ("value_prev", "value_curr", "expected_value", "expected_type"),
    [
        (10, 25, 15.0, "buy"),
        (None, -25, 25.0, "sell"),
        (None, None, None, "custom"),
    ],
)
def test_large_delta_event_preserves_amount_and_normalizes_known_types(
    value_prev, value_curr, expected_value, expected_type
):
    event = daily_diff_flow._large_delta_event_from_diff(
        {
            "manager_id": 7,
            "report_date": "2026-05-09",
            "cusip": "012345678",
            "delta_type": (
                "ADD" if expected_type == "buy" else "EXIT" if expected_type == "sell" else "CUSTOM"
            ),
            "value_prev": value_prev,
            "value_curr": value_curr,
        }
    )

    assert event.event_type == "large_delta"
    assert event.payload["delta_type"] == expected_type
    assert event.payload["value_usd"] == expected_value
