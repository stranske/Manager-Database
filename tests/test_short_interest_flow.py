"""Tests for #1470 short-interest context: stubbed, finite, and non-disruptive."""

from __future__ import annotations

import sqlite3

import pytest

from adapters.short_interest import (
    DEFAULT_FINRA_SHORT_INTEREST_URL,
    ShortInterestFetchError,
    normalize_short_interest_row,
    short_interest_url,
)
from etl import short_interest_flow


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, filed_date TEXT);
        CREATE TABLE holdings (filing_id INTEGER, cusip TEXT, resolved_ticker TEXT, superseded_at TEXT);
        CREATE TABLE conviction_scores (score_id INTEGER PRIMARY KEY, manager_id INTEGER, filing_id INTEGER, cusip TEXT, conviction_pct REAL);
        INSERT INTO managers VALUES (1, 'Alpha');
        INSERT INTO filings VALUES (10, 1, '2026-07-01');
        INSERT INTO holdings VALUES (10, '037833100', 'AAPL', NULL);
        INSERT INTO conviction_scores VALUES (1, 1, 10, '037833100', 5.0);
        """)
    return conn


def _fetch_ok(ticker: str):
    assert ticker == "AAPL"
    return [
        {
            "symbol": ticker,
            "shortInterest": "250",
            "floatShares": "1000",
            "settlementDate": "2026-07-15",
        }
    ]


def _fetch_error(_: str):
    raise RuntimeError("rate limited")


def test_ingest_stubbed_short_interest_and_annotation():
    conn = _conn()
    result = short_interest_flow.ingest_short_interest_for_manager(conn, 1, fetcher=_fetch_ok)
    assert result == {"inserted": 1, "errors": [], "issuer_count": 1}
    annotation = short_interest_flow.short_interest_annotation(conn, "AAPL", cusip="037833100")
    assert annotation is not None
    assert annotation["short_interest_pct"] == pytest.approx(25.0)
    assert annotation["short_interest_report_date"] == "2026-07-15"


def test_fetch_failure_preserves_conviction_rows_and_returns_no_annotation():
    conn = _conn()
    before = conn.execute("SELECT COUNT(*) FROM conviction_scores").fetchone()[0]
    result = short_interest_flow.ingest_short_interest_for_manager(conn, 1, fetcher=_fetch_error)
    after = conn.execute("SELECT COUNT(*) FROM conviction_scores").fetchone()[0]
    assert before == after == 1
    assert result["inserted"] == 0 and result["errors"]
    assert short_interest_flow.short_interest_annotation(conn, "AAPL") is None


def test_deliberate_break_raises_when_graceful_guard_is_disabled():
    with pytest.raises(ShortInterestFetchError):
        short_interest_flow.ingest_short_interest_for_manager(
            _conn(), 1, fetcher=_fetch_error, raise_on_fetch_error=True
        )


def test_repeated_ingest_upserts_one_row_with_latest_values():
    conn = _conn()
    short_interest_flow.ingest_short_interest_for_manager(conn, 1, fetcher=_fetch_ok)

    def _fetch_updated(ticker: str):
        return [
            {
                "symbol": ticker,
                "shortInterest": "400",
                "floatShares": "1000",
                "settlementDate": "2026-07-15",
            }
        ]

    result = short_interest_flow.ingest_short_interest_for_manager(conn, 1, fetcher=_fetch_updated)
    assert result["errors"] == []
    rows = conn.execute(
        "SELECT short_interest, short_interest_pct FROM short_interest "
        "WHERE ticker = 'AAPL' AND report_date = '2026-07-15' AND source = 'finra'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == pytest.approx(400.0)
    assert rows[0][1] == pytest.approx(40.0)


def test_normalization_keeps_zero_values_and_rejects_unusable_dates():
    zero_row = normalize_short_interest_row(
        {
            "symbol": "AAPL",
            "short_interest": 0,
            "short_interest_pct": 0,
            "report_date": "2026-07-15",
        }
    )
    assert zero_row is not None
    assert zero_row["short_interest"] == 0.0
    assert zero_row["short_interest_pct"] == 0.0

    assert normalize_short_interest_row({"symbol": "AAPL", "shortInterest": "250"}) is None
    assert (
        normalize_short_interest_row(
            {"symbol": "AAPL", "shortInterest": "250", "report_date": "2026-99-99"}
        )
        is None
    )


def test_blank_feed_url_override_falls_back_to_the_default(monkeypatch):
    monkeypatch.setenv("FINRA_SHORT_INTEREST_URL", "")
    assert short_interest_url() == DEFAULT_FINRA_SHORT_INTEREST_URL
    monkeypatch.setenv("FINRA_SHORT_INTEREST_URL", "https://feeds.test/short-interest")
    assert short_interest_url() == "https://feeds.test/short-interest"


def test_annotation_prefers_cusip_over_a_reassigned_ticker():
    conn = _conn()
    short_interest_flow.ensure_short_interest_table(conn)
    short_interest_flow.upsert_short_interest(
        conn,
        [
            {
                "ticker": "AAPL",
                "cusip": "037833100",
                "short_interest": 250.0,
                "float_shares": 1000.0,
                "short_interest_pct": 25.0,
                "report_date": "2026-07-15",
                "source": "finra",
            },
            {
                "ticker": "AAPL",
                "cusip": "999999999",
                "short_interest": 900.0,
                "float_shares": 1000.0,
                "short_interest_pct": 90.0,
                "report_date": "2026-07-31",
                "source": "exchange",
            },
        ],
    )
    annotation = short_interest_flow.short_interest_annotation(conn, "AAPL", cusip="037833100")
    assert annotation is not None
    assert annotation["short_interest_pct"] == pytest.approx(25.0)
    assert annotation["short_interest_report_date"] == "2026-07-15"


def test_annotation_does_not_initialize_schema():
    conn = _conn()
    with pytest.raises(sqlite3.OperationalError):
        short_interest_flow.short_interest_annotation(conn, "AAPL", cusip="037833100")
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'short_interest'"
        ).fetchone()[0]
        == 0
    )
