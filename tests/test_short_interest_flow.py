"""Tests for #1470 short-interest context: stubbed, finite, and non-disruptive."""

from __future__ import annotations

import sqlite3

import pytest

from adapters.short_interest import ShortInterestFetchError
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
