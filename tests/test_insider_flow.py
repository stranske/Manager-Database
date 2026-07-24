"""Tests for Form-4 insider ingest + conviction net-direction annotation (#1461)."""

from __future__ import annotations

import sqlite3

import pytest

from adapters.insider import InsiderFetchError, net_direction_for_rows, normalize_form4_rows
from etl import insider_flow


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)
        """)
    conn.execute("""
        CREATE TABLE filings (
            filing_id INTEGER PRIMARY KEY,
            manager_id INTEGER,
            filed_date TEXT
        )
        """)
    conn.execute("""
        CREATE TABLE holdings (
            filing_id INTEGER,
            cusip TEXT,
            resolved_ticker TEXT,
            superseded_at TEXT
        )
        """)
    conn.execute("""
        CREATE TABLE conviction_scores (
            score_id INTEGER PRIMARY KEY,
            manager_id INTEGER,
            filing_id INTEGER,
            cusip TEXT,
            name_of_issuer TEXT,
            shares INTEGER,
            value_usd REAL,
            conviction_pct REAL,
            portfolio_weight REAL
        )
        """)
    conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'Alpha')")
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, filed_date) VALUES (10, 1, '2026-01-01')"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, resolved_ticker, superseded_at) "
        "VALUES (10, '037833100', 'AAPL', NULL)"
    )
    conn.execute(
        "INSERT INTO conviction_scores(manager_id, filing_id, cusip, name_of_issuer, "
        "shares, value_usd, conviction_pct, portfolio_weight) "
        "VALUES (1, 10, '037833100', 'Apple', 100, 1000, 5.0, 0.05)"
    )
    conn.commit()
    return conn


def _fake_fetcher_ok(issuer: str, *, lookback_days: int = 90):
    assert issuer
    assert lookback_days > 0
    return [
        {
            "issuer_cik": "0000320193",
            "ticker": "AAPL",
            "insider_name": "Cook",
            "txn_code": "P",
            "shares": 100,
            "txn_date": "2026-07-01",
            "acquired_disposed": "A",
        },
        {
            "issuer_cik": "0000320193",
            "ticker": "AAPL",
            "insider_name": "Cook",
            "txn_code": "P",
            "shares": 50,
            "txn_date": "2026-07-02",
            "acquired_disposed": "A",
        },
        {
            "issuer_cik": "0000320193",
            "ticker": "AAPL",
            "insider_name": "Cook",
            "txn_code": "S",
            "shares": 20,
            "txn_date": "2026-07-03",
            "acquired_disposed": "D",
        },
    ]


def _fake_fetcher_raises(issuer: str, *, lookback_days: int = 90):
    raise RuntimeError(f"boom for {issuer}")


def test_normalize_and_net_direction():
    rows = normalize_form4_rows(_fake_fetcher_ok("AAPL"))
    assert len(rows) == 3
    assert rows[0]["issuer_cik"] == "0000320193"
    assert net_direction_for_rows(rows) == "net buy"


def test_ingest_stubbed_edgartools_net_buy():
    conn = _conn()
    result = insider_flow.ingest_insider_for_manager(conn, 1, fetcher=_fake_fetcher_ok)
    assert result["inserted"] >= 3
    assert result["errors"] == []
    direction = insider_flow.insider_net_direction_for_ticker(conn, "AAPL")
    assert direction == "net buy"
    stored = conn.execute("SELECT COUNT(*) FROM insider_transactions").fetchone()[0]
    assert stored == 3


def test_robustness_fetch_error_does_not_drop_conviction():
    conn = _conn()
    before = conn.execute("SELECT COUNT(*) FROM conviction_scores").fetchone()[0]
    result = insider_flow.ingest_insider_for_manager(conn, 1, fetcher=_fake_fetcher_raises)
    after = conn.execute("SELECT COUNT(*) FROM conviction_scores").fetchone()[0]
    assert before == after == 1
    assert result["inserted"] == 0
    assert result["errors"]
    direction = insider_flow.insider_net_direction_for_ticker(conn, "AAPL")
    assert direction == "unknown"


def test_deliberate_break_raises_when_guard_disabled():
    conn = _conn()
    with pytest.raises(InsiderFetchError):
        insider_flow.ingest_insider_for_manager(
            conn,
            1,
            fetcher=_fake_fetcher_raises,
            raise_on_fetch_error=True,
        )


def test_annotate_conviction_rows_joins_direction():
    conn = _conn()
    insider_flow.ingest_insider_for_manager(conn, 1, fetcher=_fake_fetcher_ok)
    annotated = insider_flow.annotate_conviction_rows(
        conn,
        [{"cusip": "037833100", "ticker": "AAPL", "conviction_pct": 5.0}],
    )
    assert annotated[0]["insider_net_direction"] == "net buy"
    assert annotated[0]["conviction_pct"] == 5.0
