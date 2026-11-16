"""Tests for Form-4 insider ingest + conviction net-direction annotation (#1461)."""

from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pytest

from adapters.insider import InsiderFetchError, net_direction_for_rows, normalize_form4_rows
from api import signals
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


def _transaction_date(days_before_today: int) -> str:
    """Return a stable rolling-window fixture date relative to the current day."""
    return (date.today() - timedelta(days=days_before_today)).isoformat()


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
            "txn_date": _transaction_date(3),
            "acquired_disposed": "A",
        },
        {
            "issuer_cik": "0000320193",
            "ticker": "AAPL",
            "insider_name": "Cook",
            "txn_code": "P",
            "shares": 50,
            "txn_date": _transaction_date(2),
            "acquired_disposed": "A",
        },
        {
            "issuer_cik": "0000320193",
            "ticker": "AAPL",
            "insider_name": "Cook",
            "txn_code": "S",
            "shares": 20,
            "txn_date": _transaction_date(1),
            "acquired_disposed": "D",
        },
    ]


def _fake_fetcher_raises(issuer: str, *, lookback_days: int = 90):
    raise RuntimeError(f"boom for {issuer}")


def _conn_two_issuers() -> sqlite3.Connection:
    conn = _conn()
    conn.execute("ALTER TABLE holdings ADD COLUMN knowledge_time TEXT")
    conn.execute("UPDATE holdings SET knowledge_time = '2026-01-02T00:00:00Z'")
    # Known before the 2026-01-15 cutoff, superseded after it: visible as-of.
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, resolved_ticker, superseded_at, knowledge_time) "
        "VALUES (10, '594918104', 'MSFT', '2026-02-01T00:00:00Z', '2026-01-02T00:00:00Z')"
    )
    # Learned after the cutoff: not visible as-of.
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, resolved_ticker, superseded_at, knowledge_time) "
        "VALUES (10, '67066G104', 'NVDA', NULL, '2026-02-01T00:00:00Z')"
    )
    conn.commit()
    return conn


def _stub_fetcher_ok_and_fail(issuer: str, *, lookback_days: int = 90):
    if issuer.upper() == "MSFT":
        raise RuntimeError(f"boom for {issuer}")
    return [
        *_fake_fetcher_ok(issuer, lookback_days=lookback_days),
        {
            "ticker": issuer,
            "insider_name": "Cook",
            "txn_code": "P",
            "shares": 10,
            "txn_date": _transaction_date(1),
            "acquired_disposed": "A",
        },
    ]


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


def test_insider_direction_includes_day_90_and_excludes_day_91(
    monkeypatch: pytest.MonkeyPatch,
):
    """The rolling cutoff includes day 90 and excludes older transactions."""
    reference_date = date(2030, 1, 15)

    class FixedDate(date):
        @classmethod
        def today(cls) -> FixedDate:
            return cls(reference_date.year, reference_date.month, reference_date.day)

    def boundary_fetcher(issuer: str, *, lookback_days: int = 90):
        assert issuer
        assert lookback_days == 90
        return [
            {
                "issuer_cik": "0000320193",
                "ticker": "AAPL",
                "insider_name": "Cook",
                "txn_code": "S",
                "shares": 10,
                "txn_date": (reference_date - timedelta(days=89)).isoformat(),
                "acquired_disposed": "D",
            },
            {
                "issuer_cik": "0000320193",
                "ticker": "AAPL",
                "insider_name": "Cook",
                "txn_code": "P",
                "shares": 100,
                "txn_date": (reference_date - timedelta(days=90)).isoformat(),
                "acquired_disposed": "A",
            },
            {
                "issuer_cik": "0000320193",
                "ticker": "AAPL",
                "insider_name": "Cook",
                "txn_code": "S",
                "shares": 1000,
                "txn_date": (reference_date - timedelta(days=91)).isoformat(),
                "acquired_disposed": "D",
            },
        ]

    monkeypatch.setattr(insider_flow, "date", FixedDate)
    conn = _conn()
    result = insider_flow.ingest_insider_for_manager(conn, 1, fetcher=boundary_fetcher)

    assert result["inserted"] == 3
    assert insider_flow.insider_net_direction_for_ticker(conn, "AAPL") == "net buy"


def test_annotate_conviction_rows_joins_direction():
    conn = _conn()
    insider_flow.ingest_insider_for_manager(conn, 1, fetcher=_fake_fetcher_ok)
    annotated = insider_flow.annotate_conviction_rows(
        conn,
        [{"cusip": "037833100", "ticker": "AAPL", "conviction_pct": 5.0}],
    )
    assert annotated[0]["insider_net_direction"] == "net buy"
    assert annotated[0]["conviction_pct"] == 5.0


def test_local_form4_command_persists_and_annotates(monkeypatch: pytest.MonkeyPatch, capsys):
    db_path = Path(tempfile.mkstemp(suffix=".db")[1])
    seed = _conn_two_issuers()
    disk = sqlite3.connect(db_path)
    seed.backup(disk)
    disk.close()
    seed.close()

    def _open_db(*args, **kwargs):
        return sqlite3.connect(db_path)

    monkeypatch.setattr(insider_flow, "connect_db", _open_db)
    conn = sqlite3.connect(db_path)
    real_ingest = insider_flow.ingest_insider_for_issuers

    fetched_issuers: list[list[str]] = []

    def _ingest_with_stub(conn, issuers, **kwargs):
        fetched_issuers.append(sorted(issuer["ticker"] for issuer in issuers))
        if kwargs.get("fetcher") is None:
            kwargs = {**kwargs, "fetcher": _stub_fetcher_ok_and_fail}
        return real_ingest(conn, issuers, **kwargs)

    monkeypatch.setattr(insider_flow, "ingest_insider_for_issuers", _ingest_with_stub)

    assert (
        insider_flow.main(
            ["--as-of", "2026-01-15", "--lookback-days", "90", "--json"],
        )
        == 0
    )
    summary = json.loads(capsys.readouterr().out)
    assert summary == {
        "fetch_errors": 1,
        "filings_scanned": None,
        "issuers_resolved": 2,
        "rows_inserted": 3,
        "skipped_transaction_codes": None,
        "unmatched_issuers": None,
    }
    assert fetched_issuers == [["AAPL", "MSFT"]]

    stored = conn.execute("SELECT COUNT(*) FROM insider_transactions").fetchone()[0]
    assert stored == 3

    rows = signals.query_conviction_scores(conn, 1, filing_id=10)
    aapl = next(row for row in rows if row.cusip == "037833100")
    assert aapl.insider_net_direction == "net buy"

    count_before = stored
    second = insider_flow.run_form4_ingest(
        conn,
        as_of=date(2026, 1, 15),
        lookback_days=90,
        fetcher=_stub_fetcher_ok_and_fail,
    )
    assert second["rows_inserted"] == 0
    assert second["fetch_errors"] == 1
    count_after = conn.execute("SELECT COUNT(*) FROM insider_transactions").fetchone()[0]
    assert count_after == count_before

    assert insider_flow.main(["--as-of", "2026-01-15", "--lookback-days", "90"]) == 0
    assert capsys.readouterr().out.splitlines() == [
        "issuers_resolved: 2",
        "filings_scanned: null",
        "rows_inserted: 0",
        "unmatched_issuers: null",
        "skipped_transaction_codes: null",
        "fetch_errors: 1",
    ]
    assert fetched_issuers[-1] == ["AAPL", "MSFT"]
    assert conn.execute("SELECT COUNT(*) FROM insider_transactions").fetchone()[0] == count_before


def test_rerun_with_missing_insider_name_inserts_no_duplicates():
    conn = _conn()

    def ownerless_fetcher(issuer: str, *, lookback_days: int = 90):
        return [
            {
                "issuer_cik": "0000320193",
                "ticker": "AAPL",
                "insider_name": None,
                "txn_code": "P",
                "shares": 25,
                "txn_date": _transaction_date(3),
                "acquired_disposed": "A",
            }
        ]

    first = insider_flow.run_form4_ingest(
        conn, as_of=date.today(), lookback_days=90, fetcher=ownerless_fetcher
    )
    second = insider_flow.run_form4_ingest(
        conn, as_of=date.today(), lookback_days=90, fetcher=ownerless_fetcher
    )
    assert first["rows_inserted"] == 1
    assert second["rows_inserted"] == 0
    assert conn.execute("SELECT COUNT(*) FROM insider_transactions").fetchone()[0] == 1
