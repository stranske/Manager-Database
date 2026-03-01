import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from diff_holdings import _fetch_latest_sets, diff_holdings


def _setup_canonical_db() -> sqlite3.Connection:
    """Create an in-memory SQLite DB with the canonical managers/filings/holdings schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT UNIQUE)"
    )
    conn.execute(
        "CREATE TABLE filings ("
        "filing_id INTEGER PRIMARY KEY, manager_id INTEGER, "
        "type TEXT, filed_date TEXT, source TEXT, raw_key TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings ("
        "holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, "
        "shares INTEGER, value_usd REAL)"
    )
    # Seed a manager and two filings with overlapping holdings.
    conn.execute("INSERT INTO managers(manager_id, name, cik) VALUES (1, 'TestFund', '0000000000')")
    conn.executemany(
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) VALUES (?,?,?,?,?)",
        [
            (101, 1, "13F-HR", "2024-04-01", "edgar"),
            (102, 1, "13F-HR", "2024-01-01", "edgar"),
        ],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (?,?,?,?,?)",
        [
            # Current filing (101): AAA increased, CCC added, EEE decreased
            (101, "AAA", "CorpA", 120, 1200),
            (101, "CCC", "CorpC", 40, 400),
            (101, "EEE", "CorpE", 8, 80),
            # Prior filing (102): AAA baseline, BBB will exit, EEE baseline
            (102, "AAA", "CorpA", 100, 1000),
            (102, "BBB", "CorpB", 30, 300),
            (102, "EEE", "CorpE", 10, 100),
        ],
    )
    return conn


def test_diff_holdings_returns_structured_four_delta_types():
    """All four delta types must appear with correct values."""
    conn = _setup_canonical_db()
    rows = diff_holdings(1, conn)
    conn.close()

    assert rows == [
        {
            "cusip": "AAA",
            "name_of_issuer": "CorpA",
            "delta_type": "INCREASE",
            "shares_prev": 100,
            "shares_curr": 120,
            "value_prev": 1000,
            "value_curr": 1200,
        },
        {
            "cusip": "BBB",
            "name_of_issuer": "CorpB",
            "delta_type": "EXIT",
            "shares_prev": 30,
            "shares_curr": None,
            "value_prev": 300,
            "value_curr": None,
        },
        {
            "cusip": "CCC",
            "name_of_issuer": "CorpC",
            "delta_type": "ADD",
            "shares_prev": None,
            "shares_curr": 40,
            "value_prev": None,
            "value_curr": 400,
        },
        {
            "cusip": "EEE",
            "name_of_issuer": "CorpE",
            "delta_type": "DECREASE",
            "shares_prev": 10,
            "shares_curr": 8,
            "value_prev": 100,
            "value_curr": 80,
        },
    ]


def test_diff_holdings_accepts_cik_lookup():
    """CIK string should resolve to the same results as integer manager_id."""
    conn = _setup_canonical_db()
    by_cik = diff_holdings("0000000000", conn)
    by_id = diff_holdings(1, conn)
    conn.close()
    assert by_cik == by_id


def test_diff_holdings_accepts_numeric_string_manager_id():
    """A digit-only string should be treated as a manager_id."""
    conn = _setup_canonical_db()
    rows = diff_holdings("1", conn)
    conn.close()
    assert len(rows) == 4  # Same 4 delta types as above


def test_diff_requires_two_filings():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT)")
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, "
        "type TEXT, filed_date TEXT, source TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings (holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, shares INTEGER, value_usd REAL)"
    )
    conn.execute("INSERT INTO managers VALUES (1, 'Test', '0000000000')")
    conn.execute("INSERT INTO filings VALUES (101, 1, '13F-HR', '2024-04-01', 'edgar')")
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, shares, value_usd) VALUES (101, 'AAA', 120, 1200)"
    )

    with pytest.raises(SystemExit):
        diff_holdings(1, conn)
    conn.close()


def test_diff_requires_existing_manager():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT)")
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, "
        "type TEXT, filed_date TEXT, source TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings (holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, shares INTEGER, value_usd REAL)"
    )
    with pytest.raises(SystemExit):
        diff_holdings("9999999999", conn)
    conn.close()


def test_fetch_latest_sets_only_returns_top_two_dates():
    """Even with 3+ filing dates, only the latest two should be returned."""
    conn = _setup_canonical_db()
    # Add an older third filing.
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) "
        "VALUES (103, 1, '13F-HR', '2023-10-01', 'edgar')"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (103, 'DDD', 'CorpD', 1, 10)"
    )

    latest, prior = _fetch_latest_sets(1, conn)
    conn.close()

    # DDD is only in the oldest filing — must NOT appear.
    assert "DDD" not in latest
    assert "DDD" not in prior
    # AAA should be in both.
    assert "AAA" in latest and "AAA" in prior


def test_fetch_latest_sets_uses_postgres_placeholders():
    """Non-sqlite3 connections should produce %s placeholders."""

    class FakePostgresConn:
        def __init__(self):
            self.sql = ""
            self.params: tuple[int, ...] | None = None

        def execute(self, sql, params):
            self.sql = sql
            self.params = params
            return iter(
                [
                    ("2024-04-01", "AAA", 110, 1100, "CorpA"),
                    ("2024-01-01", "AAA", 100, 1000, "CorpA"),
                ]
            )

    conn = FakePostgresConn()
    _fetch_latest_sets(7, conn)
    assert "%s" in conn.sql
    assert "?" not in conn.sql
    assert conn.params == (7,)
