import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import diff_holdings as diff_holdings_module
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
    rows = diff_holdings(1, conn).deltas
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


@pytest.mark.golden
def test_diff_holdings_golden():
    """Golden gate: the exact delta list for a fixture exercising all four branches.

    Two filings per manager produce one of each ``delta_type``
    (INCREASE/EXIT/ADD/DECREASE). Asserting the exact list pins the
    classification logic in ``diff_holdings`` — e.g. swapping INCREASE/DECREASE
    in ``_compare_optional`` handling makes this test fail.
    """
    conn = _setup_canonical_db()
    rows = diff_holdings(1, conn).deltas
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
    by_cik = diff_holdings("0000000000", conn).deltas
    by_id = diff_holdings(1, conn).deltas
    conn.close()
    assert by_cik == by_id


def test_diff_holdings_accepts_numeric_string_manager_id():
    """A digit-only string should be treated as a manager_id."""
    conn = _setup_canonical_db()
    rows = diff_holdings("1", conn).deltas
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


def test_fetch_latest_sets_reconciles_13f_amendments_before_diffing(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT UNIQUE)"
    )
    conn.execute(
        "CREATE TABLE filings ("
        "filing_id INTEGER PRIMARY KEY, manager_id INTEGER, type TEXT, "
        "period_end TEXT, filed_date TEXT, source TEXT, raw_key TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings ("
        "holding_id INTEGER PRIMARY KEY AUTOINCREMENT, filing_id INTEGER, "
        "cusip TEXT, name_of_issuer TEXT, shares INTEGER, value_usd REAL)"
    )
    conn.execute("INSERT INTO managers(manager_id, name, cik) VALUES (1, 'TestFund', '0000000000')")
    conn.executemany(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            (101, 1, "13F-HR", "2024-03-31", "2024-04-15", "edgar"),
            (102, 1, "13F-HR/A", "2024-03-31", "2024-04-20", "edgar"),
            (201, 1, "13F-HR", "2023-12-31", "2024-01-15", "edgar"),
        ],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (101, "AAA", "CorpA", 100, 1000),
            (101, "BBB", "CorpB", 50, 500),
            (102, "AAA", "CorpA", 130, 1300),
            (102, "CCC", "CorpC", 10, 100),
            (201, "AAA", "CorpA", 90, 900),
            (201, "DDD", "CorpD", 5, 50),
        ],
    )

    rows = diff_holdings(1, conn).deltas

    assert rows == [
        {
            "cusip": "AAA",
            "name_of_issuer": "CorpA",
            "delta_type": "INCREASE",
            "shares_prev": 90,
            "shares_curr": 130,
            "value_prev": 900,
            "value_curr": 1300,
        },
        {
            "cusip": "CCC",
            "name_of_issuer": "CorpC",
            "delta_type": "ADD",
            "shares_prev": None,
            "shares_curr": 10,
            "value_prev": None,
            "value_curr": 100,
        },
        {
            "cusip": "DDD",
            "name_of_issuer": "CorpD",
            "delta_type": "EXIT",
            "shares_prev": 5,
            "shares_curr": None,
            "value_prev": 50,
            "value_curr": None,
        },
    ]

    def disable_supersede(_manager_id, _conn):
        return [("2024-03-31 amended", 102), ("2024-03-31 original", 101)]

    monkeypatch.setattr(diff_holdings_module, "_select_authoritative_filings", disable_supersede)
    broken_rows = diff_holdings(1, conn).deltas
    conn.close()

    assert broken_rows == [
        {
            "cusip": "AAA",
            "name_of_issuer": "CorpA",
            "delta_type": "INCREASE",
            "shares_prev": 100,
            "shares_curr": 130,
            "value_prev": 1000,
            "value_curr": 1300,
        },
        {
            "cusip": "BBB",
            "name_of_issuer": "CorpB",
            "delta_type": "EXIT",
            "shares_prev": 50,
            "shares_curr": None,
            "value_prev": 500,
            "value_curr": None,
        },
        {
            "cusip": "CCC",
            "name_of_issuer": "CorpC",
            "delta_type": "ADD",
            "shares_prev": None,
            "shares_curr": 10,
            "value_prev": None,
            "value_curr": 100,
        },
    ]


def test_fetch_latest_sets_uses_postgres_placeholders():
    """Non-sqlite3 connections should produce %s placeholders."""

    class FakeCursor:
        def __init__(self, rows):
            self.rows = rows

        def __iter__(self):
            return iter(self.rows)

        def fetchall(self):
            return self.rows

    class FakePostgresConn:
        def __init__(self):
            self.sql = ""
            self.params: tuple[int, ...] | None = None
            self.calls: list[tuple[str, tuple[int, ...]]] = []

        def execute(self, sql, params):
            self.sql = sql
            self.params = params
            self.calls.append((sql, params))
            if "information_schema.columns" in sql:
                return FakeCursor(
                    [(column,) for column in {"filing_id", "manager_id", "type", "filed_date"}]
                )
            if "FROM filings" in sql:
                return FakeCursor(
                    [
                        (101, "2024-04-01", "2024-04-01", "13F-HR"),
                        (102, "2024-01-01", "2024-01-01", "13F-HR"),
                    ]
                )
            return FakeCursor(
                [
                    ("AAA", 110, 1100, "CorpA"),
                    ("AAA", 100, 1000, "CorpA"),
                ]
            )

    conn = FakePostgresConn()
    _fetch_latest_sets(7, conn)
    assert all("?" not in sql for sql, _params in conn.calls)
    assert any("%s" in sql and params == (7,) for sql, params in conn.calls)


def test_diff_holdings_uses_default_connect_db_when_conn_missing(monkeypatch):
    """When conn=None, diff_holdings should defer to connect_db() defaults."""

    calls: list[object] = []

    class DummyConn:
        def close(self):
            calls.append("closed")

    dummy_conn = DummyConn()

    def fake_connect_db(db_path=None):
        calls.append(db_path)
        return dummy_conn

    monkeypatch.setattr(diff_holdings_module, "connect_db", fake_connect_db)
    monkeypatch.setattr(diff_holdings_module, "_resolve_manager_id", lambda _identifier, _conn: 1)
    monkeypatch.setattr(
        diff_holdings_module, "_fetch_latest_sets", lambda _manager_id, _conn: ({}, {})
    )

    rows = diff_holdings_module.diff_holdings("0000000000").deltas

    assert rows == []
    assert calls == [None, "closed"]
