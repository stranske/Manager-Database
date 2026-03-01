import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from diff_holdings import _fetch_latest_sets, diff_holdings


def _setup_manager_filing_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, cik TEXT)")
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, filed_date TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings (filing_id INTEGER, cusip TEXT, shares INTEGER, value_usd INTEGER)"
    )
    conn.execute("INSERT INTO managers VALUES (?, ?)", (1, "0000000000"))
    conn.executemany(
        "INSERT INTO filings VALUES (?, ?, ?)",
        [
            (101, 1, "2024-04-01"),
            (102, 1, "2024-01-01"),
        ],
    )
    conn.executemany(
        "INSERT INTO holdings VALUES (?, ?, ?, ?)",
        [
            (101, "AAA", 120, 1200),  # INCREASE vs prior.
            (101, "CCC", 40, 400),  # ADD.
            (101, "EEE", 8, 80),  # DECREASE vs prior.
            (102, "AAA", 100, 1000),
            (102, "BBB", 30, 300),  # EXIT.
            (102, "EEE", 10, 100),
        ],
    )
    return conn


def test_diff_holdings_returns_structured_four_delta_types_for_manager_id():
    conn = _setup_manager_filing_db()
    rows = diff_holdings(1, conn)
    conn.close()

    assert rows == [
        {
            "cusip": "AAA",
            "delta_type": "INCREASE",
            "shares_prev": 100,
            "shares_curr": 120,
            "value_prev": 1000,
            "value_curr": 1200,
        },
        {
            "cusip": "BBB",
            "delta_type": "EXIT",
            "shares_prev": 30,
            "shares_curr": None,
            "value_prev": 300,
            "value_curr": None,
        },
        {
            "cusip": "CCC",
            "delta_type": "ADD",
            "shares_prev": None,
            "shares_curr": 40,
            "value_prev": None,
            "value_curr": 400,
        },
        {
            "cusip": "EEE",
            "delta_type": "DECREASE",
            "shares_prev": 10,
            "shares_curr": 8,
            "value_prev": 100,
            "value_curr": 80,
        },
    ]


def test_diff_holdings_accepts_cik_lookup():
    conn = _setup_manager_filing_db()
    by_cik = diff_holdings("0000000000", conn)
    by_id = diff_holdings(1, conn)
    conn.close()

    assert by_cik == by_id


def test_diff_requires_two_filings():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, cik TEXT)")
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, filed_date TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings (filing_id INTEGER, cusip TEXT, shares INTEGER, value_usd INTEGER)"
    )
    conn.execute("INSERT INTO managers VALUES (?, ?)", (1, "0000000000"))
    conn.execute("INSERT INTO filings VALUES (?, ?, ?)", (101, 1, "2024-04-01"))
    conn.execute("INSERT INTO holdings VALUES (?, ?, ?, ?)", (101, "AAA", 120, 1200))

    with pytest.raises(SystemExit):
        diff_holdings(1, conn)
    conn.close()


def test_diff_requires_existing_manager():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, cik TEXT)")
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, filed_date TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings (filing_id INTEGER, cusip TEXT, shares INTEGER, value_usd INTEGER)"
    )
    with pytest.raises(SystemExit):
        diff_holdings("0000000000", conn)
    conn.close()


def test_fetch_latest_sets_joins_manager_filings_sqlite():
    conn = _setup_manager_filing_db()
    conn.execute("INSERT INTO filings VALUES (?, ?, ?)", (103, 1, "2023-10-01"))
    conn.execute("INSERT INTO holdings VALUES (?, ?, ?, ?)", (103, "DDD", 1, 10))

    latest, prior = _fetch_latest_sets(1, conn)

    assert latest == {
        "AAA": {"shares": 120, "value_usd": 1200},
        "CCC": {"shares": 40, "value_usd": 400},
        "EEE": {"shares": 8, "value_usd": 80},
    }
    assert prior == {
        "AAA": {"shares": 100, "value_usd": 1000},
        "BBB": {"shares": 30, "value_usd": 300},
        "EEE": {"shares": 10, "value_usd": 100},
    }
    conn.close()


def test_fetch_latest_sets_uses_postgres_placeholders():
    class FakePostgresConn:
        def __init__(self):
            self.sql = ""
            self.params: tuple[int] | None = None

        def execute(self, sql, params):
            self.sql = sql
            self.params = params
            return iter(
                [
                    ("2024-04-01", "AAA", 110, 1100),
                    ("2024-01-01", "AAA", 100, 1000),
                ]
            )

    conn = FakePostgresConn()
    _fetch_latest_sets(7, conn)
    assert "WHERE m.manager_id = %s" in conn.sql
    assert conn.params == (7,)
