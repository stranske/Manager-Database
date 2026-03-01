import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from diff_holdings import _fetch_latest_sets, diff_holdings


def setup_db(tmp_path: Path):
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE holdings (cik TEXT, accession TEXT, filed DATE, nameOfIssuer TEXT, cusip TEXT, value INTEGER, sshPrnamt INTEGER)"
    )
    data = [
        ("0000000000", "a", "2024-01-01", "CorpA", "AAA", 1, 1),
        ("0000000000", "a", "2024-01-01", "CorpB", "BBB", 1, 1),
        ("0000000000", "b", "2024-04-01", "CorpA", "AAA", 1, 1),
        ("0000000000", "b", "2024-04-01", "CorpC", "CCC", 1, 1),
    ]
    conn.executemany("INSERT INTO holdings VALUES (?,?,?,?,?,?,?)", data)
    conn.commit()
    conn.close()
    return str(db_path)


def test_diff(tmp_path):
    db_path = setup_db(tmp_path)
    adds, exits = diff_holdings("0000000000", db_path)
    assert adds == {"CCC"}
    assert exits == {"BBB"}


def test_diff_requires_two_filings(tmp_path):
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE holdings (cik TEXT, accession TEXT, filed DATE, nameOfIssuer TEXT, cusip TEXT, value INTEGER, sshPrnamt INTEGER)"
    )
    # Only one filing
    conn.execute(
        "INSERT INTO holdings VALUES (?,?,?,?,?,?,?)",
        ("0000000000", "a", "2024-01-01", "CorpA", "AAA", 1, 1),
    )
    conn.commit()
    conn.close()
    with pytest.raises(SystemExit):
        diff_holdings("0000000000", str(db_path))


def test_diff_requires_existing_cik(tmp_path):
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE holdings (cik TEXT, accession TEXT, filed DATE, nameOfIssuer TEXT, cusip TEXT, value INTEGER, sshPrnamt INTEGER)"
    )
    # Leave the table empty to exercise the missing CIK path.
    conn.commit()
    conn.close()
    with pytest.raises(SystemExit):
        diff_holdings("0000000000", str(db_path))


def test_fetch_latest_sets_joins_manager_filings_sqlite():
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
            (103, 1, "2023-10-01"),
        ],
    )
    conn.executemany(
        "INSERT INTO holdings VALUES (?, ?, ?, ?)",
        [
            (101, "AAA", 110, 1100),
            (101, "CCC", 50, 500),
            (102, "AAA", 100, 1000),
            (102, "BBB", 20, 200),
            (103, "DDD", 1, 10),
        ],
    )

    latest, prior = _fetch_latest_sets(1, conn)

    assert latest == {
        "AAA": {"shares": 110, "value_usd": 1100},
        "CCC": {"shares": 50, "value_usd": 500},
    }
    assert prior == {
        "AAA": {"shares": 100, "value_usd": 1000},
        "BBB": {"shares": 20, "value_usd": 200},
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
