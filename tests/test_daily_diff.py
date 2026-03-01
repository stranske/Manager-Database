import datetime as dt
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.daily_diff_flow import compute, daily_diff_flow


def setup_db(tmp_path: Path) -> str:
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT UNIQUE)"
    )
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, filed_date TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings (filing_id INTEGER, cusip TEXT, shares INTEGER, value_usd INTEGER)"
    )
    conn.execute("""CREATE TABLE daily_diffs (
            diff_id INTEGER PRIMARY KEY AUTOINCREMENT,
            manager_id INTEGER NOT NULL,
            report_date TEXT NOT NULL,
            cusip TEXT NOT NULL,
            name_of_issuer TEXT,
            delta_type TEXT NOT NULL,
            shares_prev INTEGER,
            shares_curr INTEGER,
            value_prev REAL,
            value_curr REAL
        )""")
    conn.executemany(
        "INSERT INTO managers(manager_id, name, cik) VALUES (?, ?, ?)",
        [
            (1, "Manager One", "0000000000"),
            (2, "Manager Two", "0000000001"),
        ],
    )
    conn.executemany(
        "INSERT INTO filings(filing_id, manager_id, filed_date) VALUES (?, ?, ?)",
        [
            (101, 1, "2024-01-01"),
            (102, 1, "2024-04-01"),
            (201, 2, "2024-01-01"),
            (202, 2, "2024-04-01"),
        ],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, shares, value_usd) VALUES (?, ?, ?, ?)",
        [
            (101, "AAA", 100, 1000),
            (101, "BBB", 30, 300),
            (101, "EEE", 10, 100),
            (102, "AAA", 120, 1200),
            (102, "CCC", 40, 400),
            (102, "EEE", 8, 80),
            (201, "XXX", 10, 100),
            (202, "XXX", 10, 100),
            (202, "YYY", 5, 50),
        ],
    )
    conn.commit()
    conn.close()
    return str(db_path)


def test_compute_processes_all_managers_and_writes_daily_diffs(tmp_path: Path):
    db_path = setup_db(tmp_path)
    compute.fn("2024-05-01", db_path)

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT manager_id, report_date, cusip, delta_type "
        "FROM daily_diffs ORDER BY manager_id, cusip"
    ).fetchall()
    conn.close()
    assert rows == [
        (1, "2024-05-01", "AAA", "INCREASE"),
        (1, "2024-05-01", "BBB", "EXIT"),
        (1, "2024-05-01", "CCC", "ADD"),
        (1, "2024-05-01", "EEE", "DECREASE"),
        (2, "2024-05-01", "YYY", "ADD"),
    ]


def test_compute_persists_all_four_delta_types_with_values(tmp_path: Path):
    db_path = setup_db(tmp_path)
    compute.fn("2024-05-01", db_path, ["0000000000"])

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT cusip, delta_type, shares_prev, shares_curr, value_prev, value_curr "
        "FROM daily_diffs WHERE manager_id = 1 ORDER BY cusip"
    ).fetchall()
    conn.close()
    assert rows == [
        ("AAA", "INCREASE", 100, 120, 1000.0, 1200.0),
        ("BBB", "EXIT", 30, None, 300.0, None),
        ("CCC", "ADD", None, 40, None, 400.0),
        ("EEE", "DECREASE", 10, 8, 100.0, 80.0),
    ]


def test_compute_is_idempotent_for_same_date(tmp_path: Path):
    db_path = setup_db(tmp_path)
    compute.fn("2024-05-01", db_path)
    compute.fn("2024-05-01", db_path)

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT manager_id, report_date, cusip, delta_type FROM daily_diffs ORDER BY manager_id, cusip"
    ).fetchall()
    conn.close()
    assert rows == [
        (1, "2024-05-01", "AAA", "INCREASE"),
        (1, "2024-05-01", "BBB", "EXIT"),
        (1, "2024-05-01", "CCC", "ADD"),
        (1, "2024-05-01", "EEE", "DECREASE"),
        (2, "2024-05-01", "YYY", "ADD"),
    ]


def test_daily_diff_flow_uses_env_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.setenv("CIK_LIST", "0000000000")

    class DateShim:
        @staticmethod
        def today() -> dt.date:
            return dt.date(2024, 5, 2)

    class DateTimeShim:
        date = DateShim
        timedelta = dt.timedelta

    monkeypatch.setattr("etl.daily_diff_flow.dt", DateTimeShim)
    monkeypatch.setattr("etl.daily_diff_flow.compute", compute.fn)
    daily_diff_flow.fn()
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT manager_id, report_date, cusip, delta_type FROM daily_diffs ORDER BY cusip"
    ).fetchall()
    conn.close()
    assert rows == [
        (1, "2024-05-01", "AAA", "INCREASE"),
        (1, "2024-05-01", "BBB", "EXIT"),
        (1, "2024-05-01", "CCC", "ADD"),
        (1, "2024-05-01", "EEE", "DECREASE"),
    ]


def test_daily_diff_flow_strips_ciks(monkeypatch: pytest.MonkeyPatch):
    seen = []

    def fake_compute(date_value: str, db_path: str, cik_list: list[str] | None = None) -> None:
        seen.append((date_value, db_path, cik_list))

    monkeypatch.setenv("CIK_LIST", "0001, 0002")
    monkeypatch.setattr("etl.daily_diff_flow.compute", fake_compute)
    daily_diff_flow.fn(cik_list=None, date="2024-01-01")

    assert seen == [("2024-01-01", "dev.db", ["0001", "0002"])]


def test_daily_diff_flow_refreshes_materialized_view(monkeypatch: pytest.MonkeyPatch):
    class FakeConn:
        def __init__(self):
            self.executed = []
            self.closed = False

        def execute(self, sql):
            self.executed.append(sql)

        def close(self):
            self.closed = True

    fake_conn = FakeConn()
    calls = []

    def fake_compute(date_value: str, db_path: str, cik_list: list[str] | None = None) -> None:
        calls.append((date_value, db_path, cik_list))

    monkeypatch.setattr("etl.daily_diff_flow.compute", fake_compute)
    monkeypatch.setattr("etl.daily_diff_flow.connect_db", lambda _db_path: fake_conn)

    daily_diff_flow.fn(cik_list=["0001", "0002"], date="2024-01-01")

    assert calls == [("2024-01-01", "dev.db", ["0001", "0002"])]
    assert fake_conn.executed == ["REFRESH MATERIALIZED VIEW mv_daily_report"]
    assert fake_conn.closed is True
