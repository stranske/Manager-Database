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


@pytest.mark.nightly
def test_daily_diff_flow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    daily_diff_flow(["0000000000"], date="2024-05-01")
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT cik, cusip, change FROM daily_diff ORDER BY cusip"
    ).fetchall()
    conn.close()
    assert rows == [
        ("0000000000", "BBB", "EXIT"),
        ("0000000000", "CCC", "ADD"),
    ]


def test_compute_writes_daily_diff_rows(tmp_path: Path):
    db_path = setup_db(tmp_path)
    # Run the task body directly to avoid Prefect orchestration in unit tests.
    compute.fn("0000000000", "2024-05-01", db_path)
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT date, cik, cusip, change FROM daily_diff ORDER BY cusip"
    ).fetchall()
    conn.close()
    assert rows == [
        ("2024-05-01", "0000000000", "BBB", "EXIT"),
        ("2024-05-01", "0000000000", "CCC", "ADD"),
    ]


def test_daily_diff_flow_uses_env_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
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

    # Ensure the default date path resolves to yesterday deterministically.
    monkeypatch.setattr("etl.daily_diff_flow.dt", DateTimeShim)
    # Avoid Prefect orchestration by running the task body directly.
    monkeypatch.setattr("etl.daily_diff_flow.compute", compute.fn)
    daily_diff_flow.fn()
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT date, cik, cusip, change FROM daily_diff ORDER BY cusip"
    ).fetchall()
    conn.close()
    assert rows == [
        ("2024-05-01", "0000000000", "BBB", "EXIT"),
        ("2024-05-01", "0000000000", "CCC", "ADD"),
    ]


def test_compute_handles_no_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "dev.db"

    def fake_diff_holdings(_cik: str, _db_path: str):
        # Force the no-change path to confirm zero inserts.
        return set(), set()

    monkeypatch.setattr("etl.daily_diff_flow.diff_holdings", fake_diff_holdings)
    compute.fn("0000000000", "2024-05-01", str(db_path))

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT * FROM daily_diff").fetchall()
    conn.close()
    assert rows == []


def test_daily_diff_flow_strips_ciks(monkeypatch: pytest.MonkeyPatch):
    seen = []

    def fake_compute(cik: str, _date: str, _db_path: str) -> None:
        seen.append(cik)

    # Ensure whitespace in CIK_LIST does not affect the split logic.
    monkeypatch.setenv("CIK_LIST", "0001, 0002")
    monkeypatch.setattr("etl.daily_diff_flow.compute", fake_compute)
    daily_diff_flow.fn(cik_list=None, date="2024-01-01")

    assert seen == ["0001", "0002"]
