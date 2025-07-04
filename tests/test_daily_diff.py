import sqlite3
from pathlib import Path

import pytest

from etl.daily_diff_flow import daily_diff_flow


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
