import sqlite3
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.summariser_flow import summarise


def setup_db(path: Path) -> str:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE daily_diff (date TEXT, cik TEXT, cusip TEXT, change TEXT)"
    )
    conn.execute(
        "INSERT INTO daily_diff VALUES (?,?,?,?)",
        ("2024-01-02", "1", "AAA", "ADD"),
    )
    conn.execute(
        "INSERT INTO daily_diff VALUES (?,?,?,?)",
        ("2024-01-02", "1", "BBB", "EXIT"),
    )
    conn.commit()
    conn.close()
    return str(path)


def test_summarise(tmp_path, monkeypatch):
    db_file = tmp_path / "dev.db"
    setup_db(db_file)
    monkeypatch.setenv("DB_PATH", str(db_file))
    result = summarise.fn("2024-01-02")
    assert result == "2 changes on 2024-01-02"
