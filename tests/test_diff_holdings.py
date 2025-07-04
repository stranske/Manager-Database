import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from diff_holdings import diff_holdings


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
