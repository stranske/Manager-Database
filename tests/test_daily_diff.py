import sqlite3
import sys
from datetime import date
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.daily_diff_flow import compute_manager_diffs, daily_diff_flow


class FixedDate(date):
    @classmethod
    def today(cls):
        return cls(2024, 5, 2)


def _setup_db(tmp_path: Path) -> str:
    """Create a SQLite DB with canonical schema and sample data for two managers."""
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
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
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (101, 1, "13F-HR", "2024-01-01", "edgar"),
            (102, 1, "13F-HR", "2024-04-01", "edgar"),
            (201, 2, "13F-HR", "2024-01-01", "edgar"),
            (202, 2, "13F-HR", "2024-04-01", "edgar"),
        ],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (101, "AAA", "CorpA", 100, 1000),
            (101, "BBB", "CorpB", 30, 300),
            (101, "EEE", "CorpE", 10, 100),
            (102, "AAA", "CorpA", 120, 1200),
            (102, "CCC", "CorpC", 40, 400),
            (102, "EEE", "CorpE", 8, 80),
            (201, "XXX", "CorpX", 10, 100),
            (202, "XXX", "CorpX", 10, 100),
            (202, "YYY", "CorpY", 5, 50),
        ],
    )
    conn.commit()
    conn.close()
    return str(db_path)


def _setup_db_with_fk(tmp_path: Path) -> str:
    """Create a SQLite DB with foreign key constraint on daily_diffs."""
    db_path = tmp_path / "dev_fk.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
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
    conn.execute("""CREATE TABLE daily_diffs (
            diff_id INTEGER PRIMARY KEY AUTOINCREMENT,
            manager_id INTEGER NOT NULL REFERENCES managers(manager_id),
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
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (101, 1, "13F-HR", "2024-01-01", "edgar"),
            (102, 1, "13F-HR", "2024-04-01", "edgar"),
            (201, 2, "13F-HR", "2024-01-01", "edgar"),
            (202, 2, "13F-HR", "2024-04-01", "edgar"),
        ],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (101, "AAA", "CorpA", 100, 1000),
            (101, "BBB", "CorpB", 30, 300),
            (101, "EEE", "CorpE", 10, 100),
            (102, "AAA", "CorpA", 120, 1200),
            (102, "CCC", "CorpC", 40, 400),
            (102, "EEE", "CorpE", 8, 80),
            (201, "XXX", "CorpX", 10, 100),
            (202, "XXX", "CorpX", 10, 100),
            (202, "YYY", "CorpY", 5, 50),
        ],
    )
    conn.commit()
    conn.close()
    return str(db_path)


def test_compute_manager_diffs_persists_all_four_delta_types_with_values(tmp_path):
    """compute_manager_diffs writes all 4 delta types with correct prev/curr values."""
    db_path = _setup_db(tmp_path)
    conn = sqlite3.connect(db_path)
    count = compute_manager_diffs.fn(1, "2024-05-01", conn)
    conn.commit()

    rows = conn.execute(
        "SELECT cusip, delta_type, shares_prev, shares_curr, value_prev, value_curr "
        "FROM daily_diffs WHERE manager_id = 1 ORDER BY cusip"
    ).fetchall()
    conn.close()

    assert count == 4
    assert rows == [
        ("AAA", "INCREASE", 100, 120, 1000.0, 1200.0),
        ("BBB", "EXIT", 30, None, 300.0, None),
        ("CCC", "ADD", None, 40, None, 400.0),
        ("EEE", "DECREASE", 10, 8, 100.0, 80.0),
    ]


def test_compute_manager_diffs_valid_manager_fk(tmp_path):
    """All daily_diffs rows reference valid manager_ids (FK integrity)."""
    db_path = _setup_db_with_fk(tmp_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    compute_manager_diffs.fn(1, "2024-05-01", conn)
    conn.commit()

    orphan_count = conn.execute("""
        SELECT COUNT(*)
        FROM daily_diffs d
        LEFT JOIN managers m ON m.manager_id = d.manager_id
        WHERE m.manager_id IS NULL
    """).fetchone()[0]
    conn.close()
    assert orphan_count == 0


def test_compute_manager_diffs_is_idempotent(tmp_path):
    """Running compute_manager_diffs twice doesn't create duplicates."""
    db_path = _setup_db(tmp_path)
    conn = sqlite3.connect(db_path)
    compute_manager_diffs.fn(1, "2024-05-01", conn)
    conn.commit()
    compute_manager_diffs.fn(1, "2024-05-01", conn)
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM daily_diffs").fetchone()[0]
    conn.close()
    assert count == 4  # Not 8 (duplicated)


def test_daily_diff_flow_processes_multiple_managers(tmp_path, monkeypatch):
    """Flow iterates all managers and writes diffs for each."""
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.delenv("DB_URL", raising=False)

    daily_diff_flow.fn(date="2024-05-01")

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


def test_daily_diff_flow_defaults_to_yesterday(tmp_path, monkeypatch):
    """When date=None, the flow uses yesterday's date."""
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.delenv("DB_URL", raising=False)

    import etl.daily_diff_flow as ddf

    monkeypatch.setattr(ddf.dt, "date", FixedDate)

    daily_diff_flow.fn()

    conn = sqlite3.connect(db_path)
    dates = conn.execute("SELECT DISTINCT report_date FROM daily_diffs").fetchall()
    conn.close()
    assert dates == [("2024-05-01",)]


def test_daily_diff_flow_refreshes_matview_on_postgres(monkeypatch):
    """Flow calls REFRESH MATERIALIZED VIEW for non-SQLite (Postgres) connections."""
    executed_sql = []

    class FakePostgresConn:
        """Mimics a Postgres connection (not sqlite3.Connection)."""

        def execute(self, sql, params=None):
            executed_sql.append(sql)
            return self

        def fetchall(self):
            return [(1,)]

        def close(self):
            pass

    import etl.daily_diff_flow as ddf

    monkeypatch.setattr(ddf, "connect_db", lambda: FakePostgresConn())
    monkeypatch.setattr(ddf, "diff_holdings", lambda mid, conn: [])

    daily_diff_flow.fn(date="2024-01-01")

    assert "SELECT 1 FROM daily_diffs LIMIT 1" in executed_sql
    assert "BEGIN" in executed_sql
    assert "COMMIT" in executed_sql
    assert "REFRESH MATERIALIZED VIEW mv_daily_report" in executed_sql
    assert all("AUTOINCREMENT" not in sql for sql in executed_sql)
    assert all("CREATE TABLE IF NOT EXISTS daily_diffs" not in sql for sql in executed_sql)
