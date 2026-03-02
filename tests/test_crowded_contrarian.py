import json
import sqlite3

from etl.conviction_flow import detect_crowded_trades


def _setup_db(tmp_path, manager_count: int) -> str:
    db_path = tmp_path / "conviction.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute(
        "CREATE TABLE filings ("
        "filing_id INTEGER PRIMARY KEY, "
        "manager_id INTEGER NOT NULL, "
        "type TEXT, "
        "period_end TEXT, "
        "filed_date TEXT, "
        "source TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings ("
        "holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "filing_id INTEGER NOT NULL, "
        "cusip TEXT, "
        "name_of_issuer TEXT, "
        "shares INTEGER, "
        "value_usd REAL)"
    )
    conn.execute(
        "CREATE TABLE crowded_trades ("
        "crowd_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "cusip TEXT NOT NULL, "
        "name_of_issuer TEXT, "
        "manager_count INTEGER NOT NULL, "
        "manager_ids TEXT NOT NULL, "
        "total_value_usd REAL, "
        "avg_conviction_pct REAL, "
        "max_conviction_pct REAL, "
        "report_date TEXT NOT NULL, "
        "computed_at TEXT DEFAULT CURRENT_TIMESTAMP, "
        "UNIQUE(cusip, report_date))"
    )

    for manager_id in range(1, manager_count + 1):
        conn.execute(
            "INSERT INTO managers(manager_id, name) VALUES (?, ?)",
            (manager_id, f"Manager {manager_id}"),
        )
        old_filing_id = manager_id * 10
        new_filing_id = old_filing_id + 1
        conn.execute(
            "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
            "VALUES (?, ?, '13F-HR', '2023-12-31', '2024-01-15', 'edgar')",
            (old_filing_id, manager_id),
        )
        conn.execute(
            "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
            "VALUES (?, ?, '13F-HR', '2024-03-31', '2024-04-15', 'edgar')",
            (new_filing_id, manager_id),
        )

        conn.execute(
            "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
            "VALUES (?, 'OLD000001', 'Old Co', 1, 999)",
            (old_filing_id,),
        )

        aapl_value = 300 if manager_id == manager_count else 100
        other_value = 100
        conn.execute(
            "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
            "VALUES (?, '037833100', 'Apple Inc', 10, ?)",
            (new_filing_id, aapl_value),
        )
        conn.execute(
            "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
            "VALUES (?, ?, ?, 10, ?)",
            (new_filing_id, f"OTHER{manager_id:03d}", f"Other {manager_id}", other_value),
        )

    conn.commit()
    conn.close()
    return str(db_path)


def test_detect_crowded_trades_identifies_aapl_across_latest_filings(tmp_path):
    db_path = _setup_db(tmp_path, manager_count=5)
    conn = sqlite3.connect(db_path)

    inserted = detect_crowded_trades.fn("2024-05-01", min_managers=3, conn=conn)

    row = conn.execute(
        "SELECT cusip, name_of_issuer, manager_count, manager_ids, total_value_usd, "
        "avg_conviction_pct, max_conviction_pct "
        "FROM crowded_trades WHERE report_date = '2024-05-01'"
    ).fetchone()
    conn.close()

    assert inserted == 1
    assert row[0] == "037833100"
    assert row[1] == "Apple Inc"
    assert row[2] == 5
    assert json.loads(row[3]) == [1, 2, 3, 4, 5]
    assert row[4] == 700.0
    assert round(row[5], 2) == 55.0
    assert row[6] == 75.0


def test_detect_crowded_trades_applies_threshold_filter(tmp_path):
    db_path = _setup_db(tmp_path, manager_count=2)
    conn = sqlite3.connect(db_path)

    inserted = detect_crowded_trades.fn("2024-05-01", min_managers=3, conn=conn)
    count = conn.execute("SELECT COUNT(*) FROM crowded_trades").fetchone()[0]
    conn.close()

    assert inserted == 0
    assert count == 0


def test_detect_crowded_trades_is_idempotent_for_same_report_date(tmp_path):
    db_path = _setup_db(tmp_path, manager_count=3)
    conn = sqlite3.connect(db_path)

    first = detect_crowded_trades.fn("2024-05-01", min_managers=3, conn=conn)

    conn.execute(
        "INSERT INTO managers(manager_id, name) VALUES (?, ?)",
        (4, "Manager 4"),
    )
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
        "VALUES (40, 4, '13F-HR', '2023-12-31', '2024-01-15', 'edgar')"
    )
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
        "VALUES (41, 4, '13F-HR', '2024-03-31', '2024-04-15', 'edgar')"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (40, 'OLD000001', 'Old Co', 1, 999)"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (41, '037833100', 'Apple Inc', 10, 100)"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (41, 'OTHER004', 'Other 4', 10, 100)"
    )
    conn.commit()

    second = detect_crowded_trades.fn("2024-05-01", min_managers=3, conn=conn)

    rows = conn.execute(
        "SELECT cusip, manager_count, manager_ids FROM crowded_trades "
        "WHERE report_date = '2024-05-01'"
    ).fetchall()
    conn.close()

    assert first == 1
    assert second == 1
    assert len(rows) == 1
    assert rows[0][0] == "037833100"
    assert rows[0][1] == 4
    assert json.loads(rows[0][2]) == [1, 2, 3, 4]


def test_detect_crowded_trades_uses_env_threshold_when_unspecified(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, manager_count=3)
    conn = sqlite3.connect(db_path)
    monkeypatch.setenv("CROWDED_TRADE_MIN_MANAGERS", "4")

    inserted = detect_crowded_trades.fn("2024-05-01", conn=conn)
    count = conn.execute("SELECT COUNT(*) FROM crowded_trades").fetchone()[0]
    conn.close()

    assert inserted == 0
    assert count == 0


def test_detect_crowded_trades_ignores_future_filings(tmp_path):
    db_path = _setup_db(tmp_path, manager_count=5)
    conn = sqlite3.connect(db_path)

    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
        "VALUES (12, 1, '13F-HR', '2024-06-30', '2024-07-15', 'edgar')"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (12, '594918104', 'Microsoft Corp', 10, 100)"
    )
    conn.commit()

    inserted = detect_crowded_trades.fn("2024-05-01", min_managers=3, conn=conn)
    row = conn.execute(
        "SELECT manager_count, manager_ids FROM crowded_trades "
        "WHERE report_date = '2024-05-01' AND cusip = '037833100'"
    ).fetchone()
    conn.close()

    assert inserted == 1
    assert row[0] == 5
    assert json.loads(row[1]) == [1, 2, 3, 4, 5]
