import sqlite3
from pathlib import Path

from api import managers as managers_module
from etl.daily_diff_flow import daily_diff_flow


def test_conflicting_update_records_flag(tmp_path: Path) -> None:
    db_path = tmp_path / "quality.db"
    conn = sqlite3.connect(db_path)
    managers_module._ensure_manager_table(conn)
    manager_id = managers_module._insert_manager(
        conn,
        managers_module.ManagerCreate(
            name="Conflict Manager",
            cik="0000000001",
            lei="LEI-A",
            registry_ids={"fca_frn": "122927"},
        ),
    )

    updated = managers_module._update_manager(
        conn,
        manager_id,
        managers_module.ManagerUpdate(
            lei="LEI-B",
            registry_ids={"fca_frn": "998877"},
        ),
    )
    row = managers_module._fetch_manager(conn, str(db_path), manager_id)
    conn.close()

    assert updated is True
    assert row is not None
    manager = managers_module._to_manager_response(row)
    assert manager.lei == "LEI-B"
    assert manager.registry_ids == {"fca_frn": "998877"}
    assert {
        "field": "lei",
        "old": "LEI-A",
        "new": "LEI-B",
    }.items() <= manager.quality_flags[0].items()
    assert {
        "field": "registry_ids.fca_frn",
        "old": "122927",
        "new": "998877",
    }.items() <= manager.quality_flags[1].items()


def test_delta_missing_fields(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "diff_quality.db"
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
    conn.execute(
        "INSERT INTO managers(manager_id, name, cik) VALUES (1, 'Quality Manager', '0000000001')"
    )
    conn.executemany(
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (101, 1, "13F-HR", "2024-01-01", "edgar"),
            (102, 1, "13F-HR", "2024-04-01", "edgar"),
        ],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (101, "MISS12345", "Missing Shares Corp", None, 100.0),
            (102, "MISS12345", "Missing Shares Corp", None, 130.0),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)

    result = daily_diff_flow.fn(date="2024-05-01")

    missing_fields = result.outputs["data_quality"]["missing_fields"]
    assert result.outputs["data_quality"]["confidence"] == "low"
    assert {
        "manager_id": 1,
        "cusip": "MISS12345",
        "field": "shares_prev",
    } in missing_fields
    assert {
        "manager_id": 1,
        "cusip": "MISS12345",
        "field": "shares_curr",
    } in missing_fields
