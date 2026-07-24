"""Acceptance tests for bitemporal holdings + as_of query (#1463)."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from etl import ingest_flow, point_in_time


def _connect(tmp_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "bitemporal.db")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE managers (
            manager_id INTEGER PRIMARY KEY,
            name TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE filings (
            filing_id INTEGER PRIMARY KEY,
            manager_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            period_end TEXT,
            filed_date TEXT,
            source TEXT NOT NULL,
            raw_key TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE holdings (
            holding_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id INTEGER NOT NULL,
            cusip TEXT,
            name_of_issuer TEXT,
            shares INTEGER,
            value_usd REAL,
            content_hash TEXT,
            knowledge_time TEXT NOT NULL,
            superseded_at TEXT,
            version INTEGER NOT NULL DEFAULT 1,
            created_at TEXT
        )
        """
    )
    conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'Test Manager')")
    conn.commit()
    return conn


def test_holdings_as_of_preserves_versions_and_no_lookahead(tmp_path):
    conn = _connect(tmp_path)
    # v1 filing
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source, raw_key) "
        "VALUES (10, 1, '13F-HR', '2024-03-31', '2024-05-01', 'us', 'acc-v1')"
    )
    v1_rows = [
        {"cusip": "037833100", "nameOfIssuer": "APPLE INC", "sshPrnamt": 100, "value": 1000},
    ]
    t1 = "2024-05-01T12:00:00Z"
    ingest_flow._replace_holdings_rows(
        conn,
        filing_id=10,
        manager_id=1,
        identifier="0000320193",
        external_id="acc-v1",
        filed_date="2024-05-01",
        parsed_rows=v1_rows,
        jurisdiction="us",
    )
    # Force known knowledge_time for v1 rows
    conn.execute("UPDATE holdings SET knowledge_time = ? WHERE filing_id = 10", (t1,))
    conn.commit()

    before = point_in_time.holdings_as_of(conn, 1, datetime(2024, 5, 2, tzinfo=UTC))
    assert len(before) == 1
    assert before[0]["cusip"] == "037833100"
    assert int(before[0]["shares"]) == 100

    # Amended filing with changed holdings (v2)
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source, raw_key) "
        "VALUES (11, 1, '13F-HR/A', '2024-03-31', '2024-06-01', 'us', 'acc-v2')"
    )
    v2_rows = [
        {"cusip": "037833100", "nameOfIssuer": "APPLE INC", "sshPrnamt": 250, "value": 2500},
    ]
    t2 = "2024-06-01T12:00:00Z"
    ingest_flow._replace_holdings_rows(
        conn,
        filing_id=11,
        manager_id=1,
        identifier="0000320193",
        external_id="acc-v2",
        filed_date="2024-06-01",
        parsed_rows=v2_rows,
        jurisdiction="us",
    )
    conn.execute("UPDATE holdings SET knowledge_time = ? WHERE filing_id = 11", (t2,))
    # Keep superseded_at on v1 as the amendment knowledge time
    conn.execute(
        "UPDATE holdings SET superseded_at = ? WHERE filing_id = 10 AND superseded_at IS NOT NULL",
        (t2,),
    )
    conn.commit()

    # Before v2 knowledge: still v1
    as_of_before_v2 = point_in_time.holdings_as_of(
        conn, 1, datetime(2024, 5, 15, tzinfo=UTC)
    )
    assert len(as_of_before_v2) == 1
    assert int(as_of_before_v2[0]["shares"]) == 100
    assert int(as_of_before_v2[0]["filing_id"]) == 10

    # After v2 knowledge: v2, and v1 retained with superseded_at
    as_of_after = point_in_time.holdings_as_of(conn, 1, datetime(2024, 6, 15, tzinfo=UTC))
    assert len(as_of_after) == 1
    assert int(as_of_after[0]["shares"]) == 250
    assert int(as_of_after[0]["filing_id"]) == 11

    retained = conn.execute(
        "SELECT holding_id, superseded_at FROM holdings WHERE filing_id = 10"
    ).fetchall()
    assert retained
    assert all(row["superseded_at"] is not None for row in retained)

    # No look-ahead: as_of before v2 must not return v2
    assert all(int(row["filing_id"]) != 11 for row in as_of_before_v2)


def test_identical_reingest_is_noop(tmp_path):
    conn = _connect(tmp_path)
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source, raw_key) "
        "VALUES (20, 1, '13F-HR', '2024-06-30', '2024-08-01', 'us', 'acc-same')"
    )
    rows = [
        {"cusip": "594918104", "nameOfIssuer": "MICROSOFT", "sshPrnamt": 10, "value": 50},
    ]
    first = ingest_flow._replace_holdings_rows(
        conn,
        filing_id=20,
        manager_id=1,
        identifier="0000789019",
        external_id="acc-same",
        filed_date="2024-08-01",
        parsed_rows=rows,
        jurisdiction="us",
    )
    second = ingest_flow._replace_holdings_rows(
        conn,
        filing_id=20,
        manager_id=1,
        identifier="0000789019",
        external_id="acc-same",
        filed_date="2024-08-01",
        parsed_rows=rows,
        jurisdiction="us",
    )
    assert first == 1
    assert second == 0
    versions = conn.execute("SELECT COUNT(*), MAX(version) FROM holdings WHERE filing_id = 20").fetchone()
    assert int(versions[0]) == 1
    assert int(versions[1]) == 1


def test_current_holdings_helper_hides_superseded(tmp_path):
    conn = _connect(tmp_path)
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source, raw_key) "
        "VALUES (30, 1, '13F-HR', '2024-09-30', '2024-11-01', 'us', 'acc-cur')"
    )
    rows_v1 = [
        {"cusip": "023135106", "nameOfIssuer": "AMAZON", "sshPrnamt": 1, "value": 10},
    ]
    rows_v2 = [
        {"cusip": "023135106", "nameOfIssuer": "AMAZON", "sshPrnamt": 9, "value": 90},
    ]
    ingest_flow._replace_holdings_rows(
        conn,
        filing_id=30,
        manager_id=1,
        identifier="0001018724",
        external_id="acc-cur",
        filed_date="2024-11-01",
        parsed_rows=rows_v1,
        jurisdiction="us",
    )
    ingest_flow._replace_holdings_rows(
        conn,
        filing_id=30,
        manager_id=1,
        identifier="0001018724",
        external_id="acc-cur",
        filed_date="2024-11-01",
        parsed_rows=rows_v2,
        jurisdiction="us",
    )
    current = point_in_time.current_holdings(conn, filing_id=30)
    assert len(current) == 1
    assert int(current[0]["shares"]) == 9
    total = conn.execute("SELECT COUNT(*) FROM holdings WHERE filing_id = 30").fetchone()[0]
    assert int(total) == 2


def test_deliberate_break_mutate_breaks_as_of(tmp_path):
    """Reverting append-only to mutate makes the as-of/no-look-ahead test fail."""
    conn = _connect(tmp_path)
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source, raw_key) "
        "VALUES (40, 1, '13F-HR', '2024-12-31', '2025-02-01', 'us', 'acc-break')"
    )
    v1 = [{"cusip": "A", "nameOfIssuer": "A", "sshPrnamt": 1, "value": 1}]
    v2 = [{"cusip": "A", "nameOfIssuer": "A", "sshPrnamt": 2, "value": 2}]
    ingest_flow._replace_holdings_rows(
        conn,
        filing_id=40,
        manager_id=1,
        identifier="1",
        external_id="acc-break",
        filed_date="2025-02-01",
        parsed_rows=v1,
        jurisdiction="us",
    )
    conn.execute(
        "UPDATE holdings SET knowledge_time = ? WHERE filing_id = 40",
        ("2025-02-01T00:00:00Z",),
    )

    # Deliberate break: mutate/delete instead of append
    ingest_flow._delete_holdings_rows(conn, filing_id=40)
    ingest_flow._insert_holdings_rows(
        conn,
        filing_id=40,
        manager_id=1,
        identifier="1",
        external_id="acc-break",
        filed_date="2025-02-01",
        parsed_rows=v2,
        jurisdiction="us",
        knowledge_time="2025-03-01T00:00:00Z",
        version=2,
    )
    conn.commit()

    before = point_in_time.holdings_as_of(conn, 1, datetime(2025, 2, 15, tzinfo=UTC))
    # Under mutate semantics the historical v1 row is gone, so as-of cannot reconstruct it.
    assert before == [] or all(int(row["shares"]) != 1 for row in before)
