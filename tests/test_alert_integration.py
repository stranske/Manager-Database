from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from alerts.db import ensure_alert_tables
from alerts.integration import (
    build_new_filing_event,
    evaluate_and_record_alerts,
    evaluate_and_record_new_filing_alerts,
    fire_alerts_for_event,
)


def _setup_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT NOT NULL, cik TEXT)"
    )
    conn.execute("INSERT INTO managers(manager_id, name, cik) VALUES (1, 'Elliott', '0001791786')")
    ensure_alert_tables(conn)
    return conn


def _insert_rule(conn: sqlite3.Connection, *, manager_id: int | None = None) -> int:
    cursor = conn.execute(
        """INSERT INTO alert_rules(
            name, event_type, condition_json, channels, enabled, manager_id
        ) VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "New Filing Rule",
            "new_filing",
            '{"any_new_filing": true}',
            '["streamlit"]',
            1,
            manager_id,
        ),
    )
    conn.commit()
    assert cursor.lastrowid is not None
    return int(cursor.lastrowid)


def test_build_new_filing_event_populates_expected_payload():
    event = build_new_filing_event(
        filing_id=42,
        manager_id=1,
        filing_type="13F-HR",
        filed_date="2024-05-01",
        payload={"source": "edgar"},
    )

    assert event.event_type == "new_filing"
    assert event.manager_id == 1
    assert event.payload == {
        "source": "edgar",
        "type": "13F-HR",
        "filing_id": 42,
        "filed_date": "2024-05-01",
    }


def test_build_new_filing_event_requires_type():
    with pytest.raises(ValueError, match="filing_type is required"):
        build_new_filing_event(filing_id=1, manager_id=1)


def test_evaluate_and_record_alerts_persists_alert_history(tmp_path):
    conn = _setup_db(tmp_path / "alerts.db")
    try:
        rule_id = _insert_rule(conn)
        event = build_new_filing_event(
            filing_id=100,
            manager_id=1,
            filing_type="13F-HR",
            filed_date="2024-05-01",
        )

        alert_ids = evaluate_and_record_alerts(conn, event)

        assert len(alert_ids) == 1
        row = conn.execute(
            """SELECT rule_id, event_type, payload_json, delivered_channels
               FROM alert_history
               WHERE alert_id = ?""",
            (alert_ids[0],),
        ).fetchone()
        assert row == (
            rule_id,
            "new_filing",
            '{"filed_date":"2024-05-01","filing_id":100,"type":"13F-HR"}',
            '["streamlit"]',
        )
    finally:
        conn.close()


def test_evaluate_and_record_new_filing_alerts_returns_empty_when_rule_does_not_match(tmp_path):
    conn = _setup_db(tmp_path / "alerts.db")
    try:
        _insert_rule(conn, manager_id=2)

        alert_ids = evaluate_and_record_new_filing_alerts(
            conn,
            filing_id=100,
            manager_id=1,
            filing_type="13F-HR",
        )

        assert alert_ids == []
        count = conn.execute("SELECT COUNT(*) FROM alert_history").fetchone()
        assert count == (0,)
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_fire_alerts_for_event_dispatches_matching_channels(tmp_path):
    conn = _setup_db(tmp_path / "dispatch.db")
    try:
        _insert_rule(conn)

        alert_ids = await fire_alerts_for_event(
            conn,
            build_new_filing_event(
                filing_id=101,
                manager_id=1,
                filing_type="13F-HR",
                filed_date="2024-05-02",
            ),
        )

        row = conn.execute(
            "SELECT delivered_channels FROM alert_history WHERE alert_id = ?",
            (alert_ids[0],),
        ).fetchone()
    finally:
        conn.close()

    assert len(alert_ids) == 1
    assert row == ('["streamlit"]',)
