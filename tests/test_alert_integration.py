from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from alerts.db import ensure_alert_tables, fetch_rule_by_id, insert_pending_alert, rule_from_row
from alerts.engine import AlertEngine
from alerts.integration import (
    build_new_filing_event,
    evaluate_and_record_alerts,
    evaluate_and_record_new_filing_alerts,
    fire_alerts_for_event,
)
from alerts.models import AlertEvent, FiredAlert


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


def test_saved_legacy_new_filing_rule_matches_edgar_event(tmp_path):
    conn = _setup_db(tmp_path / "legacy-alerts.db")
    try:
        rule_id = _insert_rule(conn)
        conn.execute(
            "UPDATE alert_rules SET condition_json = ? WHERE rule_id = ?",
            (json.dumps({"filing_type": "13F-HR", "source": "sec"}), rule_id),
        )
        event = build_new_filing_event(
            filing_id=42,
            manager_id=1,
            filing_type="13F-HR",
            payload={"accession": "0000000000-24-000001", "source": "edgar"},
        )

        assert len(AlertEngine(conn).evaluate(event)) == 1
        assert (
            AlertEngine(conn).evaluate(
                build_new_filing_event(
                    filing_id=43, manager_id=1, filing_type="13D", payload={"source": "edgar"}
                )
            )
            == []
        )
        assert (
            AlertEngine(conn).evaluate(
                build_new_filing_event(
                    filing_id=44, manager_id=1, filing_type="13F-HR", payload={"source": "manual"}
                )
            )
            == []
        )
    finally:
        conn.close()


def test_large_delta_net_rule_matches_buy_and_sell_events(tmp_path):
    conn = _setup_db(tmp_path / "net-delta-alerts.db")
    try:
        conn.execute(
            """INSERT INTO alert_rules(name, event_type, condition_json, channels, enabled)
               VALUES (?, ?, ?, ?, ?)""",
            (
                "Net Delta Rule",
                "large_delta",
                json.dumps({"delta_type": "net", "value_usd_gt": 0}),
                '["streamlit"]',
                1,
            ),
        )
        engine = AlertEngine(conn)

        for delta_type in ("buy", "sell"):
            event = AlertEvent(
                event_type="large_delta",
                manager_id=1,
                payload={"delta_type": delta_type, "value_usd": 100},
            )
            assert len(engine.evaluate(event)) == 1

        for delta_type, value_usd in (("buy", 0), ("sell", 0), ("other", 100)):
            event = AlertEvent(
                event_type="large_delta",
                manager_id=1,
                payload={"delta_type": delta_type, "value_usd": value_usd},
            )
            assert engine.evaluate(event) == []
    finally:
        conn.close()


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


def test_insert_pending_alert_dedupes_edgar_accession_without_filing_id(tmp_path):
    """Legacy no-filing_id EDGAR replays must not duplicate on a fresh occurred_at."""
    conn = _setup_db(tmp_path / "dedupe.db")
    try:
        rule_id = _insert_rule(conn)
        row = fetch_rule_by_id(conn, rule_id)
        assert row is not None
        rule = rule_from_row(row)
        payload = {"accession": "0000000000-24-000001", "source": "edgar"}
        first = FiredAlert(
            rule=rule,
            event=AlertEvent(
                event_type="new_filing",
                manager_id=1,
                payload=payload,
                occurred_at=datetime(2026, 4, 15, 12, 0, tzinfo=UTC),
            ),
            channels=["streamlit"],
        )
        second = FiredAlert(
            rule=rule,
            event=AlertEvent(
                event_type="new_filing",
                manager_id=1,
                payload=payload,
                occurred_at=datetime(2026, 4, 15, 12, 0, tzinfo=UTC) + timedelta(hours=1),
            ),
            channels=["streamlit"],
        )
        first_id = insert_pending_alert(conn, first)
        second_id = insert_pending_alert(conn, second)
        assert first_id == second_id
        count = conn.execute("SELECT COUNT(*) FROM alert_history").fetchone()
        assert count == (1,)
    finally:
        conn.close()
