from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from alerts.db import ensure_alert_tables
from alerts.engine import AlertEngine
from alerts.models import AlertEvent, AlertRuleCreate


def _setup_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'Elliott')")
    conn.execute("INSERT INTO managers(manager_id, name) VALUES (2, 'SIR Capital')")
    ensure_alert_tables(conn)
    return conn


def _insert_rule(
    conn: sqlite3.Connection,
    *,
    name: str,
    event_type: str,
    condition_json: str,
    channels: str = '["streamlit"]',
    enabled: int = 1,
    manager_id: int | None = None,
) -> None:
    conn.execute(
        """INSERT INTO alert_rules(
            name, event_type, condition_json, channels, enabled, manager_id
        ) VALUES (?, ?, ?, ?, ?, ?)""",
        (name, event_type, condition_json, channels, enabled, manager_id),
    )
    conn.commit()


def test_alert_engine_matches_value_threshold_and_delta_type(tmp_path):
    conn = _setup_db(tmp_path / "alerts.db")
    try:
        _insert_rule(
            conn,
            name="Large Buy Rule",
            event_type="large_delta",
            condition_json='{"delta_type":"buy","value_usd_gt":1000000}',
        )

        engine = AlertEngine(conn)
        fired = engine.evaluate(
            AlertEvent(
                event_type="large_delta",
                manager_id=1,
                payload={"delta_type": "buy", "value_usd": 2500000},
            )
        )

        assert len(fired) == 1
        assert fired[0].rule.name == "Large Buy Rule"
        assert fired[0].channels == ["streamlit"]
    finally:
        conn.close()


def test_alert_engine_manager_filter_and_disabled_rules(tmp_path):
    conn = _setup_db(tmp_path / "alerts.db")
    try:
        _insert_rule(
            conn,
            name="Disabled Rule",
            event_type="new_filing",
            condition_json='{"any_new_filing":true}',
            enabled=0,
        )
        _insert_rule(
            conn,
            name="Manager Specific Rule",
            event_type="new_filing",
            condition_json='{"any_new_filing":true}',
            manager_id=2,
        )

        engine = AlertEngine(conn)
        fired = engine.evaluate(
            AlertEvent(event_type="new_filing", manager_id=1, payload={"type": "13F-HR"})
        )

        assert fired == []
    finally:
        conn.close()


def test_alert_engine_supports_news_and_crowded_trade_conditions(tmp_path):
    conn = _setup_db(tmp_path / "alerts.db")
    try:
        _insert_rule(
            conn,
            name="News Surge",
            event_type="news_spike",
            condition_json='{"news_count_gt":5,"time_window_hours":4}',
            channels='["email","streamlit"]',
        )
        _insert_rule(
            conn,
            name="Crowding Rule",
            event_type="crowded_trade_change",
            condition_json='{"manager_count_gte":8}',
        )
        engine = AlertEngine(conn)

        recent_news = engine.evaluate(
            AlertEvent(
                event_type="news_spike",
                manager_id=1,
                payload={"news_count": 7},
                occurred_at=datetime.now(UTC) - timedelta(hours=1),
            )
        )
        crowded = engine.evaluate(
            AlertEvent(
                event_type="crowded_trade_change",
                manager_id=1,
                payload={"manager_count": 9},
            )
        )

        assert [alert.rule.name for alert in recent_news] == ["News Surge"]
        assert recent_news[0].channels == ["email", "streamlit"]
        assert [alert.rule.name for alert in crowded] == ["Crowding Rule"]
    finally:
        conn.close()


def test_alert_engine_returns_multiple_matches_for_same_event(tmp_path):
    conn = _setup_db(tmp_path / "alerts.db")
    try:
        _insert_rule(
            conn,
            name="All Filing Rule",
            event_type="new_filing",
            condition_json='{"any_new_filing":true}',
        )
        _insert_rule(
            conn,
            name="Manager Filing Rule",
            event_type="new_filing",
            condition_json='{"any_new_filing":true}',
            manager_id=1,
        )
        engine = AlertEngine(conn)

        fired = engine.evaluate(
            AlertEvent(event_type="new_filing", manager_id=1, payload={"type": "13F-HR"})
        )

        assert [alert.rule.name for alert in fired] == ["All Filing Rule", "Manager Filing Rule"]
    finally:
        conn.close()


def test_alert_engine_returns_empty_when_no_rules_match(tmp_path):
    conn = _setup_db(tmp_path / "alerts.db")
    try:
        _insert_rule(
            conn,
            name="Old News Only",
            event_type="news_spike",
            condition_json='{"news_count_gt":10,"time_window_hours":1}',
        )
        engine = AlertEngine(conn)

        fired = engine.evaluate(
            AlertEvent(
                event_type="news_spike",
                manager_id=1,
                payload={"news_count": 4},
                occurred_at=datetime.now(UTC) - timedelta(hours=3),
            )
        )

        assert fired == []
    finally:
        conn.close()


def test_alert_models_reject_invalid_event_types_and_channels():
    with pytest.raises(ValueError, match="Unsupported event_type"):
        AlertEvent(event_type="manager_update", manager_id=1, payload={})

    with pytest.raises(ValueError, match="Unsupported channel"):
        AlertRuleCreate(
            name="Bad Channel",
            event_type="new_filing",
            condition_json={},
            channels=["pagerduty"],
        )


def test_alert_engine_ensures_schema_once_at_init(tmp_path, monkeypatch):
    conn = _setup_db(tmp_path / "alerts.db")
    try:
        calls: list[str] = []

        def _record_ensure(db_conn: sqlite3.Connection) -> None:
            assert db_conn is conn
            calls.append("ensure")

        monkeypatch.setattr("alerts.engine.ensure_alert_tables", _record_ensure)
        engine = AlertEngine(conn)

        engine.evaluate(AlertEvent(event_type="new_filing", manager_id=1, payload={}))
        engine.evaluate(AlertEvent(event_type="new_filing", manager_id=1, payload={}))

        assert calls == ["ensure"]
    finally:
        conn.close()
