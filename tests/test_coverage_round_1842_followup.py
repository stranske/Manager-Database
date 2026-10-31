"""Round 1842 follow-up: remaining dashboard/chat helper branches."""

from __future__ import annotations

import sqlite3
from datetime import date

import pandas as pd

from api import chat, managers
from ui import dashboard


def test_api_base_url_prefers_alerts_env(monkeypatch):
    monkeypatch.setenv("API_BASE_URL", "http://api.example")
    monkeypatch.setenv("ALERTS_API_BASE_URL", "http://alerts.example")
    assert dashboard._api_base_url() == "http://alerts.example"


def test_load_unacknowledged_alert_count_offline_mode(monkeypatch):
    monkeypatch.setenv("UI_OFFLINE", "1")
    assert dashboard.load_unacknowledged_alert_count() == 0


def test_render_active_campaigns_widget_empty_snapshot(monkeypatch):
    class _FakeSt:
        def __init__(self):
            self.captions = []

        def subheader(self, _value):
            return None

        def caption(self, value):
            self.captions.append(value)

    fake = _FakeSt()
    monkeypatch.setattr(dashboard, "st", fake)
    monkeypatch.setattr(
        dashboard, "load_active_campaigns_summary", lambda **kwargs: pd.DataFrame()
    )
    dashboard.render_active_campaigns_widget(show_heading=True)
    assert fake.captions == ["No active materialized campaign snapshot is available."]


def test_chat_normalizers_and_chain_context_name_resolution():
    assert chat._normalize_manager_ids([1, "2"]) == [1, 2]
    assert chat._normalize_manager_ids(None) == []
    assert chat._normalize_cusips([" AAA ", ""]) == ["AAA"]
    assert chat._parse_date_range(["2026-01-01", "2026-01-31"]) == (
        date(2026, 1, 1),
        date(2026, 1, 31),
    )

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO managers VALUES (7, 'Alpha Capital')")
    normalized = chat._normalize_chain_context({"manager_name": "Alpha Capital"}, conn)
    assert normalized["manager_ids"] == [7]


def test_universe_schema_quality_flags_on_partial_legacy_sqlite_table(monkeypatch):
    conn = sqlite3.connect(":memory:")
    try:
        monkeypatch.setattr(managers, "_ensure_manager_table", lambda _conn: None)
        conn.execute(
            """CREATE TABLE managers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                cik TEXT,
                jurisdiction TEXT,
                jurisdictions TEXT NOT NULL DEFAULT '[]',
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"""
        )
        managers._ensure_universe_schema(conn)
        columns = {
            str(row[0]) for row in conn.execute(managers.SQLITE_TABLE_INFO_SQL, ("managers",))
        }
        assert "quality_flags" in columns
    finally:
        conn.close()
