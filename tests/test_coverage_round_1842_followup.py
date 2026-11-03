"""Round 1842 follow-up: remaining dashboard/chat helper branches.

Evidence for issue #1842 closer repair (merged #1876 baseline; this module
change is the offline ``httpx.Client`` call-count guard only):

  PYTHONPATH=. /opt/anaconda3/bin/python3 -m pytest \\
    tests/test_coverage_round_1842_followup.py -m 'not slow' -q

Break-and-revert receipts live in the repair PR body (2026-11-03); each test
fails on a deliberate production mutation and passes after restore.

Gate listed-target uncovered units (U = missing_lines + missing_branches,
Python 3.13, ``pr-00-gate.yml`` coverage.json):

* Baseline after #1843 Gate run 39362579319 (commit 3631f0ff): daily_diff 2,
  managers 16, chat 14, dashboard 67 (sum 99).
* Head after #1876 Gate run 41248974079 (commit 4ffa37f7): daily_diff 2,
  managers 13, chat 4, dashboard 31 (sum 50).
* Measured reduction on listed targets: 49 U; shortfall vs ~180 chunk goal:
  max(0, 180 − 109) ≈ 71 from issue-dispatch baseline (159 U), or ~71 after
  crediting #1843+#1876 together (see repair PR ranking/deferrals).
"""

from __future__ import annotations

import sqlite3
from datetime import date
from unittest.mock import MagicMock

import pandas as pd

from api import chat, managers
from ui import dashboard


def test_api_base_url_prefers_alerts_env(monkeypatch):
    monkeypatch.setenv("API_BASE_URL", "http://api.example/")
    monkeypatch.setenv("ALERTS_API_BASE_URL", "http://alerts.example/")
    assert dashboard._api_base_url() == "http://alerts.example"

    monkeypatch.delenv("ALERTS_API_BASE_URL")
    assert dashboard._api_base_url() == "http://api.example"

    monkeypatch.delenv("API_BASE_URL")
    assert dashboard._api_base_url() == "http://localhost:8000"


def test_load_unacknowledged_alert_count_offline_mode(monkeypatch):
    """Offline badge must short-circuit without constructing httpx.Client.

    Clear the streamlit cache before/after so a cached online count (e.g. 7 from
    ``test_dashboard``) cannot mask the UI_OFFLINE guard. A request failure also
    returns 0, so assert Client is never constructed rather than only checking 0.
    """
    dashboard.load_unacknowledged_alert_count.clear()
    monkeypatch.setenv("UI_OFFLINE", "1")

    client_ctor = MagicMock()
    monkeypatch.setattr(dashboard.httpx, "Client", client_ctor)
    try:
        result = dashboard.load_unacknowledged_alert_count()
        assert client_ctor.call_count == 0
        assert result == 0
    finally:
        dashboard.load_unacknowledged_alert_count.clear()


def test_render_active_campaigns_widget_empty_snapshot(monkeypatch):
    class _FakeSt:
        def __init__(self):
            self.captions = []
            self.subheaders = []

        def subheader(self, value):
            self.subheaders.append(value)

        def caption(self, value):
            self.captions.append(value)

    fake = _FakeSt()
    monkeypatch.setattr(dashboard, "st", fake)
    monkeypatch.setattr(dashboard, "load_active_campaigns_summary", lambda **kwargs: pd.DataFrame())
    dashboard.render_active_campaigns_widget(show_heading=True)
    assert fake.subheaders == ["Active Campaigns"]
    assert fake.captions == ["No active materialized campaign snapshot is available."]

    fake.captions.clear()
    fake.subheaders.clear()
    dashboard.render_active_campaigns_widget(show_heading=False)
    assert fake.subheaders == []
    assert fake.captions == ["No active materialized campaign snapshot is available."]


def test_chat_normalizers_and_chain_context_name_resolution():
    assert chat._normalize_manager_ids([1, "2", "bad", None]) == [1, 2]
    assert chat._normalize_manager_ids(None) == []
    assert chat._normalize_cusips([" AAA ", "", None]) == ["AAA"]
    assert chat._parse_date_range(["2026-01-01", "2026-01-31"]) == (
        date(2026, 1, 1),
        date(2026, 1, 31),
    )
    assert chat._parse_date_range({"start": "2026-02-01", "end": "2026-02-28"}) == (
        date(2026, 2, 1),
        date(2026, 2, 28),
    )
    assert chat._parse_date_range(["2026-01-01", "not-a-date"]) is None
    assert chat._normalize_chain_context(None, object()) == {}

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO managers VALUES (7, 'Alpha Capital')")
        by_name = chat._normalize_chain_context({"manager_name": "Alpha Capital"}, conn)
        assert by_name["manager_ids"] == [7]

        by_singular = chat._normalize_chain_context({"manager_id": "7"}, conn)
        assert by_singular["manager_ids"] == [7]

        unknown = chat._normalize_chain_context({"manager_name": "Missing Fund"}, conn)
        assert "manager_ids" not in unknown
    finally:
        conn.close()


def test_universe_schema_quality_flags_on_partial_legacy_sqlite_table(monkeypatch):
    conn = sqlite3.connect(":memory:")
    try:
        monkeypatch.setattr(managers, "_ensure_manager_table", lambda _conn: None)
        conn.execute("""CREATE TABLE managers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                cik TEXT,
                jurisdiction TEXT,
                jurisdictions TEXT NOT NULL DEFAULT '[]',
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )""")
        before = {
            str(row[0]) for row in conn.execute(managers.SQLITE_TABLE_INFO_SQL, ("managers",))
        }
        assert "quality_flags" not in before

        managers._ensure_universe_schema(conn)
        columns = {
            str(row[0]) for row in conn.execute(managers.SQLITE_TABLE_INFO_SQL, ("managers",))
        }
        assert "quality_flags" in columns
    finally:
        conn.close()
