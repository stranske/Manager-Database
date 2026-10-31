"""Round 1842 follow-up: remaining dashboard/chat helper branches.

Coverage evidence (issue #1842 acceptance; head 514f4837)
---------------------------------------------------------
Hosted Gate (Python CI 3.13/3.14 on PR #1876) is the authoritative run; local
spot-check when Dropbox venv is usable:

  PYTHONPATH=. python3.12 -m pytest tests/test_coverage_round_1842_followup.py \\
    --cov=ui.dashboard --cov=api.chat --cov=api.managers --cov-report=term -q

Module ``--cov`` totals with only this file (local 2026-10-31): dashboard 11%,
``api/chat.py`` 30%, ``api/managers.py`` 18% (2377 stmts / 21% total). Pairing
with ``tests/test_coverage_round_1842.py`` raises dashboard/chat/managers to
44%/37%/23% (34% total) — the follow-up closes helper branches called out in
#1842 after the primary round-1842 chunk landed via #1843.

Per-test deliberate-break / revert (each FAIL when broken, PASS when restored):

* ``test_api_base_url_prefers_alerts_env`` — break: assert
  ``_api_base_url()`` ignores ``ALERTS_API_BASE_URL``; revert restores env
  precedence (alerts → API → localhost default).
* ``test_load_unacknowledged_alert_count_offline_mode`` — break: drop
  ``UI_OFFLINE`` short-circuit so ``httpx.Client`` is constructed; revert
  keeps offline guard and cache clear.
* ``test_render_active_campaigns_widget_empty_snapshot`` — break: expect a
  non-empty caption when the snapshot DataFrame is empty; revert restores the
  "No active materialized campaign snapshot" caption paths.
* ``test_chat_normalizers_and_chain_context_name_resolution`` — break: return
  ``None`` from ``_parse_date_range`` for valid ISO pairs; revert restores list
  and dict date parsing plus SQLite manager-name resolution.
* ``test_universe_schema_quality_flags_on_partial_legacy_sqlite_table`` — break:
  skip ``_ensure_universe_schema``; revert adds ``quality_flags`` to partial
  legacy ``managers`` tables.
"""

from __future__ import annotations

import sqlite3
from datetime import date

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

    def fail_client(*_args, **_kwargs):
        raise AssertionError("httpx.Client should not be constructed in offline mode")

    monkeypatch.setattr(dashboard.httpx, "Client", fail_client)
    try:
        assert dashboard.load_unacknowledged_alert_count() == 0
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
