"""Focused regression coverage for autopilot round 1842's ranked gaps."""

import asyncio
from decimal import Decimal

import pandas as pd
import pytest

from api import chat, managers
from etl import daily_diff_flow
from ui import dashboard


class _DashboardStreamlit:
    def __init__(self, selected_subject=None):
        self.selected_subject = selected_subject
        self.infos = []
        self.subheaders = []

    def info(self, value):
        self.infos.append(value)

    def subheader(self, value):
        self.subheaders.append(value)

    def selectbox(self, _label, options, **_kwargs):
        return self.selected_subject if self.selected_subject is not None else options[0]


def test_daily_diff_preserves_decimal_alert_values_and_utc_on_missing_localtime(monkeypatch):
    event = daily_diff_flow._large_delta_event_from_diff(
        {
            "manager_id": 7,
            "report_date": "2026-10-19",
            "cusip": "ABC",
            "delta_type": "ADD",
            "shares_prev": 1,
            "shares_curr": 2,
            "value_prev": Decimal("3.50"),
            "value_curr": Decimal("10.25"),
        }
    )

    assert event.payload["value_prev"] == 3.5
    assert event.payload["value_curr"] == 10.25
    assert event.payload["value_usd"] == 6.75

    class _NoZoneDateTime:
        @classmethod
        def now(cls):
            return cls()

        def astimezone(self):
            return self

        tzinfo = None

    monkeypatch.delenv("TZ", raising=False)
    monkeypatch.setattr(daily_diff_flow.dt, "datetime", _NoZoneDateTime)
    monkeypatch.setattr(
        daily_diff_flow.os.path,
        "realpath",
        lambda _path: (_ for _ in ()).throw(FileNotFoundError()),
    )
    assert daily_diff_flow._resolve_local_timezone() == "UTC"


def test_manager_schema_and_stats_cover_legacy_quality_and_jurisdiction_paths():
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    managers._ensure_manager_table(conn)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(managers)")}
    assert "quality_flags" in columns

    stats = managers._build_manager_stats([(None, None, "[]", "[]", " US ")])
    assert stats.by_jurisdiction == {"us": 1}


def test_chat_payload_and_dependency_error_branches_are_stable():
    filing = chat._format_filing_summary_payload(
        {
            "manager_name": "Alpha",
            "filing_date": "2026-01-01",
            "total_positions": 2,
            "key_positions": [{"issuer": "Issuer A"}],
            "notable_changes": ["Added ABC"],
            "risk_flags": ["Concentrated"],
        }
    )
    holdings = chat._format_holdings_analysis_payload(
        {
            "thesis": "Watch concentration",
            "top_positions": [{"cusip": "ABC"}],
            "period_changes": [{"summary": "Trimmed XYZ"}],
            "concentration_metrics": {"top_10": 80},
        }
    )
    assert "Top positions: Issuer A" in filing["answer"]
    assert "Notable changes: Added ABC" in filing["answer"]
    assert "Risk flags: Concentrated" in filing["answer"]
    assert "Top positions: ABC" in holdings["answer"]
    assert "Period changes: Trimmed XYZ" in holdings["answer"]

    def timeout_check():
        raise TimeoutError

    breaker = chat.CircuitBreaker(failure_threshold=1)
    payload, reason = asyncio.run(
        chat._run_dependency_check(
            timeout_check, 0.01, circuit_breaker=breaker, include_enabled=True
        )
    )
    assert payload["healthy"] is False
    assert payload["enabled"] is True
    assert reason == "timeout"


def test_dashboard_empty_states_and_presentation_helpers(monkeypatch):
    fake_st = _DashboardStreamlit()
    monkeypatch.setattr(dashboard, "st", fake_st)
    empty = pd.DataFrame()
    monkeypatch.setattr(dashboard, "load_filing_timeline", lambda _id: empty)
    monkeypatch.setattr(dashboard, "load_latest_holdings_snapshot", lambda _id: empty)
    monkeypatch.setattr(dashboard, "load_top_deltas", lambda _id: empty)
    monkeypatch.setattr(dashboard, "load_news_stream", lambda _id, limit: empty)
    monkeypatch.setattr(dashboard, "load_manager_activism_timeline", lambda _id: empty)
    monkeypatch.setattr(dashboard, "load_manager_activism_filings", lambda _id: empty)
    monkeypatch.setattr(dashboard, "load_manager_conviction_scores", lambda _id, limit: empty)
    monkeypatch.setattr(dashboard, "load_manager_crowded_trades", lambda _id: empty)

    dashboard.render_filing_timeline(None, show_heading=False)
    dashboard.render_filing_timeline(1, show_heading=False)
    dashboard.render_latest_holdings_snapshot(None, show_heading=False)
    dashboard.render_latest_holdings_snapshot(1, show_heading=False)
    dashboard.render_top_deltas(None, show_heading=False)
    dashboard.render_top_deltas(1, show_heading=False)
    dashboard.render_news_stream(1, show_heading=False)
    dashboard.render_activism_timeline(1, show_heading=False)
    dashboard.render_current_activism_stakes(1, show_heading=False)
    dashboard.render_top_convictions(1, show_heading=False)
    dashboard.render_manager_crowded_trades(1, show_heading=False)

    assert fake_st.infos == [
        "Select a manager to view filing timeline details.",
        "No filings found for the selected manager.",
        "Select a manager to view the latest holdings snapshot.",
        "No holdings found for the selected manager.",
        "Select a manager to view top position changes.",
        "No daily deltas found for the selected manager.",
        "No recent news found for the selected manager.",
        "No activism filings or events found for the selected manager.",
        "No activism stakes found for the selected manager.",
        "No conviction scores found for the selected manager.",
        "No crowded trades overlap found for the selected manager.",
    ]
    assert dashboard._delta_type_color("ADD").startswith("color: #198754")
    assert dashboard._delta_type_color("EXIT").startswith("color: #C1121F")
    assert dashboard._delta_type_color("other") == "color: #6C757D"
    assert dashboard._topic_badges(None) == ""
    assert dashboard._topic_badges("alpha, beta") == "`alpha` `beta`"
    assert dashboard._event_color("form_upgrade") == "#F97316"
    assert dashboard._event_color("initial_stake") == "#2563EB"


@pytest.mark.parametrize(
    "filings, selected, expected",
    [
        (pd.DataFrame(), None, "No activism ownership history found for the selected manager."),
        (
            pd.DataFrame(
                [{"filed_date": "not a date", "ownership_pct": 2, "subject_company": "A"}]
            ),
            None,
            "No plottable activism ownership history found.",
        ),
        (
            pd.DataFrame(
                [{"filed_date": "2026-01-01", "ownership_pct": 2, "subject_company": " "}]
            ),
            None,
            "No subject companies available for activism charting.",
        ),
        (
            pd.DataFrame(
                [{"filed_date": "2026-01-01", "ownership_pct": 2, "subject_company": "A"}]
            ),
            "missing",
            "No activism history for the selected subject company.",
        ),
    ],
)
def test_dashboard_ownership_chart_reports_each_unplottable_state(
    monkeypatch, filings, selected, expected
):
    fake_st = _DashboardStreamlit(selected_subject=selected)
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "load_manager_activism_filings", lambda _id: filings)

    dashboard.render_ownership_chart(1, show_heading=False)

    assert fake_st.infos == [expected]
