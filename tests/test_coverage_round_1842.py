"""Focused regression coverage for autopilot round 1842's ranked gaps."""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from api import chat, managers
from etl import daily_diff_flow
from ui import dashboard


class _NullContext:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def __getattr__(self, _name):
        return lambda *args, **kwargs: None


class _DashboardStreamlit:
    def __init__(self, selected_subject=None, selected_manager="all", *, has_tabs=True):
        self.selected_subject = selected_subject
        self.selected_manager = selected_manager
        self._has_tabs = has_tabs
        self.infos = []
        self.subheaders = []
        self.markdowns = []
        self.captions = []
        self.metrics = []
        self.tables = []
        self.charts = []
        self.dividers = 0
        self.session_state = {}
        self.stopped = False
        if has_tabs:
            self.tabs = lambda labels: [_NullContext() for _ in labels]

    def info(self, value):
        self.infos.append(value)

    def subheader(self, value):
        self.subheaders.append(value)

    def markdown(self, value, **_kwargs):
        self.markdowns.append(value)

    def caption(self, value):
        self.captions.append(value)

    def metric(self, label, value, delta=None, **_kwargs):
        self.metrics.append((label, value, delta))

    def dataframe(self, frame, **_kwargs):
        self.tables.append(frame.copy())

    def altair_chart(self, chart, **_kwargs):
        self.charts.append(chart)

    def divider(self):
        self.dividers += 1

    def selectbox(self, label, options, **kwargs):
        options = list(options)
        if "Ownership history subject" in str(label):
            return self.selected_subject if self.selected_subject is not None else options[0]
        key = kwargs.get("key")
        if key:
            self.session_state.setdefault(key, self.selected_manager)
            return self.session_state[key]
        return options[0]

    def columns(self, spec=2, *_args, **_kwargs):
        if isinstance(spec, int):
            count = spec
        elif isinstance(spec, (tuple, list)):
            count = len(spec)
        else:
            count = 2
        return [_NullContext() for _ in range(count)]

    @contextmanager
    def expander(self, *_args, **_kwargs):
        yield

    def stop(self):
        self.stopped = True

    def __getattr__(self, name):
        if name == "tabs" and not self._has_tabs:
            raise AttributeError(name)
        return lambda *args, **kwargs: None


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


def test_manager_schema_and_stats_cover_legacy_quality_and_jurisdiction_paths(monkeypatch):
    conn = sqlite3.connect(":memory:")
    try:
        # Bypass the modern CREATE TABLE path so universe migration still adds quality_flags.
        monkeypatch.setattr(managers, "_ensure_manager_table", lambda _conn: None)
        conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        managers._ensure_universe_schema(conn)
        columns = {
            str(row[0]) for row in conn.execute(managers.SQLITE_TABLE_INFO_SQL, ("managers",))
        }
        assert "quality_flags" in columns

        stats = managers._build_manager_stats(
            [
                (None, None, "[]", "[]", " US "),
                (None, None, "[]", "[]", "   "),
                ("0000000001", "LEI", '["eu"]', '["activist"]', None),
            ]
        )
        assert stats.by_jurisdiction == {"eu": 1, "us": 1}
        assert stats.with_cik == 1
        assert stats.with_lei == 1
        assert stats.by_tag == {"activist": 1}
    finally:
        conn.close()


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

    empty_filing = chat._format_filing_summary_payload(
        {
            "manager_name": "Beta",
            "filing_date": "2026-02-01",
            "total_positions": 0,
            "key_positions": [{"issuer": ""}],
            "notable_changes": "not-a-list",
            "risk_flags": [],
        }
    )
    empty_holdings = chat._format_holdings_analysis_payload(
        {
            "thesis": "Sparse book",
            "top_positions": [{"cusip": ""}],
            "period_changes": "not-a-list",
            "concentration_metrics": [],
        }
    )
    assert "Top positions:" not in empty_filing["answer"]
    assert "Notable changes:" not in empty_filing["answer"]
    assert "Risk flags:" not in empty_filing["answer"]
    assert "Top positions:" not in empty_holdings["answer"]
    assert "Period changes:" not in empty_holdings["answer"]
    assert "Concentration metrics:" not in empty_holdings["answer"]

    assert chat._response_id_from_trace_url("https://trace.example/run/") == "run"
    assert chat._response_id_from_trace_url("no-slash-id") is None

    def timeout_check():
        raise TimeoutError

    def boom():
        raise RuntimeError("dependency down")

    breaker = chat.CircuitBreaker(failure_threshold=1)
    payload, reason = asyncio.run(
        chat._run_dependency_check(
            timeout_check, 0.01, circuit_breaker=breaker, include_enabled=True
        )
    )
    assert payload["healthy"] is False
    assert payload["enabled"] is True
    assert reason == "timeout"

    # Additional failure while already open must keep the breaker latched.
    breaker.record_failure()
    assert breaker.is_open()

    disabled, disabled_reason = asyncio.run(
        chat._run_dependency_check(timeout_check, 0.01, enabled=False, include_enabled=True)
    )
    assert disabled == {"healthy": True, "latency_ms": 0, "enabled": False}
    assert disabled_reason is None

    timed_out, timeout_reason = asyncio.run(chat._run_dependency_check(timeout_check, 0.01))
    assert timed_out["healthy"] is False
    assert timeout_reason == "timeout"

    failed, failed_reason = asyncio.run(chat._run_dependency_check(boom, 0.01))
    assert failed["healthy"] is False
    assert failed_reason == "dependency down"

    fallback = chat._FallbackHoldingsAnalysisChain().run("positions only")
    assert fallback == {"answer": "Holdings analysis: positions only", "sources": []}


def test_chat_classify_intent_falls_through_unknown_classifier_labels(monkeypatch):
    monkeypatch.setattr(
        chat.importlib,
        "import_module",
        lambda _name: SimpleNamespace(classify_intent=lambda _q: "not-a-direct-chain"),
    )
    assert chat._classify_intent("tell me something novel") == "rag_search"


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
    monkeypatch.setattr(dashboard, "load_manager_contrarian_signals", lambda _id: empty)

    dashboard.render_filing_timeline(None, show_heading=True)
    dashboard.render_filing_timeline(1, show_heading=False)
    dashboard.render_latest_holdings_snapshot(None, show_heading=False)
    dashboard.render_latest_holdings_snapshot(1, show_heading=False)
    dashboard.render_top_deltas(None, show_heading=False)
    dashboard.render_top_deltas(1, show_heading=False)
    dashboard.render_news_stream(1, show_heading=False)
    dashboard.render_activism_timeline(1, show_heading=True)
    dashboard.render_current_activism_stakes(1, show_heading=True)
    dashboard.render_top_convictions(1, show_heading=True)
    dashboard.render_manager_crowded_trades(1, show_heading=True)
    dashboard.render_contrarian_alerts(1, show_heading=True)
    dashboard.render_ownership_chart(1, show_heading=True)
    dashboard.render_activism_dashboard(1)
    dashboard.render_conviction_signals_dashboard(1)

    assert "Filing Timeline" in fake_st.subheaders
    assert "Activism Timeline" in fake_st.subheaders
    assert "Current Stakes" in fake_st.subheaders
    assert "Top Convictions" in fake_st.subheaders
    assert "Crowded Trades" in fake_st.subheaders
    assert "Contrarian Alerts" in fake_st.subheaders
    assert "Ownership Chart" in fake_st.subheaders
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
        "No contrarian signals found for the selected manager.",
        "No activism ownership history found for the selected manager.",
        "No activism filings or events found for the selected manager.",
        "No activism stakes found for the selected manager.",
        "No activism ownership history found for the selected manager.",
        "No conviction scores found for the selected manager.",
        "No crowded trades overlap found for the selected manager.",
        "No contrarian signals found for the selected manager.",
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


def test_dashboard_qc_and_selector_edge_paths(monkeypatch):
    fake_st = _DashboardStreamlit(selected_manager="all")
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(
        dashboard,
        "load_managers",
        lambda: pd.DataFrame([{"manager_id": 1, "name": "Alpha"}]),
    )
    assert dashboard.render_manager_selector() is None

    monkeypatch.setattr(
        dashboard,
        "load_managers",
        lambda: pd.DataFrame([{"manager_id": "legacy", "name": "Alpha"}]),
    )
    fake_st.session_state["selected_manager_id"] = "legacy"
    assert dashboard.render_manager_selector() is None

    class _MetricCol:
        def __init__(self):
            self.metrics = []

        def metric(self, label, value, delta=None, **_kwargs):
            self.metrics.append((label, value, delta))

    cols = [_MetricCol() for _ in range(4)]
    fake_st.columns = lambda *_a, **_k: cols
    monkeypatch.setattr(
        dashboard,
        "load_qc_flags",
        lambda _id: {
            "last_filing_date": pd.Timestamp("2026-09-01"),
            "is_13f_filer": False,
            "latest_holdings_count": 3,
            "news_count_30d": 1,
            "last_etl_run": datetime.now(UTC) - timedelta(hours=2),
        },
    )
    dashboard.render_qc_flags(1, show_heading=True)
    assert cols[0].metrics[0][2].startswith("-")
    assert cols[3].metrics[0][2].startswith("-")

    monkeypatch.setattr(
        dashboard,
        "load_qc_flags",
        lambda _id: {
            "last_filing_date": None,
            "is_13f_filer": True,
            "latest_holdings_count": 0,
            "news_count_30d": 0,
            "last_etl_run": None,
        },
    )
    cols = [_MetricCol() for _ in range(4)]
    fake_st.columns = lambda *_a, **_k: cols
    dashboard.render_qc_flags(1, show_heading=False)
    assert cols[0].metrics[0][1] == "N/A"
    assert cols[0].metrics[0][2] == "+1 missing filing date"


def test_dashboard_unplottable_convictions_and_tabless_manager_view(monkeypatch):
    fake_st = _DashboardStreamlit(has_tabs=False)
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(
        dashboard,
        "load_manager_conviction_scores",
        lambda _id, limit: pd.DataFrame(
            [{"name_of_issuer": "X", "cusip": "AAA", "conviction_pct": "n/a", "value_usd": 1}]
        ),
    )
    dashboard.render_top_convictions(1, show_heading=False)
    assert fake_st.infos == ["No conviction scores available for charting."]

    monkeypatch.setattr(dashboard, "load_filing_timeline", lambda _id: pd.DataFrame())
    monkeypatch.setattr(dashboard, "load_latest_holdings_snapshot", lambda _id: pd.DataFrame())
    monkeypatch.setattr(dashboard, "load_top_deltas", lambda _id: pd.DataFrame())
    monkeypatch.setattr(dashboard, "load_news_stream", lambda _id, limit=10: pd.DataFrame())
    monkeypatch.setattr(
        dashboard,
        "load_qc_flags",
        lambda _id: {
            "last_filing_date": None,
            "is_13f_filer": False,
            "latest_holdings_count": 0,
            "news_count_30d": 0,
            "last_etl_run": None,
        },
    )
    monkeypatch.setattr(
        dashboard, "load_manager_conviction_scores", lambda _id, limit: pd.DataFrame()
    )
    monkeypatch.setattr(dashboard, "load_manager_crowded_trades", lambda _id: pd.DataFrame())
    monkeypatch.setattr(dashboard, "load_manager_contrarian_signals", lambda _id: pd.DataFrame())
    dashboard.render_manager_dashboard(9)
    assert "No filings found for the selected manager." in fake_st.infos
    assert "No contrarian signals found for the selected manager." in fake_st.infos


def test_dashboard_news_stream_optional_meta_branches(monkeypatch):
    fake_st = _DashboardStreamlit()
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(
        dashboard,
        "load_news_stream",
        lambda _id, limit=10: pd.DataFrame(
            [
                {
                    "headline": "Alpha expands",
                    "url": "https://example.test/a",
                    "published_at": "2026-01-02T12:00:00",
                    "topics": "mna, activism",
                    "manager_name": "Alpha",
                },
                {
                    "headline": "",
                    "url": "",
                    "published_at": pd.NaT,
                    "topics": "",
                    "manager_name": "",
                },
            ]
        ),
    )
    dashboard.render_news_stream(None, show_heading=True)
    assert any("Alpha expands" in line for line in fake_st.markdowns)
    assert any(line.startswith("- Untitled") for line in fake_st.markdowns)


def test_fallback_holdings_chain_includes_optional_filters():
    result = chat._FallbackHoldingsAnalysisChain().run(
        "filter me",
        manager_ids=[1],
        cusips=["AAA"],
        date_range=(date(2026, 1, 1), date(2026, 1, 31)),
        as_of=datetime(2026, 1, 15, tzinfo=UTC),
    )
    assert result["sources"][0]["manager_ids"] == [1]
    assert result["sources"][0]["cusips"] == ["AAA"]
    assert result["sources"][0]["date_range"] == ["2026-01-01", "2026-01-31"]
    assert result["sources"][0]["as_of"].startswith("2026-01-15")
