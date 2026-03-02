import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter

import pandas as pd
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ui import dashboard
from ui.dashboard import (
    load_all_managers_summary,
    load_delta,
    load_filing_timeline,
    load_latest_holdings_snapshot,
    load_managers,
    load_news_stream,
    load_qc_flags,
    load_top_deltas,
    load_unacknowledged_alert_count,
    main,
    render_all_managers_summary,
    render_filing_timeline,
    render_historical_filing_trend,
    render_latest_holdings_snapshot,
    render_manager_dashboard,
    render_manager_selector,
    render_news_stream,
    render_qc_flags,
    render_top_deltas,
)


def setup_db(tmp_path: Path) -> str:
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute(
        "CREATE TABLE holdings (cik TEXT, accession TEXT, filed DATE, nameOfIssuer TEXT, "
        "cusip TEXT, value INTEGER, sshPrnamt INTEGER, filing_id INTEGER, "
        "name_of_issuer TEXT, shares INTEGER, value_usd REAL)"
    )
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, type TEXT, filed_date DATE, period_end DATE, source TEXT, raw_key TEXT)"
    )
    conn.execute(
        "CREATE TABLE daily_diffs ("
        "manager_id INTEGER, report_date DATE, cusip TEXT, name_of_issuer TEXT, "
        "delta_type TEXT, shares_prev REAL, shares_curr REAL, value_prev REAL, value_curr REAL)"
    )
    conn.execute(
        "CREATE TABLE news_items ("
        "manager_id INTEGER, headline TEXT, url TEXT, published_at DATETIME, "
        "source TEXT, topics TEXT, confidence REAL)"
    )
    conn.execute(
        "CREATE TABLE api_usage ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, ts DATETIME, source TEXT, endpoint TEXT, "
        "status INT, bytes INT, latency_ms INT, cost_usd REAL)"
    )
    manager_rows = [
        (2, "Zulu Capital"),
        (1, "Alpha Partners"),
    ]
    rows = [
        ("0", "a", "2024-01-01", "CorpA", "AAA", 1, 1, 1, "Issuer A", 100, 1500),
        ("0", "b", "2024-01-02", "CorpB", "BBB", 1, 1, 1, "Issuer B", 200, 3000),
        ("0", "c", "2024-01-02", "CorpC", "CCC", 1, 1, 2, "Issuer C", 50, 700),
    ]
    filing_rows = [
        (1, 1, "13F-HR", "2024-03-15", "2023-12-31", "sec", "raw/1"),
        (2, 1, "13F-HR/A", "2024-02-15", "2023-12-31", "sec", "raw/2"),
        (3, 2, "13F-HR", "2024-01-15", "2023-12-31", "sec", "raw/3"),
    ]
    delta_rows = [
        (1, "2024-03-15", "BBB", "Issuer B", "INCREASE", 100, 200, 1000, 4000),
        (1, "2024-03-15", "AAA", "Issuer A", "DECREASE", 300, 100, 5000, 1200),
        (1, "2024-02-15", "CCC", "Issuer C", "ADD", 0, 50, 0, 700),
        (2, "2024-03-15", "ZZZ", "Issuer Z", "EXIT", 120, 0, 1800, 0),
    ]
    news_rows = [
        (
            1,
            "Issuer B expands international footprint",
            "https://example.com/issuer-b",
            "2024-03-16 08:00:00",
            "MarketWire",
            "strategy,expansion",
            0.92,
        ),
        (
            1,
            "Issuer A announces restructuring",
            "https://example.com/issuer-a",
            "2024-03-15 10:30:00",
            "SEC Feed",
            "governance,filing",
            0.88,
        ),
        (
            2,
            "Issuer Z exits position",
            "https://example.com/issuer-z",
            "2024-03-14 09:15:00",
            "MarketWire",
            "portfolio",
            0.75,
        ),
    ]
    usage_rows = [
        ("2024-03-20 12:30:00", "etl", "/edgar", 200, 1024, 150, 0.0),
    ]
    conn.executemany("INSERT INTO managers VALUES (?,?)", manager_rows)
    conn.executemany("INSERT INTO holdings VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.executemany("INSERT INTO filings VALUES (?,?,?,?,?,?,?)", filing_rows)
    conn.executemany("INSERT INTO daily_diffs VALUES (?,?,?,?,?,?,?,?,?)", delta_rows)
    conn.executemany("INSERT INTO news_items VALUES (?,?,?,?,?,?,?)", news_rows)
    conn.executemany(
        "INSERT INTO api_usage(ts, source, endpoint, status, bytes, latency_ms, cost_usd) "
        "VALUES (?,?,?,?,?,?,?)",
        usage_rows,
    )
    conn.commit()
    conn.close()
    return str(db_path)


def setup_performance_db(tmp_path: Path, manager_count: int = 10) -> str:
    db_path = tmp_path / "perf.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute(
        "CREATE TABLE holdings (cik TEXT, accession TEXT, filed DATE, nameOfIssuer TEXT, "
        "cusip TEXT, value INTEGER, sshPrnamt INTEGER, filing_id INTEGER, "
        "name_of_issuer TEXT, shares INTEGER, value_usd REAL)"
    )
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, type TEXT, filed_date DATE, period_end DATE, source TEXT, raw_key TEXT)"
    )
    conn.execute(
        "CREATE TABLE daily_diffs ("
        "manager_id INTEGER, report_date DATE, cusip TEXT, name_of_issuer TEXT, "
        "delta_type TEXT, shares_prev REAL, shares_curr REAL, value_prev REAL, value_curr REAL)"
    )
    conn.execute(
        "CREATE TABLE news_items ("
        "manager_id INTEGER, headline TEXT, url TEXT, published_at DATETIME, "
        "source TEXT, topics TEXT, confidence REAL)"
    )
    conn.execute(
        "CREATE TABLE api_usage ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, ts DATETIME, source TEXT, endpoint TEXT, "
        "status INT, bytes INT, latency_ms INT, cost_usd REAL)"
    )

    manager_rows = [
        (manager_id, f"Manager {manager_id:02d}") for manager_id in range(1, manager_count + 1)
    ]
    conn.executemany("INSERT INTO managers VALUES (?,?)", manager_rows)

    filing_id = 1
    for manager_id in range(1, manager_count + 1):
        filing_rows = [
            (
                filing_id,
                manager_id,
                "13F-HR",
                "2026-02-14",
                "2025-12-31",
                "sec",
                f"raw/{filing_id}",
            ),
            (
                filing_id + 1,
                manager_id,
                "13F-HR/A",
                "2025-11-14",
                "2025-09-30",
                "sec",
                f"raw/{filing_id + 1}",
            ),
        ]
        conn.executemany("INSERT INTO filings VALUES (?,?,?,?,?,?,?)", filing_rows)

        holdings_rows = []
        for idx in range(1, 11):
            holdings_rows.append(
                (
                    str(manager_id),
                    f"a{filing_id}-{idx}",
                    "2026-02-14",
                    f"Corp {manager_id}-{idx}",
                    f"{manager_id:02d}{idx:04d}",
                    idx * 100,
                    idx * 10,
                    filing_id,
                    f"Issuer {manager_id}-{idx}",
                    idx * 100,
                    float(idx * 100000),
                )
            )
        conn.executemany("INSERT INTO holdings VALUES (?,?,?,?,?,?,?,?,?,?,?)", holdings_rows)

        diff_rows = []
        for idx in range(1, 11):
            diff_rows.append(
                (
                    manager_id,
                    "2026-02-14",
                    f"{manager_id:02d}{idx:04d}",
                    f"Issuer {manager_id}-{idx}",
                    "INCREASE" if idx % 2 else "DECREASE",
                    idx * 50,
                    idx * 100,
                    float(idx * 50000),
                    float(idx * 110000),
                )
            )
        conn.executemany("INSERT INTO daily_diffs VALUES (?,?,?,?,?,?,?,?,?)", diff_rows)

        news_rows = [
            (
                manager_id,
                f"Manager {manager_id:02d} portfolio update",
                f"https://example.com/m{manager_id}/update",
                "2026-02-20 10:00:00",
                "Newswire",
                "portfolio,filing",
                0.9,
            ),
            (
                manager_id,
                f"Manager {manager_id:02d} adds positions",
                f"https://example.com/m{manager_id}/adds",
                "2026-02-18 09:00:00",
                "Newswire",
                "positions,analysis",
                0.85,
            ),
        ]
        conn.executemany("INSERT INTO news_items VALUES (?,?,?,?,?,?,?)", news_rows)

        filing_id += 2

    conn.execute(
        "INSERT INTO api_usage(ts, source, endpoint, status, bytes, latency_ms, cost_usd) "
        "VALUES (?,?,?,?,?,?,?)",
        ("2026-02-20 12:30:00", "etl", "/edgar", 200, 2048, 150, 0.0),
    )
    conn.commit()
    conn.close()
    return str(db_path)


def test_load_delta_counts(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    df = load_delta()
    assert list(df["date"]) == ["2024-01-01", "2024-01-02"]
    assert list(df["filings"]) == [1, 2]


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    def __init__(self, response_payload: dict, requested_urls: list[str]):
        self._response_payload = response_payload
        self._requested_urls = requested_urls

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url: str):
        self._requested_urls.append(url)
        return _FakeResponse(self._response_payload)


def test_load_managers_sorted(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    st.cache_data.clear()
    df = load_managers()
    assert list(df["name"]) == ["Alpha Partners", "Zulu Capital"]
    assert list(df["manager_id"]) == [1, 2]


class FakeStreamlit:
    def __init__(self):
        self.session_state = {}

    def selectbox(self, _label, options, index, format_func, key):
        self.session_state.setdefault(key, options[index])
        return self.session_state[key]


def test_render_manager_selector_default_and_persist(monkeypatch):
    fake_st = FakeStreamlit()
    monkeypatch.setattr("ui.dashboard.st", fake_st)
    monkeypatch.setattr(
        "ui.dashboard.load_managers",
        lambda: pd.DataFrame(
            [
                {"manager_id": 1, "name": "Alpha Partners"},
                {"manager_id": 2, "name": "Zulu Capital"},
            ]
        ),
    )

    selected = render_manager_selector()
    assert selected is None
    assert fake_st.session_state["selected_manager_id"] == "all"

    fake_st.session_state["selected_manager_id"] = 2
    selected = render_manager_selector()
    assert selected == 2


def test_load_filing_timeline_filters_and_orders(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    df = load_filing_timeline(1)
    assert list(df["filing_id"]) == [1, 2]
    assert list(df["type"]) == ["13F-HR", "13F-HR/A"]


def test_load_latest_holdings_snapshot_filters_and_orders(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    df = load_latest_holdings_snapshot(1)
    assert list(df["name_of_issuer"]) == ["Issuer B", "Issuer A", "Issuer C"]
    assert list(df["value_usd"]) == [3000.0, 1500.0, 700.0]


def test_load_top_deltas_filters_latest_report_date_and_orders(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    df = load_top_deltas(1)
    assert list(df["cusip"]) == ["AAA", "BBB"]
    assert list(df["delta_type"]) == ["DECREASE", "INCREASE"]


def test_load_news_stream_filters_and_orders(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    df = load_news_stream(1)
    assert list(df["headline"]) == [
        "Issuer B expands international footprint",
        "Issuer A announces restructuring",
    ]
    assert list(df["source"]) == ["MarketWire", "SEC Feed"]


def test_load_qc_flags_returns_expected_summary(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    qc = load_qc_flags(1)
    assert str(qc["last_filing_date"].date()) == "2024-03-15"
    assert qc["is_13f_filer"] is True
    assert qc["latest_holdings_count"] == 2
    assert qc["news_count_30d"] == 0
    assert str(qc["last_etl_run"]) == "2024-03-20 12:30:00"


def test_load_all_managers_summary_aggregates_totals_and_activity(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    summary = load_all_managers_summary()
    assert summary["total_managers"] == 2
    assert summary["total_filings"] == 3
    assert summary["total_holdings"] == 3
    assert summary["total_news_items"] == 3
    activity = summary["recent_activity"]
    assert not activity.empty
    assert list(activity.columns) == ["activity_date", "filings", "holdings", "news_items"]
    assert int(activity["filings"].sum()) == 3
    assert int(activity["holdings"].sum()) == 3
    assert int(activity["news_items"].sum()) == 3


def test_load_all_managers_summary_identifies_stale_manager_warnings(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    summary = load_all_managers_summary()
    stale = summary["stale_managers"]
    assert not stale.empty
    assert set(stale["name"]) == {"Alpha Partners", "Zulu Capital"}
    assert stale["warning"].str.contains("13F filing stale").all()


class TimelineStreamlit:
    def __init__(self):
        self.subheaders = []
        self.charts = []
        self.tables = []
        self.info_calls = []

    def subheader(self, text):
        self.subheaders.append(text)

    def info(self, text):
        self.info_calls.append(text)

    def altair_chart(self, chart, use_container_width):
        self.charts.append((chart, use_container_width))

    def dataframe(self, df, use_container_width):
        self.tables.append((df.copy(), use_container_width))


def test_render_filing_timeline_outputs_chart_and_table(monkeypatch):
    fake_st = TimelineStreamlit()
    monkeypatch.setattr("ui.dashboard.st", fake_st)
    monkeypatch.setattr(
        "ui.dashboard.load_filing_timeline",
        lambda manager_id: pd.DataFrame(
            [
                {
                    "filing_id": 11,
                    "type": "13F-HR",
                    "filed_date": "2024-03-01",
                    "period_end": "2023-12-31",
                    "source": "sec",
                    "raw_key": "raw/11",
                }
            ]
        ),
    )

    render_filing_timeline(1)
    assert fake_st.subheaders == ["Filing Timeline"]
    assert len(fake_st.charts) == 1
    assert len(fake_st.tables) == 1
    assert fake_st.info_calls == []


class MetricColumn:
    def __init__(self):
        self.metrics = []

    def metric(self, label, value):
        self.metrics.append((label, value))


class SnapshotStreamlit:
    def __init__(self):
        self.subheaders = []
        self.info_calls = []
        self.tables = []
        self.columns_args = []
        self.columns_objects = []

    def subheader(self, text):
        self.subheaders.append(text)

    def info(self, text):
        self.info_calls.append(text)

    def columns(self, n):
        self.columns_args.append(n)
        cols = [MetricColumn() for _ in range(n)]
        self.columns_objects.append(cols)
        return cols

    def dataframe(self, df, use_container_width):
        self.tables.append((df.copy(), use_container_width))


def test_render_latest_holdings_snapshot_outputs_metrics_and_table(monkeypatch):
    fake_st = SnapshotStreamlit()
    monkeypatch.setattr("ui.dashboard.st", fake_st)
    monkeypatch.setattr(
        "ui.dashboard.load_latest_holdings_snapshot",
        lambda manager_id: pd.DataFrame(
            [
                {
                    "name_of_issuer": "Issuer B",
                    "cusip": "BBB",
                    "shares": 200,
                    "value_usd": 3000,
                },
                {
                    "name_of_issuer": "Issuer A",
                    "cusip": "AAA",
                    "shares": 100,
                    "value_usd": 1500,
                },
            ]
        ),
    )

    render_latest_holdings_snapshot(1)
    assert fake_st.subheaders == ["Latest Holdings Snapshot"]
    assert fake_st.info_calls == []
    assert fake_st.columns_args == [2]
    assert len(fake_st.tables) == 1
    assert fake_st.columns_objects[0][0].metrics == [("Total Positions", "2")]
    assert fake_st.columns_objects[0][1].metrics == [("Total AUM (USD)", "$4,500")]


class TopDeltasStreamlit:
    def __init__(self):
        self.subheaders = []
        self.info_calls = []
        self.charts = []
        self.tables = []

    def subheader(self, text):
        self.subheaders.append(text)

    def info(self, text):
        self.info_calls.append(text)

    def altair_chart(self, chart, use_container_width):
        self.charts.append((chart, use_container_width))

    def dataframe(self, df, use_container_width):
        self.tables.append((df, use_container_width))


def test_render_top_deltas_outputs_chart_and_table(monkeypatch):
    fake_st = TopDeltasStreamlit()
    monkeypatch.setattr("ui.dashboard.st", fake_st)
    monkeypatch.setattr(
        "ui.dashboard.load_top_deltas",
        lambda manager_id: pd.DataFrame(
            [
                {
                    "cusip": "BBB",
                    "name_of_issuer": "Issuer B",
                    "delta_type": "INCREASE",
                    "shares_prev": 100,
                    "shares_curr": 200,
                    "value_prev": 1000,
                    "value_curr": 4000,
                },
                {
                    "cusip": "AAA",
                    "name_of_issuer": "Issuer A",
                    "delta_type": "DECREASE",
                    "shares_prev": 300,
                    "shares_curr": 100,
                    "value_prev": 5000,
                    "value_curr": 1200,
                },
            ]
        ),
    )

    render_top_deltas(1)
    assert fake_st.subheaders == ["Top Deltas"]
    assert fake_st.info_calls == []
    assert len(fake_st.charts) == 1
    assert len(fake_st.tables) == 1


class NewsStreamStreamlit:
    def __init__(self):
        self.subheaders = []
        self.info_calls = []
        self.markdowns = []
        self.captions = []

    def subheader(self, text):
        self.subheaders.append(text)

    def info(self, text):
        self.info_calls.append(text)

    def markdown(self, text):
        self.markdowns.append(text)

    def caption(self, text):
        self.captions.append(text)


def test_render_news_stream_outputs_links_timestamps_and_topics(monkeypatch):
    fake_st = NewsStreamStreamlit()
    monkeypatch.setattr("ui.dashboard.st", fake_st)
    monkeypatch.setattr(
        "ui.dashboard.load_news_stream",
        lambda manager_id: pd.DataFrame(
            [
                {
                    "headline": "Issuer B expands international footprint",
                    "url": "https://example.com/issuer-b",
                    "published_at": "2024-03-16 08:00:00",
                    "source": "MarketWire",
                    "topics": "strategy,expansion",
                    "confidence": 0.92,
                }
            ]
        ),
    )

    render_news_stream(1)
    assert fake_st.subheaders == ["News Stream"]
    assert fake_st.info_calls == []
    assert fake_st.markdowns == [
        "- [Issuer B expands international footprint](https://example.com/issuer-b)"
    ]
    assert fake_st.captions == [
        "2024-03-16 08:00 | MarketWire | confidence 0.92 `strategy` `expansion`"
    ]


class QCMetricColumn:
    def __init__(self):
        self.metrics = []

    def metric(self, label, value, delta=None, delta_color="normal"):
        self.metrics.append(
            {
                "label": label,
                "value": value,
                "delta": delta,
                "delta_color": delta_color,
            }
        )


class QCFlagsStreamlit:
    def __init__(self):
        self.subheaders = []
        self.info_calls = []
        self.columns_args = []
        self.columns_objects = []

    def subheader(self, text):
        self.subheaders.append(text)

    def info(self, text):
        self.info_calls.append(text)

    def columns(self, n):
        self.columns_args.append(n)
        cols = [QCMetricColumn() for _ in range(n)]
        self.columns_objects.append(cols)
        return cols


def test_render_qc_flags_outputs_metrics(monkeypatch):
    fake_st = QCFlagsStreamlit()
    monkeypatch.setattr("ui.dashboard.st", fake_st)
    monkeypatch.setattr(
        "ui.dashboard.load_qc_flags",
        lambda manager_id: {
            "last_filing_date": pd.Timestamp("2025-07-01"),
            "is_13f_filer": True,
            "latest_holdings_count": 0,
            "news_count_30d": 3,
            "last_etl_run": datetime.now(UTC) - timedelta(hours=30),
        },
    )

    render_qc_flags(1)
    assert fake_st.subheaders == ["QC Flags"]
    assert fake_st.info_calls == []
    assert fake_st.columns_args == [4]
    labels = [m["label"] for col in fake_st.columns_objects[0] for m in col.metrics]
    assert labels == [
        "Last Filing Date",
        "Holdings in Latest Filing",
        "News Items (30d)",
        "Data Freshness (ETL)",
    ]
    deltas = [m["delta"] for col in fake_st.columns_objects[0] for m in col.metrics]
    assert "+1 empty filing warning" in deltas


def test_render_qc_flags_requires_manager_selection(monkeypatch):
    fake_st = QCFlagsStreamlit()
    monkeypatch.setattr("ui.dashboard.st", fake_st)
    render_qc_flags(None)
    assert fake_st.subheaders == ["QC Flags"]
    assert fake_st.info_calls == ["Select a manager to view data quality flags."]


class LayoutColumn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class LayoutExpander:
    def __init__(self, st_obj, label: str, expanded: bool):
        self._st = st_obj
        self._label = label
        self._expanded = expanded

    def __enter__(self):
        self._st.expander_calls.append((self._label, self._expanded))
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class LayoutStreamlit:
    def __init__(self):
        self.columns_calls = []
        self.expander_calls = []

    def columns(self, spec, gap=None):
        self.columns_calls.append((spec, gap))
        return [LayoutColumn(), LayoutColumn()]

    def expander(self, label, expanded=False):
        return LayoutExpander(self, label, expanded)


def test_render_manager_dashboard_uses_columns_and_expanders(monkeypatch):
    fake_st = LayoutStreamlit()
    monkeypatch.setattr("ui.dashboard.st", fake_st)

    calls = []

    def _recorder(section_name):
        def _inner(manager_id, show_heading=True):
            calls.append((section_name, manager_id, show_heading))

        return _inner

    monkeypatch.setattr("ui.dashboard.render_filing_timeline", _recorder("filing_timeline"))
    monkeypatch.setattr(
        "ui.dashboard.render_latest_holdings_snapshot", _recorder("holdings_snapshot")
    )
    monkeypatch.setattr("ui.dashboard.render_top_deltas", _recorder("top_deltas"))
    monkeypatch.setattr("ui.dashboard.render_news_stream", _recorder("news_stream"))
    monkeypatch.setattr("ui.dashboard.render_qc_flags", _recorder("qc_flags"))

    render_manager_dashboard(7)

    assert fake_st.columns_calls == [((3, 2), "large")]
    assert fake_st.expander_calls == [
        ("Filing Timeline", True),
        ("Latest Holdings Snapshot", True),
        ("Top Deltas", True),
        ("News Stream", True),
        ("QC Flags", True),
    ]
    assert calls == [
        ("filing_timeline", 7, False),
        ("holdings_snapshot", 7, False),
        ("top_deltas", 7, False),
        ("news_stream", 7, False),
        ("qc_flags", 7, False),
    ]


class PerfColumn:
    def metric(self, *_args, **_kwargs):
        return None

    def caption(self, *_args, **_kwargs):
        return None

    def altair_chart(self, *_args, **_kwargs):
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_load_unacknowledged_alert_count_uses_alerts_endpoint(monkeypatch):
    requested_urls: list[str] = []
    monkeypatch.setenv("ALERTS_API_BASE_URL", "http://alerts.local")
    monkeypatch.setattr(
        dashboard,
        "httpx",
        type(
            "FakeHttpx",
            (),
            {"Client": lambda **kwargs: _FakeClient({"count": 7}, requested_urls)},
        ),
    )
    load_unacknowledged_alert_count.clear()

    count = load_unacknowledged_alert_count()

    assert count == 7
    assert requested_urls == ["http://alerts.local/api/alerts/unacknowledged/count"]


class _FakeSidebar:
    def __init__(self):
        self.markdowns: list[str] = []
        self.metrics: list[tuple[str, int]] = []

    def markdown(self, text: str, unsafe_allow_html: bool = False) -> None:
        self.markdowns.append(text)

    def metric(self, label: str, value: int) -> None:
        self.metrics.append((label, value))


class _FakeExpander:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeStreamlit:
    def __init__(self):
        self.sidebar = _FakeSidebar()
        self.headers: list[str] = []
        self.infos: list[str] = []

    def header(self, text: str) -> None:
        self.headers.append(text)

    def info(self, text: str) -> None:
        self.infos.append(text)

    def expander(self, *_args, **_kwargs):
        return _FakeExpander()

    def selectbox(self, *_args, **_kwargs):
        return "all"


def test_main_renders_alert_badge_and_metric(monkeypatch):
    fake_st = _FakeStreamlit()
    monkeypatch.setattr(dashboard, "st", fake_st)
    monkeypatch.setattr(dashboard, "require_login", lambda: True)
    monkeypatch.setattr(dashboard, "load_unacknowledged_alert_count", lambda: 3)
    monkeypatch.setattr(dashboard, "render_manager_selector", lambda: None)
    monkeypatch.setattr(dashboard, "render_all_managers_summary", lambda show_heading=True: None)
    monkeypatch.setattr(dashboard, "render_historical_filing_trend", lambda: None)

    main()

    assert fake_st.sidebar.markdowns[0] == "### Navigation"
    assert "Alerts" in fake_st.sidebar.markdowns[1]
    assert ">3<" in fake_st.sidebar.markdowns[1]
    assert fake_st.sidebar.metrics == [("Unacknowledged Alerts", 3)]
    assert fake_st.headers == ["Holdings Delta"]


class PerfExpander:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class PerfStreamlit:
    def __init__(self):
        self.session_state = {}

    def header(self, *_args, **_kwargs):
        return None

    def selectbox(self, _label, options, index, format_func, key):
        self.session_state.setdefault(key, options[index])
        return self.session_state[key]

    def columns(self, spec, gap=None):
        if isinstance(spec, int):
            return [PerfColumn() for _ in range(spec)]
        return [PerfColumn() for _ in range(len(spec))]

    def expander(self, *_args, **_kwargs):
        return PerfExpander()

    def subheader(self, *_args, **_kwargs):
        return None

    def info(self, *_args, **_kwargs):
        return None

    def altair_chart(self, *_args, **_kwargs):
        return None

    def dataframe(self, *_args, **_kwargs):
        return None

    def markdown(self, *_args, **_kwargs):
        return None

    def caption(self, *_args, **_kwargs):
        return None

    def metric(self, *_args, **_kwargs):
        return None

    def stop(self):
        raise AssertionError("stop() should not be called during dashboard performance test")


def test_dashboard_render_under_two_seconds_with_ten_managers(tmp_path: Path, monkeypatch):
    db_path = setup_performance_db(tmp_path, manager_count=10)
    monkeypatch.setenv("DB_PATH", db_path)
    st.cache_data.clear()

    fake_st = PerfStreamlit()
    monkeypatch.setattr("ui.dashboard.st", fake_st)
    monkeypatch.setattr("ui.dashboard.require_login", lambda: True)

    start = perf_counter()
    selected_manager = render_manager_selector()
    assert selected_manager is None
    render_all_managers_summary(show_heading=False)
    fake_st.session_state["selected_manager_id"] = 1
    render_manager_dashboard(1)
    render_historical_filing_trend()
    elapsed = perf_counter() - start

    assert elapsed < 2.0, f"Dashboard render path exceeded 2 seconds: {elapsed:.3f}s"
