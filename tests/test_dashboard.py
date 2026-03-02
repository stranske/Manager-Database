import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

from ui.dashboard import (
    load_delta,
    load_filing_timeline,
    load_latest_holdings_snapshot,
    load_managers,
    load_news_stream,
    load_top_deltas,
    render_filing_timeline,
    render_latest_holdings_snapshot,
    render_manager_selector,
    render_news_stream,
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
    conn.executemany("INSERT INTO managers VALUES (?,?)", manager_rows)
    conn.executemany("INSERT INTO holdings VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.executemany("INSERT INTO filings VALUES (?,?,?,?,?,?,?)", filing_rows)
    conn.executemany("INSERT INTO daily_diffs VALUES (?,?,?,?,?,?,?,?,?)", delta_rows)
    conn.executemany("INSERT INTO news_items VALUES (?,?,?,?,?,?,?)", news_rows)
    conn.commit()
    conn.close()
    return str(db_path)


def test_load_delta_counts(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    df = load_delta()
    assert list(df["date"]) == ["2024-01-01", "2024-01-02"]
    assert list(df["filings"]) == [1, 2]


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
