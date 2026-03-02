import sqlite3
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ui.dashboard import (
    load_delta,
    load_filing_timeline,
    load_latest_holdings_snapshot,
    load_managers,
    render_filing_timeline,
    render_latest_holdings_snapshot,
    render_manager_selector,
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
    conn.executemany("INSERT INTO managers VALUES (?,?)", manager_rows)
    conn.executemany("INSERT INTO holdings VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.executemany("INSERT INTO filings VALUES (?,?,?,?,?,?,?)", filing_rows)
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
