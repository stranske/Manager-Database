import datetime as dt
import sqlite3
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from ui import daily_report
from ui.daily_report import load_diffs


def _setup_view_backed_db(tmp_path: Path) -> str:
    db_path = tmp_path / "daily_report_views.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE daily_diffs ("
        "manager_id INTEGER NOT NULL, report_date TEXT NOT NULL, cusip TEXT NOT NULL, "
        "name_of_issuer TEXT, delta_type TEXT NOT NULL, shares_prev INTEGER, shares_curr INTEGER, "
        "value_prev REAL, value_curr REAL)"
    )
    conn.execute("CREATE TABLE daily_diff (date TEXT, cik TEXT, cusip TEXT, change TEXT)")
    conn.execute(
        "CREATE VIEW mv_daily_report AS "
        "SELECT d.report_date, m.manager_id, m.name AS manager_name, d.cusip, d.name_of_issuer, "
        "d.delta_type, d.shares_prev, d.shares_curr, d.value_prev, d.value_curr "
        "FROM daily_diffs d JOIN managers m ON m.manager_id = d.manager_id"
    )
    conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'View Manager')")
    conn.execute(
        "INSERT INTO daily_diffs(manager_id, report_date, cusip, name_of_issuer, delta_type, "
        "shares_prev, shares_curr, value_prev, value_curr) "
        "VALUES (1, '2024-05-01', 'AAA', 'Issuer Alpha', 'INCREASE', 1000, 2500, 10000, 16000)"
    )
    # Fallback table is intentionally seeded with different data to prove view path is used.
    conn.execute(
        "INSERT INTO daily_diff(date, cik, cusip, change) VALUES ('2024-05-01', '0', 'ZZZ', 'ADD')"
    )
    conn.commit()
    conn.close()
    return str(db_path)


def test_load_diffs_prefers_mv_daily_report_when_available(tmp_path, monkeypatch):
    db_path = _setup_view_backed_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    st.cache_data.clear()

    diffs = load_diffs("2024-05-01")

    assert len(diffs) == 1
    assert diffs.iloc[0]["manager_name"] == "View Manager"
    assert diffs.iloc[0]["cusip"] == "AAA"
    assert diffs.iloc[0]["name_of_issuer"] == "Issuer Alpha"
    assert diffs.iloc[0]["delta_type"] == "INCREASE"


def test_daily_report_page_renders_from_mv_daily_report(tmp_path, monkeypatch):
    db_path = _setup_view_backed_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    st.cache_data.clear()

    markdown_calls: list[str] = []
    metric_calls: list[tuple[str, str, str | None]] = []

    class _Tab:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _MetricColumn:
        def metric(self, label, value, delta=None):
            metric_calls.append((label, str(value), None if delta is None else str(delta)))

    monkeypatch.setattr(daily_report, "require_login", lambda: True)
    monkeypatch.setattr(
        daily_report,
        "load_crowded_trades",
        lambda _date, min_managers=3, limit=20: pd.DataFrame(),
    )
    monkeypatch.setattr(
        daily_report,
        "load_contrarian_signals",
        lambda _date, limit=200: pd.DataFrame(),
    )
    monkeypatch.setattr(daily_report, "load_activism_events", lambda _date: pd.DataFrame())
    monkeypatch.setattr(daily_report.st, "date_input", lambda *args, **kwargs: dt.date(2024, 5, 1))
    monkeypatch.setattr(daily_report.st, "tabs", lambda labels: [_Tab(), _Tab(), _Tab()])
    monkeypatch.setattr(daily_report.st, "columns", lambda n: [_MetricColumn() for _ in range(n)])
    monkeypatch.setattr(
        daily_report.st,
        "markdown",
        lambda value, **kwargs: markdown_calls.append(value),
    )
    monkeypatch.setattr(daily_report.st, "download_button", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_report.st, "selectbox", lambda *args, **kwargs: "All topics")
    monkeypatch.setattr(daily_report.st, "dataframe", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_report.st, "info", lambda *args, **kwargs: None)

    daily_report.main()

    html_table = next((call for call in markdown_calls if "<table" in call), "")
    assert "View Manager" in html_table
    assert "Issuer Alpha" in html_table
    assert "↑ +1,500" in html_table
    assert "↑ +$6,000.00" in html_table
    assert "+60.0%" in html_table
    assert any(call[0] == "Managers w/ changes" and call[1] == "1" for call in metric_calls)
    assert any(call[0] == "Increased" and call[1] == "1" for call in metric_calls)


def test_daily_report_page_renders_under_500ms_with_ten_managers(monkeypatch):
    manager_rows = [
        {
            "manager_name": f"Manager {idx}",
            "cusip": f"CUSIP{idx:03d}",
            "name_of_issuer": f"Issuer {idx}",
            "delta_type": "INCREASE",
            "shares_prev": 1000,
            "shares_curr": 1100 + idx,
            "value_prev": 10000.0,
            "value_curr": 12000.0 + idx,
        }
        for idx in range(10)
    ]

    class _Tab:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _MetricColumn:
        def metric(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(daily_report, "require_login", lambda: True)
    monkeypatch.setattr(daily_report, "load_diffs", lambda _date: pd.DataFrame(manager_rows))
    monkeypatch.setattr(
        daily_report,
        "load_crowded_trades",
        lambda _date, min_managers=3, limit=20: pd.DataFrame(),
    )
    monkeypatch.setattr(
        daily_report,
        "load_contrarian_signals",
        lambda _date, limit=200: pd.DataFrame(),
    )
    monkeypatch.setattr(daily_report, "load_activism_events", lambda _date: pd.DataFrame())
    monkeypatch.setattr(
        daily_report,
        "load_news",
        lambda _date: pd.DataFrame(
            columns=[
                "headline",
                "url",
                "published_at",
                "source",
                "topics",
                "confidence",
                "manager_name",
            ]
        ),
    )
    monkeypatch.setattr(daily_report.st, "date_input", lambda *args, **kwargs: dt.date(2024, 5, 1))
    monkeypatch.setattr(daily_report.st, "tabs", lambda labels: [_Tab(), _Tab(), _Tab()])
    monkeypatch.setattr(daily_report.st, "columns", lambda n: [_MetricColumn() for _ in range(n)])
    monkeypatch.setattr(daily_report.st, "markdown", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_report.st, "download_button", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_report.st, "info", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_report.st, "selectbox", lambda *args, **kwargs: "All topics")
    monkeypatch.setattr(daily_report.st, "dataframe", lambda *args, **kwargs: None)

    start = time.perf_counter()
    daily_report.main()
    elapsed_ms = (time.perf_counter() - start) * 1000
    assert elapsed_ms < 500


def test_daily_report_crowded_trades_tab_renders_sortable_table(monkeypatch):
    dataframe_calls: list[tuple[pd.DataFrame, bool, bool]] = []
    selectbox_calls: list[tuple[str, list[str], str]] = []

    class _Tab:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _MetricColumn:
        def metric(self, *_args, **_kwargs):
            return None

    crowded = pd.DataFrame(
        [
            {
                "cusip": "AAA111111",
                "name_of_issuer": "Issuer Alpha",
                "manager_count": 3,
                "manager_names": ["Alpha", "Beta", "Gamma"],
                "total_value_usd": 1500.0,
                "avg_conviction_pct": 12.5,
            },
            {
                "cusip": "BBB222222",
                "name_of_issuer": "Issuer Beta",
                "manager_count": 5,
                "manager_names": ["Delta", "Echo", "Foxtrot"],
                "total_value_usd": 1000.0,
                "avg_conviction_pct": 8.0,
            },
        ]
    )

    monkeypatch.setattr(daily_report, "require_login", lambda: True)
    monkeypatch.setattr(daily_report, "load_diffs", lambda _date: pd.DataFrame())
    monkeypatch.setattr(daily_report, "load_activism_events", lambda _date: pd.DataFrame())
    monkeypatch.setattr(
        daily_report,
        "load_contrarian_signals",
        lambda _date, limit=200: pd.DataFrame(),
    )
    monkeypatch.setattr(daily_report, "load_news", lambda _date: pd.DataFrame())
    monkeypatch.setattr(daily_report, "load_crowded_trades", lambda _date: crowded.copy())
    monkeypatch.setattr(daily_report.st, "date_input", lambda *args, **kwargs: dt.date(2024, 5, 1))
    monkeypatch.setattr(daily_report.st, "tabs", lambda labels: [_Tab(), _Tab(), _Tab()])
    monkeypatch.setattr(daily_report.st, "columns", lambda n: [_MetricColumn() for _ in range(n)])
    monkeypatch.setattr(daily_report.st, "markdown", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_report.st, "download_button", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_report.st, "info", lambda *args, **kwargs: None)

    def _selectbox(label, options, key=None):
        selectbox_calls.append((label, list(options), key))
        return "Manager count"

    monkeypatch.setattr(daily_report.st, "selectbox", _selectbox)
    monkeypatch.setattr(
        daily_report.st,
        "dataframe",
        lambda df, use_container_width=True, hide_index=False: dataframe_calls.append(
            (df.copy(), use_container_width, hide_index)
        ),
    )

    daily_report.main()

    assert selectbox_calls == [
        (
            "Sort crowded trades by",
            ["Manager count", "Total value"],
            "crowded_sort_2024-05-01",
        )
    ]
    crowded_table = dataframe_calls[0][0]
    assert list(crowded_table.columns) == [
        "CUSIP",
        "Issuer",
        "# Managers",
        "Total Value",
        "Avg Conviction %",
        "Managers",
    ]
    assert crowded_table.iloc[0]["CUSIP"] == "BBB222222"
    assert crowded_table.iloc[0]["# Managers"] == 5
    assert crowded_table.iloc[0]["Managers"] == "Delta, Echo, Foxtrot"


def test_daily_report_filings_and_diffs_marks_contrarian_rows(monkeypatch):
    markdown_calls: list[str] = []

    class _Tab:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _MetricColumn:
        def metric(self, *_args, **_kwargs):
            return None

    diffs = pd.DataFrame(
        [
            {
                "manager_name": "Alpha Partners",
                "cusip": "AAA111111",
                "name_of_issuer": "Issuer Alpha",
                "delta_type": "DECREASE",
                "shares_prev": 100,
                "shares_curr": 40,
                "value_prev": 1000.0,
                "value_curr": 500.0,
            }
        ]
    )
    contrarian = pd.DataFrame(
        [
            {
                "manager_name": "Alpha Partners",
                "cusip": "AAA111111",
                "name_of_issuer": "Issuer Alpha",
                "direction": "SELL",
                "consensus_direction": "BUY",
                "delta_value": -500.0,
                "consensus_count": 4,
                "report_date": "2024-05-01",
            }
        ]
    )

    monkeypatch.setattr(daily_report, "require_login", lambda: True)
    monkeypatch.setattr(daily_report, "load_diffs", lambda _date: diffs.copy())
    monkeypatch.setattr(
        daily_report, "load_contrarian_signals", lambda _date, limit=200: contrarian
    )
    monkeypatch.setattr(daily_report, "load_activism_events", lambda _date: pd.DataFrame())
    monkeypatch.setattr(daily_report, "load_crowded_trades", lambda _date: pd.DataFrame())
    monkeypatch.setattr(daily_report, "load_news", lambda _date: pd.DataFrame())
    monkeypatch.setattr(daily_report.st, "date_input", lambda *args, **kwargs: dt.date(2024, 5, 1))
    monkeypatch.setattr(daily_report.st, "tabs", lambda labels: [_Tab(), _Tab(), _Tab()])
    monkeypatch.setattr(daily_report.st, "columns", lambda n: [_MetricColumn() for _ in range(n)])
    monkeypatch.setattr(
        daily_report.st,
        "markdown",
        lambda value, **kwargs: markdown_calls.append(value),
    )
    monkeypatch.setattr(daily_report.st, "download_button", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_report.st, "selectbox", lambda *args, **kwargs: "All topics")
    monkeypatch.setattr(daily_report.st, "dataframe", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_report.st, "info", lambda *args, **kwargs: None)

    daily_report.main()

    html_table = next((call for call in markdown_calls if "<table" in call), "")
    assert "Contrarian: SELL vs BUY" in html_table
    assert "Issuer Alpha" in html_table
