import datetime as dt
import sqlite3
from pathlib import Path

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

    class _Tab:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(daily_report, "require_login", lambda: True)
    monkeypatch.setattr(daily_report.st, "date_input", lambda *args, **kwargs: dt.date(2024, 5, 1))
    monkeypatch.setattr(daily_report.st, "tabs", lambda labels: [_Tab(), _Tab()])
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
