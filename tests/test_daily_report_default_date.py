import sqlite3

import pandas as pd
import pytest
import streamlit as st

from scripts.build_wasm_demo import build_wasm_demo
from ui import daily_report


def test_daily_report_default_date_uses_bundled_demo_data(tmp_path, monkeypatch):
    db_path = build_wasm_demo(tmp_path / "web")
    monkeypatch.setenv("DB_PATH", str(db_path))
    st.cache_data.clear()

    latest = daily_report.latest_report_date()

    assert latest is not None
    assert latest.isoformat() == "2026-03-15"
    assert not daily_report.load_diffs(latest.isoformat()).empty


def test_empty_report_message_points_to_nearest_populated_date(tmp_path, monkeypatch):
    db_path = tmp_path / "daily-report.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE daily_diffs (report_date TEXT)")
    conn.execute("INSERT INTO daily_diffs(report_date) VALUES ('2026-03-15')")
    conn.commit()
    conn.close()
    monkeypatch.setenv("DB_PATH", str(db_path))
    st.cache_data.clear()

    assert daily_report.empty_report_date_message("2026-06-22") == (
        "No data for this date — nearest report is 2026-03-15."
    )


def test_nearest_report_date_propagates_operational_query_errors(monkeypatch):
    st.cache_data.clear()

    class _Conn:
        def close(self):
            pass

    monkeypatch.setattr(daily_report, "connect_db", lambda: _Conn())

    def _raise_operational_error(*_args, **_kwargs):
        raise pd.errors.DatabaseError("database is locked")

    monkeypatch.setattr(daily_report.pd, "read_sql_query", _raise_operational_error)

    with pytest.raises(pd.errors.DatabaseError, match="database is locked"):
        daily_report.nearest_report_date("2026-06-22")
