import sqlite3
from pathlib import Path

import streamlit as st

from ui.daily_report import load_diffs, load_news


def setup_db(tmp_path: Path) -> str:
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE daily_diff (date TEXT, cik TEXT, cusip TEXT, change TEXT)"
    )
    conn.execute("CREATE TABLE news (headline TEXT, source TEXT, published TEXT)")
    diff_rows = [
        ("2024-05-01", "0", "AAA", "ADD"),
        ("2024-05-01", "0", "BBB", "EXIT"),
    ]
    news_rows = [
        ("Headline1", "src", "2024-05-01"),
        ("Headline2", "src", "2024-05-01"),
    ]
    conn.executemany("INSERT INTO daily_diff VALUES (?,?,?,?)", diff_rows)
    conn.executemany("INSERT INTO news VALUES (?,?,?)", news_rows)
    conn.commit()
    conn.close()
    return str(db_path)


def test_load_diffs_and_news(tmp_path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    st.cache_data.clear()
    diffs = load_diffs("2024-05-01")
    news = load_news("2024-05-01")
    assert len(diffs) == 2
    assert set(diffs["change"]) == {"ADD", "EXIT"}
    assert len(news) == 2
