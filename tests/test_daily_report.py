import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

from ui.daily_report import (
    format_shares_delta,
    format_news_table,
    headline_markdown,
    load_diffs,
    load_news,
    parse_topics,
    topic_choices,
)


def setup_db(tmp_path: Path) -> str:
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE daily_diff (date TEXT, cik TEXT, cusip TEXT, change TEXT)")
    conn.execute("CREATE TABLE managers (manager_id TEXT, name TEXT)")
    conn.execute(
        "CREATE TABLE news_items (headline TEXT, url TEXT, published_at TEXT, source TEXT, topics TEXT, confidence REAL, manager_id TEXT)"
    )
    diff_rows = [
        ("2024-05-01", "0", "AAA", "ADD"),
        ("2024-05-01", "0", "BBB", "EXIT"),
    ]
    manager_rows = [("m1", "Manager One"), ("m2", "Manager Two")]
    news_rows = [
        (
            "Headline1",
            "https://example.com/1",
            "2024-05-01T10:00:00",
            "src",
            "merger, earnings",
            0.9,
            "m1",
        ),
        ("Headline2", "https://example.com/2", "2024-05-01T09:00:00", "src", "guidance", 0.8, "m2"),
    ]
    conn.executemany("INSERT INTO daily_diff VALUES (?,?,?,?)", diff_rows)
    conn.executemany("INSERT INTO managers VALUES (?,?)", manager_rows)
    conn.executemany("INSERT INTO news_items VALUES (?,?,?,?,?,?,?)", news_rows)
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
    assert set(diffs["delta_type"]) == {"ADD", "EXIT"}
    assert set(diffs["manager_name"]) == {"0"}
    assert len(news) == 2
    assert "manager_name" in news.columns
    assert set(news["manager_name"]) == {"Manager One", "Manager Two"}


def test_news_topic_helpers():
    assert parse_topics("earnings, merger") == ["earnings", "merger"]
    assert parse_topics("['macro','rates']") == ["macro", "rates"]
    news = pd.DataFrame({"topics": ["earnings,merger", "guidance"]})
    assert topic_choices(news) == ["All topics", "earnings", "guidance", "merger"]


def test_format_news_table():
    news = pd.DataFrame(
        [
            {
                "headline": "Headline1",
                "url": "https://example.com/1",
                "published_at": "2024-05-01T10:00:00",
                "source": "src",
                "topics": "earnings, merger",
                "confidence": 0.8,
                "manager_name": "Manager One",
            }
        ]
    )
    table = format_news_table(news)
    assert list(table.columns) == ["Time", "Manager", "Headline", "Source", "Topics"]
    assert table.iloc[0]["Manager"] == "Manager One"
    assert table.iloc[0]["Headline"] == "Headline1"
    assert table.iloc[0]["Topics"] == "earnings, merger"


def test_headline_markdown():
    link = headline_markdown("Deal [Update]", "https://example.com/news(item)")
    assert link == "[Deal \\[Update\\]](<https://example.com/news(item)>)"
    assert headline_markdown("No URL", "") == "No URL"


def test_format_shares_delta_arrows():
    assert format_shares_delta(1000, 2500) == "<span style='color:green;'>&uarr; +1,500</span>"
    assert format_shares_delta(3000, 2200) == "<span style='color:red;'>&darr; -800</span>"
    assert format_shares_delta(1500, 1500) == "<span style='color:#666;'>&rarr; 0</span>"
    assert format_shares_delta(None, 1500) == "<span style='color:#666;'>-</span>"
