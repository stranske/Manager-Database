import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

from ui.daily_report import (
    format_activism_event_type,
    format_news_table,
    format_percent_change,
    format_shares_delta,
    format_signal_badge,
    format_value_delta,
    headline_markdown,
    load_activism_events,
    load_contrarian_signals,
    load_crowded_trades,
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
    conn.execute(
        "CREATE TABLE activism_filings (filing_id INTEGER, manager_id TEXT, filing_type TEXT, subject_company TEXT, subject_cusip TEXT, ownership_pct REAL, shares INTEGER, group_members TEXT, purpose_snippet TEXT, filed_date TEXT, url TEXT, raw_key TEXT, created_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE activism_events (event_id INTEGER, manager_id TEXT, filing_id INTEGER, event_type TEXT, subject_company TEXT, subject_cusip TEXT, ownership_pct REAL, previous_pct REAL, delta_pct REAL, threshold_crossed REAL, detected_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE crowded_trades (crowd_id INTEGER, cusip TEXT, name_of_issuer TEXT, manager_count INTEGER, manager_ids TEXT, total_value_usd REAL, avg_conviction_pct REAL, max_conviction_pct REAL, report_date TEXT, computed_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE contrarian_signals (signal_id INTEGER, manager_id TEXT, cusip TEXT, name_of_issuer TEXT, direction TEXT, consensus_direction TEXT, manager_delta_shares INTEGER, manager_delta_value REAL, consensus_count INTEGER, report_date TEXT, detected_at TEXT)"
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
    activism_filings = [
        (
            1,
            "m1",
            "SC 13D",
            "Example Corp",
            "AAA111111",
            5.2,
            100,
            "[]",
            "board seat",
            "2024-05-01",
            "https://example.com/filing/1",
            "raw/1",
            "2024-05-01T08:00:00",
        )
    ]
    activism_events = [
        (
            10,
            "m1",
            1,
            "threshold_crossing",
            "Example Corp",
            "AAA111111",
            5.2,
            4.9,
            0.3,
            5.0,
            "2024-05-01T09:00:00",
        )
    ]
    crowded_trades = [
        (
            20,
            "AAA111111",
            "Example Corp",
            3,
            "[1, 2, 3]",
            1200000.0,
            14.75,
            21.5,
            "2024-05-01",
            "2024-05-01T10:00:00",
        )
    ]
    contrarian_signals = [
        (
            30,
            "m1",
            "AAA111111",
            "Example Corp",
            "SELL",
            "BUY",
            -100,
            -2500.0,
            4,
            "2024-05-01",
            "2024-05-01T11:00:00",
        )
    ]
    conn.executemany("INSERT INTO daily_diff VALUES (?,?,?,?)", diff_rows)
    conn.executemany("INSERT INTO managers VALUES (?,?)", manager_rows)
    conn.executemany("INSERT INTO news_items VALUES (?,?,?,?,?,?,?)", news_rows)
    conn.executemany(
        "INSERT INTO activism_filings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", activism_filings
    )
    conn.executemany("INSERT INTO activism_events VALUES (?,?,?,?,?,?,?,?,?,?,?)", activism_events)
    conn.executemany("INSERT INTO crowded_trades VALUES (?,?,?,?,?,?,?,?,?,?)", crowded_trades)
    conn.executemany(
        "INSERT INTO contrarian_signals VALUES (?,?,?,?,?,?,?,?,?,?,?)", contrarian_signals
    )
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
    activism = load_activism_events("2024-05-01")
    crowded = load_crowded_trades("2024-05-01", min_managers=3)
    contrarian = load_contrarian_signals("2024-05-01")
    assert len(activism) == 1
    assert activism.iloc[0]["event_type"] == "threshold_crossing"
    assert activism.iloc[0]["filed_date"] == "2024-05-01"
    assert len(crowded) == 1
    assert crowded.iloc[0]["manager_names"] == []
    assert len(contrarian) == 1
    assert contrarian.iloc[0]["consensus_direction"] == "BUY"


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
    assert format_shares_delta(1000, 2500) == "<span style='color:green;'>↑ +1,500</span>"
    assert format_shares_delta(3000, 2200) == "<span style='color:red;'>↓ -800</span>"
    assert format_shares_delta(1500, 1500) == "<span style='color:#666;'>→ 0</span>"
    assert format_shares_delta(None, 1500) == "<span style='color:green;'>↑ +1,500</span>"
    assert format_shares_delta(800, None) == "<span style='color:red;'>↓ -800</span>"
    assert format_shares_delta(None, None) == "<span style='color:#666;'>-</span>"


def test_format_value_and_percent_delta():
    assert format_value_delta(1000, 1300) == "<span style='color:green;'>↑ +$300.00</span>"
    assert format_value_delta(1000, 800) == "<span style='color:red;'>↓ -$200.00</span>"
    assert format_value_delta(None, None) == "<span style='color:#666;'>-</span>"

    assert format_percent_change(1000, 1250) == "<span style='color:green;'>+25.0%</span>"
    assert format_percent_change(1000, 750) == "<span style='color:red;'>-25.0%</span>"
    assert format_percent_change(0, 200) == "<span style='color:#666;'>n/a</span>"
    assert format_percent_change(None, None) == "<span style='color:#666;'>-</span>"


def test_format_activism_event_type():
    assert "color:#dc2626" in format_activism_event_type("threshold_crossing")
    assert "initial_stake" in format_activism_event_type("initial_stake")
    assert "&lt;script&gt;" in format_activism_event_type("<script>")


def test_format_signal_badge():
    badge = format_signal_badge("SELL", "BUY")
    assert "Contrarian" in badge
    assert "SELL" in badge
    assert "BUY" in badge
