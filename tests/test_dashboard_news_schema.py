"""News feeds must survive both supported SQLite manager schemas."""

import sqlite3
from types import SimpleNamespace

import pandas as pd
import pytest

from ui import dashboard


@pytest.mark.parametrize(
    ("id_column", "postgres"), [("id", False), ("manager_id", False), ("manager_id", True)]
)
@pytest.mark.parametrize("manager_id", [None, 7])
def test_news_stream_schema_filter_order_and_limit(monkeypatch, id_column, postgres, manager_id):
    conn = sqlite3.connect(":memory:")
    conn.execute(f"CREATE TABLE managers ({id_column} INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO managers VALUES (?, ?)", [(7, "Alpha"), (8, "Beta")])
    conn.execute(
        "CREATE TABLE news_items (manager_id INTEGER, headline TEXT, url TEXT, "
        "published_at TEXT, source TEXT, topics TEXT, confidence REAL)"
    )
    conn.executemany(
        "INSERT INTO news_items VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (7, "old", "https://old", "2026-09-01", "feed", "[]", 0.5),
            (8, "other", "https://other", "2026-09-04", "feed", "[]", 0.6),
            (7, "new", "https://new", "2026-09-03", "wire", '["activism"]', 0.9),
            (None, "unlinked", "https://unlinked", "2026-09-05", "wire", "[]", 0.8),
        ],
    )
    if postgres:
        # Execute the PostgreSQL query through SQLite after translating only its
        # placeholders. This checks query semantics, not a live PostgreSQL driver.
        proxy = SimpleNamespace(close=conn.close)
        read_sql_query = pd.read_sql_query

        def read_postgres_query(query, connection, params):
            assert connection is proxy
            assert "?" not in query
            assert query.count("%s") == len(params)
            return read_sql_query(query.replace("%s", "?"), conn, params=params)

        monkeypatch.setattr(dashboard, "connect_db", lambda: proxy)
        monkeypatch.setattr(dashboard.pd, "read_sql_query", read_postgres_query)
    else:
        monkeypatch.setattr(dashboard, "connect_db", lambda: conn)

    result = dashboard.load_news_stream(manager_id, limit=2)

    assert list(result.columns) == [
        "headline",
        "url",
        "published_at",
        "source",
        "topics",
        "confidence",
        "manager_name",
    ]
    if manager_id is None:
        assert result["headline"].tolist() == ["unlinked", "other"]
        assert pd.isna(result.loc[0, "manager_name"])
        assert result.loc[1, "manager_name"] == "Beta"
    else:
        assert result["headline"].tolist() == ["new", "old"]
        assert result["manager_name"].tolist() == ["Alpha", "Alpha"]
        assert result.iloc[0].to_dict() == {
            "headline": "new",
            "url": "https://new",
            "published_at": "2026-09-03",
            "source": "wire",
            "topics": '["activism"]',
            "confidence": 0.9,
            "manager_name": "Alpha",
        }
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        conn.execute("SELECT 1")


def test_news_stream_missing_schema_returns_empty_columns_and_closes(monkeypatch):
    conn = sqlite3.connect(":memory:")
    monkeypatch.setattr(dashboard, "connect_db", lambda: conn)

    result = dashboard.load_news_stream(None)

    assert result.empty
    assert list(result.columns) == [
        "headline",
        "url",
        "published_at",
        "source",
        "topics",
        "confidence",
        "manager_name",
    ]
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        conn.execute("SELECT 1")
