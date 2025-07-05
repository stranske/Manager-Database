"""Simple news search page."""

from __future__ import annotations

import pandas as pd
import streamlit as st

import sqlite3

from adapters.base import connect_db
from . import require_login


def search_news(term: str) -> pd.DataFrame:
    conn = connect_db()
    if isinstance(conn, sqlite3.Connection):
        conn.execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS news_fts USING fts5(headline, content='news', content_rowid='rowid')"
        )
        conn.execute("INSERT INTO news_fts(news_fts) VALUES('rebuild')")
        query = (
            "SELECT news.headline, news.source "
            "FROM news_fts JOIN news ON news_fts.rowid = news.rowid "
            "WHERE news_fts MATCH ? ORDER BY news.published DESC LIMIT 20"
        )
        df = pd.read_sql_query(query, conn, params=(term,))
    else:
        query = (
            "SELECT headline, source "
            "FROM news "
            "WHERE to_tsvector('english', headline) @@ plainto_tsquery('english', %s) "
            "ORDER BY published DESC LIMIT 20"
        )
        df = pd.read_sql_query(query, conn, params=(term,))
    conn.close()
    return df


def main() -> None:
    if not require_login():
        st.stop()
    st.header("News Search")
    q = st.text_input("Keyword")
    if q:
        df = search_news(q)
        st.dataframe(df)


if __name__ == "__main__":
    main()
