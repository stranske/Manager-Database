"""Simple news search page."""

from __future__ import annotations

import sqlite3

import pandas as pd
import streamlit as st

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


def search_notes(term: str) -> pd.DataFrame:
    conn = connect_db()
    if isinstance(conn, sqlite3.Connection):
        conn.execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts USING fts5(content, content='notes', content_rowid='rowid')"
        )
        conn.execute("INSERT INTO notes_fts(notes_fts) VALUES('rebuild')")
        query = (
            "SELECT notes.filename "
            "FROM notes_fts JOIN notes ON notes_fts.rowid = notes.rowid "
            "WHERE notes_fts MATCH ? LIMIT 20"
        )
        df = pd.read_sql_query(query, conn, params=(term,))
    else:
        query = (
            "SELECT filename "
            "FROM notes "
            "WHERE to_tsvector('english', content) @@ plainto_tsquery('english', %s) "
            "LIMIT 20"
        )
        df = pd.read_sql_query(query, conn, params=(term,))
    conn.close()
    return df


def main() -> None:
    if not require_login():
        st.stop()
    st.header("Search")
    q = st.text_input("Keyword")
    if q:
        news = search_news(q)
        notes = search_notes(q)
        if not news.empty:
            st.subheader("News")
            st.dataframe(news)
        if not notes.empty:
            st.subheader("Notes")
            st.dataframe(notes)


if __name__ == "__main__":
    main()
