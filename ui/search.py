"""Simple news search page."""

from __future__ import annotations

import sqlite3
from collections import Counter

import pandas as pd
import streamlit as st

from adapters.base import connect_db
from api.search import SearchResult, universal_search

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


_ENTITY_LABELS: dict[str, str] = {
    "manager": "Managers",
    "filing": "Filings",
    "holding": "Holdings",
    "news": "News",
    "document": "Documents",
}


def _count_results_by_entity_type(results: list[SearchResult]) -> dict[str, int]:
    counts = Counter(item.entity_type for item in results)
    ordered: dict[str, int] = {}
    for entity_type in _ENTITY_LABELS:
        if counts.get(entity_type, 0) > 0:
            ordered[entity_type] = counts[entity_type]
    for entity_type, count in sorted(counts.items()):
        if entity_type not in ordered:
            ordered[entity_type] = count
    return ordered


def _render_result(result: SearchResult) -> None:
    manager_text = f" | Manager: {result.manager_name}" if result.manager_name else ""
    ts_text = f" | {result.timestamp}" if result.timestamp else ""
    st.markdown(f"**{result.headline}**")
    st.caption(f"{result.entity_type.upper()}{manager_text}{ts_text}")
    if result.snippet:
        st.write(result.snippet)
    if result.url:
        st.link_button("Open source", result.url)
    st.divider()


def main() -> None:
    if not require_login():
        st.stop()
    st.header("Universal Search")
    q = st.text_input("Search managers, filings, holdings, news, and documents")
    limit = st.number_input("Result limit", min_value=1, max_value=100, value=20, step=1)
    if q:
        conn = connect_db()
        try:
            results = universal_search(q, conn, int(limit))
        finally:
            conn.close()

        if not results:
            st.info("No results found.")
            return

        counts = _count_results_by_entity_type(results)
        st.subheader("Summary")
        metric_columns = st.columns(len(counts) + 1)
        metric_columns[0].metric("Total", len(results))
        for idx, (entity_type, count) in enumerate(counts.items(), start=1):
            metric_columns[idx].metric(_ENTITY_LABELS.get(entity_type, entity_type.title()), count)

        st.subheader("Results")
        for entity_type in _ENTITY_LABELS:
            grouped = [item for item in results if item.entity_type == entity_type]
            if not grouped:
                continue
            with st.expander(f"{_ENTITY_LABELS[entity_type]} ({len(grouped)})", expanded=True):
                for item in grouped:
                    _render_result(item)


if __name__ == "__main__":
    main()
