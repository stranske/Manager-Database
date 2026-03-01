"""Simple news search page."""

from __future__ import annotations

import sqlite3
from collections import Counter
from html import escape

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

_ENTITY_BADGE_COLORS: dict[str, tuple[str, str]] = {
    "manager": ("#1F2937", "#E5E7EB"),
    "filing": ("#1E3A8A", "#DBEAFE"),
    "holding": ("#854D0E", "#FEF3C7"),
    "news": ("#166534", "#DCFCE7"),
    "document": ("#6D28D9", "#EDE9FE"),
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


def _entity_badge_html(entity_type: str) -> str:
    bg, fg = _ENTITY_BADGE_COLORS.get(entity_type, ("#374151", "#F3F4F6"))
    label = escape(entity_type.upper())
    return (
        "<span style='display:inline-block;padding:0.1rem 0.5rem;border-radius:999px;"
        f"background:{bg};color:{fg};font-size:0.72rem;font-weight:700'>{label}</span>"
    )


def _format_result_meta_html(result: SearchResult) -> str:
    manager_text = f" | Manager: {escape(result.manager_name)}" if result.manager_name else ""
    ts_text = f" | {escape(result.timestamp)}" if result.timestamp else ""
    badge_html = _entity_badge_html(result.entity_type)
    return (
        f"{badge_html} "
        f"<span style='color:#6B7280'>Relevance {result.relevance:.2f}{manager_text}{ts_text}</span>"
    )


def _group_results_by_entity_type(
    results: list[SearchResult],
) -> list[tuple[str, list[SearchResult]]]:
    grouped: dict[str, list[SearchResult]] = {}
    for result in results:
        grouped.setdefault(result.entity_type, []).append(result)
    return sorted(
        grouped.items(),
        key=lambda item: (
            max(result.relevance for result in item[1]),
            -len(item[1]),
            _ENTITY_LABELS.get(item[0], item[0]),
        ),
        reverse=True,
    )


def _render_result(result: SearchResult) -> None:
    st.markdown(f"**{result.headline}**")
    st.markdown(_format_result_meta_html(result), unsafe_allow_html=True)
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
        st.caption("Results are sorted by relevance and grouped by entity type.")
        for entity_type, grouped in _group_results_by_entity_type(results):
            label = _ENTITY_LABELS.get(entity_type, entity_type.title())
            with st.expander(f"{label} ({len(grouped)})", expanded=True):
                for item in sorted(grouped, key=lambda r: r.relevance, reverse=True):
                    _render_result(item)


if __name__ == "__main__":
    main()
