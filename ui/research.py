"""Streamlit research assistant chat page."""

from __future__ import annotations

import datetime as dt
import os
import uuid
from typing import Any, cast

import pandas as pd
import requests  # type: ignore[import-untyped]
import streamlit as st

from adapters.base import connect_db

from . import require_login

CHAT_API_URL = os.getenv("CHAT_API_URL", "http://localhost:8000/api/chat")
REQUEST_TIMEOUT_SECONDS = 45

CHAIN_OPTIONS: list[str] = [
    "Auto (recommended)",
    "Filing Summary",
    "Holdings Analysis",
    "Database Query",
    "RAG Search",
]

CHAIN_MAP: dict[str, str | None] = {
    "Auto (recommended)": None,
    "Filing Summary": "filing_summary",
    "Holdings Analysis": "holdings_analysis",
    "Database Query": "nl_query",
    "RAG Search": "rag_search",
}

QUICK_ACTIONS: list[tuple[str, str]] = [
    ("📊 Summarize latest filing", "Summarize the most recent 13F filing"),
    ("🔍 Top crowded trades", "What are the most crowded trades right now?"),
    ("📈 Recent activism", "Show recent activism events"),
]


def _load_manager_list() -> list[str]:
    """Load manager names for optional context filters."""
    conn = connect_db()
    queries = [
        "SELECT DISTINCT manager_name AS name FROM holdings ORDER BY manager_name",
        "SELECT DISTINCT name FROM managers ORDER BY name",
    ]
    try:
        for query in queries:
            try:
                df = pd.read_sql_query(query, conn)
            except Exception:
                continue
            if not df.empty and "name" in df.columns:
                names = [
                    str(value).strip()
                    for value in df["name"].dropna().tolist()
                    if str(value).strip()
                ]
                if names:
                    return names
    finally:
        conn.close()
    return []


def _build_context(
    selected_manager: str, filing_id_input: int, date_range: tuple[dt.date, dt.date] | tuple[()]
) -> dict[str, Any] | None:
    context: dict[str, Any] = {}
    if selected_manager != "All":
        context["manager_name"] = selected_manager
    if filing_id_input > 0:
        context["filing_id"] = filing_id_input
    if len(date_range) == 2:
        start_date, end_date = date_range
        context["date_range"] = {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
        }
    return context or None


def _source_markdown(source: Any) -> str:
    if isinstance(source, str):
        return source
    if not isinstance(source, dict):
        return str(source)

    source_type = source.get("type", "source")
    parts: list[str] = [f"**{source_type}**"]

    if source.get("document_id"):
        parts.append(f"doc `{source['document_id']}`")
    if source.get("filing_id"):
        parts.append(f"filing `{source['filing_id']}`")
    if source.get("url"):
        parts.append(f"[link]({source['url']})")
    if source.get("filing_url"):
        parts.append(f"[filing]({source['filing_url']})")
    if source.get("news_reference"):
        parts.append(f"news: {source['news_reference']}")

    filing_urls = source.get("filing_urls")
    if isinstance(filing_urls, list):
        for filing_url in filing_urls:
            if filing_url:
                parts.append(f"[filing]({filing_url})")

    news_references = source.get("news_references")
    if isinstance(news_references, list):
        for news_reference in news_references:
            if news_reference:
                parts.append(f"news: {news_reference}")

    description = str(source.get("description", "")).strip()
    if description:
        parts.append(description)

    return ": ".join([parts[0], " | ".join(parts[1:])]) if len(parts) > 1 else parts[0]


def _render_sources(sources: list[Any]) -> None:
    for source in sources:
        st.markdown(f"- {_source_markdown(source)}")


def _call_chat_api(
    question: str, chain_mode: str, context: dict[str, Any] | None
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "question": question,
        "chain": CHAIN_MAP.get(chain_mode),
        "context": context,
    }
    headers = {"x-session-id": st.session_state.chat_session_id}
    response = requests.post(
        CHAT_API_URL,
        json=payload,
        headers=headers,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    if response.status_code >= 400:
        detail: str
        try:
            detail = response.json().get("detail") or response.text
        except Exception:
            detail = response.text
        raise RuntimeError(f"{response.status_code}: {detail}")

    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError("Invalid response from chat API") from exc


def _append_message(
    role: str,
    content: str,
    sources: list[Any] | None = None,
    chain_used: str | None = None,
    latency_ms: int | None = None,
    trace_url: str | None = None,
    sql: str | None = None,
) -> None:
    message: dict[str, Any] = {
        "role": role,
        "content": content,
        "sources": sources or [],
    }
    if chain_used:
        message["chain_used"] = chain_used
    if latency_ms is not None:
        message["latency_ms"] = latency_ms
    if trace_url:
        message["trace_url"] = trace_url
    if sql:
        message["sql"] = sql
    st.session_state.messages.append(message)


def _render_assistant_metadata(message: dict[str, Any]) -> None:
    chain_used = message.get("chain_used")
    latency_ms = message.get("latency_ms")
    trace_url = message.get("trace_url")
    sql = message.get("sql")

    if chain_used or latency_ms is not None or trace_url:
        col1, col2, col3 = st.columns(3)
        if chain_used:
            col1.caption(f"Chain: {chain_used}")
        if latency_ms is not None:
            col2.caption(f"Latency: {latency_ms}ms")
        if trace_url:
            col3.caption(f"[Trace]({trace_url})")

    if sql:
        with st.expander("🔍 Generated SQL"):
            st.code(str(sql), language="sql")


def _run_chat_turn(prompt: str, chain_mode: str, context: dict[str, Any] | None) -> None:
    _append_message("user", prompt)
    with st.chat_message("assistant"):
        with st.spinner("Researching..."):
            try:
                result = _call_chat_api(prompt, chain_mode, context)
            except Exception as exc:
                error_text = f"Request failed: {exc}"
                st.error(error_text)
                _append_message("assistant", error_text)
                return

        answer = str(result.get("answer", ""))
        sources = result.get("sources") or []
        chain_used = str(result.get("chain_used", "unknown"))
        latency_ms = int(result.get("latency_ms", 0))
        trace_url = result.get("trace_url")
        sql = result.get("sql")
        st.markdown(answer)

        _render_assistant_metadata(
            {
                "chain_used": chain_used,
                "latency_ms": latency_ms,
                "trace_url": trace_url,
                "sql": sql,
            }
        )

        if sources:
            with st.expander("📄 Sources", expanded=True):
                _render_sources(sources)

    _append_message(
        "assistant",
        answer,
        sources=sources,
        chain_used=chain_used,
        latency_ms=latency_ms,
        trace_url=trace_url,
        sql=str(sql) if sql else None,
    )


def _init_session_state() -> None:
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "pending_prompt" not in st.session_state:
        st.session_state.pending_prompt = None
    if "chat_session_id" not in st.session_state:
        st.session_state.chat_session_id = str(uuid.uuid4())


def _render_history() -> None:
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("role") == "assistant":
                _render_assistant_metadata(msg)
            sources = msg.get("sources") or []
            if sources:
                with st.expander("📄 Sources"):
                    _render_sources(sources)


def main() -> None:
    st.set_page_config(page_title="Research Assistant", page_icon="🔬", layout="wide")
    if not require_login():
        st.stop()

    _init_session_state()
    st.title("🔬 Research Assistant")

    manager_list = _load_manager_list()
    chain_mode = st.sidebar.selectbox(
        "Chain Mode",
        CHAIN_OPTIONS,
        help="Auto mode classifies your question and routes to the best chain.",
    )

    with st.sidebar.expander("Context Filters", expanded=False):
        selected_manager = st.selectbox("Manager (optional)", ["All", *manager_list])
        filing_id_input = int(st.number_input("Filing ID (for summaries)", value=0, min_value=0))
        date_range = st.date_input("Date range", value=[])

    date_range_tuple = cast(
        tuple[dt.date, dt.date] | tuple[()],
        tuple(date_range) if isinstance(date_range, (list, tuple)) else (),
    )

    context = _build_context(selected_manager, filing_id_input, date_range_tuple)

    _render_history()

    pending_prompt = st.session_state.pending_prompt
    if pending_prompt:
        st.session_state.pending_prompt = None
        _run_chat_turn(pending_prompt, chain_mode, context)

    if prompt := st.chat_input("Ask about your manager universe..."):
        _run_chat_turn(prompt, chain_mode, context)

    st.divider()
    st.caption("Quick actions:")
    col1, col2, col3, col4 = st.columns(4)
    for col, (label, quick_prompt) in zip((col1, col2, col3), QUICK_ACTIONS, strict=True):
        if col.button(label):
            st.session_state.pending_prompt = quick_prompt
            st.rerun()
    if col4.button("🗑️ Clear chat"):
        st.session_state.messages = []
        st.session_state.pending_prompt = None
        st.rerun()


if __name__ == "__main__":
    main()
