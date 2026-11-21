from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import Any
from unittest.mock import Mock

import pytest

from llm.cost_tracking import estimate_cost_usd, log_llm_usage

# Deterministic provider observations replayed in issue #1939 acceptance tests.
FOCUSED_CHAT_USAGE: dict[str, Any] = {
    "provider": "openai",
    "model": "gpt-4o-mini",
    "tokens_in": 512,
    "tokens_out": 128,
    "latency_ms": 842,
    "trace_url": "https://smith.langchain.com/r/chat-replay-1939",
}

FOCUSED_ANALYSIS_USAGE: dict[str, Any] = {
    "provider": "anthropic",
    "model": "claude-sonnet-4-20250514",
    "tokens_in": 2048,
    "tokens_out": 896,
    "latency_ms": 1520,
}


def test_estimate_cost_usd_uses_model_pricing():
    assert estimate_cost_usd("gpt-4o-mini", 1000, 500) == 0.00045
    assert estimate_cost_usd("unknown-model", 1000, 500) == 0.0


def test_log_llm_usage_pins_bytes_zero_for_focused_chat_input():
    """Current boundary: token arguments do not populate ``api_usage.bytes`` yet."""
    conn = sqlite3.connect(":memory:")
    try:
        log_llm_usage(conn, **FOCUSED_CHAT_USAGE)
        row = conn.execute(
            "SELECT source, endpoint, status, bytes, latency_ms, cost_usd FROM api_usage"
        ).fetchone()
    finally:
        conn.close()

    assert row == (
        "langchain",
        "openai/gpt-4o-mini https://smith.langchain.com/r/chat-replay-1939",
        200,
        0,
        842,
        0.000154,
    )


def test_log_llm_usage_pins_bytes_zero_for_focused_analysis_input():
    conn = sqlite3.connect(":memory:")
    try:
        log_llm_usage(conn, **FOCUSED_ANALYSIS_USAGE)
        row = conn.execute(
            "SELECT source, endpoint, status, bytes, latency_ms, cost_usd FROM api_usage"
        ).fetchone()
    finally:
        conn.close()

    assert row == (
        "langchain",
        "anthropic/claude-sonnet-4-20250514",
        200,
        0,
        1520,
        0.019584,
    )


def test_log_llm_usage_owned_sqlite_connection_commits_and_closes(tmp_path, monkeypatch):
    db_path = tmp_path / "usage.db"
    conn = sqlite3.connect(db_path)
    monkeypatch.setattr("llm.cost_tracking.connect_db", lambda: conn)

    log_llm_usage(None, **FOCUSED_CHAT_USAGE)

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        conn.execute("SELECT 1")

    with closing(sqlite3.connect(db_path)) as reader:
        assert reader.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 1


def test_log_llm_usage_borrowed_connection_stays_open():
    conn = sqlite3.connect(":memory:")
    try:
        log_llm_usage(conn, **FOCUSED_ANALYSIS_USAGE)
        assert conn.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 1
        conn.execute("SELECT 1").fetchone()
    finally:
        conn.close()


def test_log_llm_usage_owned_non_sqlite_connection_closes_without_sqlite_commit(monkeypatch):
    conn = Mock()
    monkeypatch.setattr("llm.cost_tracking.connect_db", lambda: conn)
    monkeypatch.setattr("llm.cost_tracking.is_sqlite", lambda _conn: False)
    monkeypatch.setattr("llm.cost_tracking.ensure_api_usage_schema", lambda _conn: None)
    monkeypatch.setattr("llm.cost_tracking.get_placeholder", lambda _conn: "%s")

    log_llm_usage(None, **FOCUSED_ANALYSIS_USAGE)

    conn.execute.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once_with()


def test_log_llm_usage_creates_api_usage_and_inserts_row():
    conn = sqlite3.connect(":memory:")
    try:
        log_llm_usage(
            conn,
            provider="openai",
            model="gpt-4o-mini",
            tokens_in=1000,
            tokens_out=500,
            latency_ms=321,
            trace_url="https://smith.langchain.com/r/abc123",
        )

        row = conn.execute(
            "SELECT source, endpoint, status, bytes, latency_ms, cost_usd FROM api_usage"
        ).fetchone()
    finally:
        conn.close()

    assert row == (
        "langchain",
        "openai/gpt-4o-mini https://smith.langchain.com/r/abc123",
        200,
        0,
        321,
        0.00045,
    )
