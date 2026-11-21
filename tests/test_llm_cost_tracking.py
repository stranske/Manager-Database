from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from llm import langsmith_fleet as fleet
from llm.cost_tracking import (
    USAGE_BYTES_UNKNOWN,
    estimate_cost_usd,
    legacy_placeholder_usage_bytes,
    log_llm_usage,
    observed_cost_usd,
    observed_usage_bytes,
    persist_observed_provider_usage,
)

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


def test_legacy_bytes_placeholder_compatibility_control_for_focused_chat_input():
    """Pre-1939 ``log_llm_usage`` always wrote ``bytes=0`` regardless of token counts."""
    assert legacy_placeholder_usage_bytes() == 0
    assert observed_usage_bytes(
        FOCUSED_CHAT_USAGE["tokens_in"], FOCUSED_CHAT_USAGE["tokens_out"]
    ) == (FOCUSED_CHAT_USAGE["tokens_in"] + FOCUSED_CHAT_USAGE["tokens_out"])
    assert legacy_placeholder_usage_bytes() != observed_usage_bytes(
        FOCUSED_CHAT_USAGE["tokens_in"], FOCUSED_CHAT_USAGE["tokens_out"]
    )


def test_legacy_bytes_placeholder_compatibility_control_for_focused_analysis_input():
    assert legacy_placeholder_usage_bytes() == 0
    assert observed_usage_bytes(
        FOCUSED_ANALYSIS_USAGE["tokens_in"], FOCUSED_ANALYSIS_USAGE["tokens_out"]
    ) == (FOCUSED_ANALYSIS_USAGE["tokens_in"] + FOCUSED_ANALYSIS_USAGE["tokens_out"])


def test_focused_chat_fixture_legacy_readback_zero_observed_readback_matches_tokens():
    """Same ``FOCUSED_CHAT_USAGE``: pre-1939 readback stored ``bytes=0``; post-change stores token sum."""
    conn = sqlite3.connect(":memory:")
    legacy_bytes = legacy_placeholder_usage_bytes()
    observed_bytes = observed_usage_bytes(
        FOCUSED_CHAT_USAGE["tokens_in"], FOCUSED_CHAT_USAGE["tokens_out"]
    )

    persist_observed_provider_usage(conn, invocation="chat", **FOCUSED_CHAT_USAGE)
    readback = conn.execute("SELECT bytes, cost_usd FROM api_usage").fetchone()

    assert legacy_bytes == 0
    assert readback == (
        observed_bytes,
        observed_cost_usd(
            FOCUSED_CHAT_USAGE["model"],
            FOCUSED_CHAT_USAGE["tokens_in"],
            FOCUSED_CHAT_USAGE["tokens_out"],
        ),
    )
    assert readback[0] != legacy_bytes
    conn.close()


def test_log_llm_usage_owned_sqlite_connection_commits_and_closes(tmp_path, monkeypatch):
    db_path = tmp_path / "usage.db"
    conn = sqlite3.connect(db_path)
    monkeypatch.setattr("llm.cost_tracking.connect_db", lambda: conn)

    persist_observed_provider_usage(None, invocation="chat", **FOCUSED_CHAT_USAGE)

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        conn.execute("SELECT 1")

    with closing(sqlite3.connect(db_path)) as reader:
        assert reader.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 1


def test_log_llm_usage_borrowed_connection_stays_open():
    conn = sqlite3.connect(":memory:")
    try:
        persist_observed_provider_usage(conn, invocation="analysis", **FOCUSED_ANALYSIS_USAGE)
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

    persist_observed_provider_usage(None, invocation="analysis", **FOCUSED_ANALYSIS_USAGE)

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
        1500,
        321,
        0.00045,
    )


def test_chat_and_analysis_persist_observed_usage_once(tmp_path, monkeypatch):
    """Replay deterministic chat and analysis observations through ``persist_observed_provider_usage``."""
    conn = sqlite3.connect(":memory:")
    fleet_path = tmp_path / "fleet.ndjson"
    monkeypatch.setenv(fleet.ENV_FLEET_PATH, str(fleet_path))
    monkeypatch.delenv(fleet.ENV_LANGSMITH_KEY, raising=False)

    persist_observed_provider_usage(conn, invocation="chat", **FOCUSED_CHAT_USAGE)
    persist_observed_provider_usage(conn, invocation="analysis", **FOCUSED_ANALYSIS_USAGE)

    chat_row = conn.execute(
        "SELECT source, endpoint, status, bytes, latency_ms, cost_usd FROM api_usage ORDER BY rowid LIMIT 1"
    ).fetchone()
    analysis_row = conn.execute(
        "SELECT source, endpoint, status, bytes, latency_ms, cost_usd FROM api_usage ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    assert conn.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 2

    chat_bytes = observed_usage_bytes(
        FOCUSED_CHAT_USAGE["tokens_in"], FOCUSED_CHAT_USAGE["tokens_out"]
    )
    analysis_bytes = observed_usage_bytes(
        FOCUSED_ANALYSIS_USAGE["tokens_in"], FOCUSED_ANALYSIS_USAGE["tokens_out"]
    )
    chat_cost = observed_cost_usd(
        FOCUSED_CHAT_USAGE["model"],
        FOCUSED_CHAT_USAGE["tokens_in"],
        FOCUSED_CHAT_USAGE["tokens_out"],
    )
    analysis_cost = observed_cost_usd(
        FOCUSED_ANALYSIS_USAGE["model"],
        FOCUSED_ANALYSIS_USAGE["tokens_in"],
        FOCUSED_ANALYSIS_USAGE["tokens_out"],
    )

    assert chat_row == (
        "langchain",
        "openai/gpt-4o-mini https://smith.langchain.com/r/chat-replay-1939",
        200,
        chat_bytes,
        FOCUSED_CHAT_USAGE["latency_ms"],
        chat_cost,
    )
    assert analysis_row == (
        "langchain",
        "anthropic/claude-sonnet-4-20250514",
        200,
        analysis_bytes,
        FOCUSED_ANALYSIS_USAGE["latency_ms"],
        analysis_cost,
    )
    assert legacy_placeholder_usage_bytes() != chat_row[3]

    log_llm_usage(
        conn,
        provider="openai",
        model="gpt-4o-mini",
        tokens_in=None,
        tokens_out=None,
        latency_ms=1,
    )
    unknown_row = conn.execute(
        "SELECT bytes, cost_usd FROM api_usage ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    assert unknown_row == (USAGE_BYTES_UNKNOWN, None)
    assert conn.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 3

    chat_fleet = fleet.record_chat_event(
        context=fleet.ChatFleetContext(
            run_id="chat-replay-1939",
            request_id="req-chat-1939",
            endpoint="/api/chat",
            chain="nl_query",
            provider=FOCUSED_CHAT_USAGE["provider"],
            model=FOCUSED_CHAT_USAGE["model"],
            trace_url=FOCUSED_CHAT_USAGE["trace_url"],
        ),
        latency_ms=FOCUSED_CHAT_USAGE["latency_ms"],
        http_status=200,
        token_usage=fleet.TokenUsage(
            input_tokens=FOCUSED_CHAT_USAGE["tokens_in"],
            output_tokens=FOCUSED_CHAT_USAGE["tokens_out"],
            total_tokens=chat_bytes,
        ),
        cost_usd=chat_cost,
        response_id="chat-replay-1939",
        artifact_path=fleet_path,
    )
    analysis_fleet = fleet.build_chat_fleet_record(
        context=fleet.ChatFleetContext(
            run_id="analysis-replay-1939",
            request_id="req-analysis-1939",
            endpoint="/api/chat/holdings-analysis",
            chain="holdings_analysis",
            provider=FOCUSED_ANALYSIS_USAGE["provider"],
            model=FOCUSED_ANALYSIS_USAGE["model"],
        ),
        latency_ms=FOCUSED_ANALYSIS_USAGE["latency_ms"],
        http_status=200,
        token_usage=fleet.TokenUsage(
            input_tokens=FOCUSED_ANALYSIS_USAGE["tokens_in"],
            output_tokens=FOCUSED_ANALYSIS_USAGE["tokens_out"],
            total_tokens=analysis_bytes,
        ),
        cost_usd=analysis_cost,
    )
    missing_usage_fleet = fleet.build_chat_fleet_record(
        context=fleet.ChatFleetContext(
            run_id="missing-usage-1939",
            request_id="req-missing-1939",
            endpoint="/api/chat",
            chain="nl_query",
        ),
        latency_ms=1,
        http_status=200,
        token_usage=fleet.TokenUsage(),
    )

    assert chat_fleet["domain"]["input_tokens"] == FOCUSED_CHAT_USAGE["tokens_in"]
    assert chat_fleet["domain"]["output_tokens"] == FOCUSED_CHAT_USAGE["tokens_out"]
    assert chat_fleet["domain"]["cost_usd"] == chat_cost
    assert analysis_fleet["domain"]["total_tokens"] == analysis_bytes
    assert "input_tokens" not in missing_usage_fleet["domain"]
    assert "cost_usd" not in missing_usage_fleet["domain"]

    records = [
        json.loads(line)
        for line in Path(fleet_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(records) == 1
    assert records[0]["run_id"] == "chat-replay-1939"
    conn.close()
