from __future__ import annotations

import sqlite3

from llm.cost_tracking import estimate_cost_usd, log_llm_usage


def test_estimate_cost_usd_uses_model_pricing():
    assert estimate_cost_usd("gpt-4o-mini", 1000, 500) == 0.00045
    assert estimate_cost_usd("unknown-model", 1000, 500) == 0.0


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
