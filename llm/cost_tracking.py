"""Helpers for logging LLM usage into api_usage."""

from __future__ import annotations

import sqlite3
from typing import Any

from adapters.base import connect_db

_MODEL_PRICING_PER_1K_TOKENS: dict[str, tuple[float, float]] = {
    "gpt-4o-mini": (0.00015, 0.0006),
    "claude-sonnet-4-20250514": (0.003, 0.015),
}


def _placeholder(conn: Any) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def estimate_cost_usd(model: str, tokens_in: int, tokens_out: int) -> float:
    in_rate, out_rate = _MODEL_PRICING_PER_1K_TOKENS.get(model, (0.0, 0.0))
    return round((tokens_in / 1000.0) * in_rate + (tokens_out / 1000.0) * out_rate, 6)


def _ensure_api_usage_table(conn: Any) -> None:
    if isinstance(conn, sqlite3.Connection):
        conn.execute("""CREATE TABLE IF NOT EXISTS api_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT,
                endpoint TEXT,
                status INT,
                bytes INT,
                latency_ms INT,
                cost_usd REAL
            )""")
        return

    conn.execute("""CREATE TABLE IF NOT EXISTS api_usage (
            id bigserial PRIMARY KEY,
            ts timestamptz DEFAULT now(),
            source text,
            endpoint text,
            status int,
            bytes int,
            latency_ms int,
            cost_usd numeric(10,4)
        )""")


def log_llm_usage(
    db_conn: Any | None,
    *,
    provider: str,
    model: str,
    tokens_in: int,
    tokens_out: int,
    latency_ms: int,
    trace_url: str | None = None,
) -> None:
    conn = db_conn or connect_db()
    owns_connection = db_conn is None
    endpoint = f"{provider}/{model}"
    cost_usd = estimate_cost_usd(model, tokens_in, tokens_out)
    ph = _placeholder(conn)
    _ensure_api_usage_table(conn)
    conn.execute(
        f"INSERT INTO api_usage(source, endpoint, status, bytes, latency_ms, cost_usd) VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
        (
            "langchain",
            endpoint if trace_url is None else f"{endpoint} {trace_url}",
            200,
            0,
            latency_ms,
            cost_usd,
        ),
    )
    if isinstance(conn, sqlite3.Connection):
        conn.commit()
    if owns_connection:
        conn.close()
