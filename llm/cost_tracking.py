"""Helpers for logging LLM usage into api_usage."""

from __future__ import annotations

from typing import Any, Literal

from adapters.base import connect_db, ensure_api_usage_schema, get_placeholder, is_sqlite

_MODEL_PRICING_PER_1K_TOKENS: dict[str, tuple[float, float]] = {
    "gpt-4o-mini": (0.00015, 0.0006),
    "claude-sonnet-4-20250514": (0.003, 0.015),
    "claude-sonnet-5-5": (0.002, 0.010),
}

# Pre-1939 ``log_llm_usage`` ignored provider token counts and always stored zero bytes.
USAGE_BYTES_UNKNOWN = -1


def legacy_placeholder_usage_bytes() -> int:
    """Return the bytes value persisted before issue #1939 (always zero)."""
    return 0


def observed_usage_bytes(tokens_in: int | None, tokens_out: int | None) -> int:
    """Map observed token counts to the ``api_usage.bytes`` column."""
    if tokens_in is None or tokens_out is None:
        return USAGE_BYTES_UNKNOWN
    return int(tokens_in) + int(tokens_out)


def estimate_cost_usd(model: str, tokens_in: int, tokens_out: int) -> float:
    in_rate, out_rate = _MODEL_PRICING_PER_1K_TOKENS.get(model, (0.0, 0.0))
    return round((tokens_in / 1000.0) * in_rate + (tokens_out / 1000.0) * out_rate, 6)


def observed_cost_usd(model: str, tokens_in: int | None, tokens_out: int | None) -> float | None:
    if tokens_in is None or tokens_out is None:
        return None
    return estimate_cost_usd(model, tokens_in, tokens_out)


def persist_observed_provider_usage(
    db_conn: Any | None,
    *,
    invocation: Literal["chat", "analysis"],
    provider: str,
    model: str,
    tokens_in: int | None,
    tokens_out: int | None,
    latency_ms: int,
    trace_url: str | None = None,
) -> None:
    """Supported entry point for persisting one chat or analysis provider observation."""
    _ = invocation
    log_llm_usage(
        db_conn,
        provider=provider,
        model=model,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        latency_ms=latency_ms,
        trace_url=trace_url,
    )


def log_llm_usage(
    db_conn: Any | None,
    *,
    provider: str,
    model: str,
    tokens_in: int | None,
    tokens_out: int | None,
    latency_ms: int,
    trace_url: str | None = None,
) -> None:
    conn = db_conn or connect_db()
    owns_connection = db_conn is None
    endpoint = f"{provider}/{model}"
    usage_bytes = observed_usage_bytes(tokens_in, tokens_out)
    cost_usd = observed_cost_usd(model, tokens_in, tokens_out)
    ph = get_placeholder(conn)
    ensure_api_usage_schema(conn)
    conn.execute(
        f"INSERT INTO api_usage(source, endpoint, status, bytes, latency_ms, cost_usd) VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
        (
            "langchain",
            endpoint if trace_url is None else f"{endpoint} {trace_url}",
            200,
            usage_bytes,
            latency_ms,
            cost_usd,
        ),
    )
    if is_sqlite(conn):
        conn.commit()
    if owns_connection:
        conn.close()
