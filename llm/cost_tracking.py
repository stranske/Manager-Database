"""Helpers for logging LLM usage into api_usage."""

from __future__ import annotations

import logging
import math
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Literal

from langchain_core.callbacks import BaseCallbackHandler

from adapters.base import connect_db, ensure_api_usage_schema, get_placeholder, is_sqlite

logger = logging.getLogger(__name__)

_MODEL_PRICING_PER_1K_TOKENS: dict[str, tuple[float, float]] = {
    "gpt-4o-mini": (0.00015, 0.0006),
    "claude-sonnet-4-20250514": (0.003, 0.015),
    "claude-sonnet-5-5": (0.002, 0.010),
}

# Cached prompt tokens per 1k (billed below full input rate when the provider reports cache hits).
_MODEL_CACHED_INPUT_PER_1K_TOKENS: dict[str, float] = {
    "gpt-4o-mini": 0.000075,
    "claude-sonnet-4-20250514": 0.0003,
    "claude-sonnet-5-5": 0.0002,
}

# Pre-1939 ``log_llm_usage`` ignored provider token counts and always stored zero bytes.
USAGE_BYTES_UNKNOWN = -1

UNKNOWN_PROVIDER = "unknown"


def legacy_placeholder_usage_bytes() -> int:
    """Return the bytes value persisted before issue #1939 (always zero)."""
    return 0


def observed_usage_bytes(tokens_in: int | None, tokens_out: int | None) -> int:
    """Map observed token counts to the ``api_usage.bytes`` column."""
    if tokens_in is None or tokens_out is None:
        return USAGE_BYTES_UNKNOWN
    return int(tokens_in) + int(tokens_out)


def estimate_cost_usd(
    model: str,
    tokens_in: int,
    tokens_out: int,
    *,
    tokens_cached_in: int = 0,
) -> float:
    in_rate, out_rate = _MODEL_PRICING_PER_1K_TOKENS.get(model, (0.0, 0.0))
    cached_rate = _MODEL_CACHED_INPUT_PER_1K_TOKENS.get(model, in_rate)
    cached = max(min(tokens_cached_in, tokens_in), 0)
    fresh_in = tokens_in - cached
    return round(
        (fresh_in / 1000.0) * in_rate
        + (cached / 1000.0) * cached_rate
        + (tokens_out / 1000.0) * out_rate,
        6,
    )


def observed_cost_usd(
    model: str,
    tokens_in: int | None,
    tokens_out: int | None,
    reported_cost_usd: float | None = None,
    *,
    tokens_cached_in: int | None = None,
) -> float | None:
    """Return the provider-reported cost, else a priced estimate, else ``None`` (unknown)."""
    if reported_cost_usd is not None:
        cost = float(reported_cost_usd)
        if not math.isfinite(cost) or cost < 0:
            return None
        return round(cost, 6)
    if tokens_in is None or tokens_out is None:
        return None
    if model not in _MODEL_PRICING_PER_1K_TOKENS:
        return None
    return estimate_cost_usd(
        model,
        tokens_in,
        tokens_out,
        tokens_cached_in=tokens_cached_in or 0,
    )


@dataclass(frozen=True)
class ProviderUsage:
    """Usage a provider reported for one invocation; ``None`` means not reported."""

    tokens_in: int | None = None
    tokens_out: int | None = None
    cost_usd: float | None = None
    tokens_cached_in: int | None = None


def _as_count(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not value.is_integer():
        return None
    return int(value) if value >= 0 else None


def _as_cost(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    cost = float(value)
    if not math.isfinite(cost) or cost < 0:
        return None
    return cost


def _first_reported(sources: list[Mapping[str, Any]], keys: tuple[str, ...], convert: Any) -> Any:
    for source in sources:
        for key in keys:
            converted = convert(source.get(key))
            if converted is not None:
                return converted
    return None


def _cached_input_tokens(usage_metadata: Any) -> int | None:
    if not isinstance(usage_metadata, Mapping):
        return None
    details = usage_metadata.get("input_token_details")
    if not isinstance(details, Mapping):
        return None
    return _as_count(details.get("cache_read"))


def _usage_from_metadata(usage_metadata: Any, response_metadata: Any) -> ProviderUsage:
    sources: list[Mapping[str, Any]] = []
    if isinstance(usage_metadata, Mapping):
        sources.append(usage_metadata)
    if isinstance(response_metadata, Mapping):
        for nested in ("token_usage", "usage"):
            if isinstance(response_metadata.get(nested), Mapping):
                sources.append(response_metadata[nested])
        sources.append(response_metadata)
    return ProviderUsage(
        tokens_in=_first_reported(sources, ("input_tokens", "prompt_tokens"), _as_count),
        tokens_out=_first_reported(sources, ("output_tokens", "completion_tokens"), _as_count),
        cost_usd=_first_reported(sources, ("cost_usd", "cost", "total_cost"), _as_cost),
        tokens_cached_in=_cached_input_tokens(usage_metadata),
    )


def extract_provider_usage(response: Any) -> ProviderUsage:
    """Read reported token counts and cost from a provider message before it is parsed."""
    return _usage_from_metadata(
        getattr(response, "usage_metadata", None), getattr(response, "response_metadata", None)
    )


def _merge_provider_usage(left: ProviderUsage, right: ProviderUsage) -> ProviderUsage:
    return ProviderUsage(
        tokens_in=left.tokens_in if left.tokens_in is not None else right.tokens_in,
        tokens_out=left.tokens_out if left.tokens_out is not None else right.tokens_out,
        cost_usd=left.cost_usd if left.cost_usd is not None else right.cost_usd,
        tokens_cached_in=(
            left.tokens_cached_in if left.tokens_cached_in is not None else right.tokens_cached_in
        ),
    )


def extract_usage_from_llm_result(result: Any) -> ProviderUsage:
    """Read reported usage from a LangChain ``LLMResult`` (one provider invocation)."""
    merged = ProviderUsage()
    for generations in getattr(result, "generations", None) or []:
        for generation in generations:
            message = getattr(generation, "message", None)
            if message is None:
                continue
            merged = _merge_provider_usage(merged, extract_provider_usage(message))
    return _merge_provider_usage(
        merged, _usage_from_metadata(None, getattr(result, "llm_output", None))
    )


class ProviderUsageCollector(BaseCallbackHandler):
    """Collect usage at the provider boundary, before output parsers discard it.

    One ``on_llm_end`` event is one actual provider invocation, so a structured-output
    attempt and the text fallback that follows it are each observed exactly once.
    """

    def __init__(self) -> None:
        self.usages: list[ProviderUsage] = []

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        self.usages.append(extract_usage_from_llm_result(response))


@dataclass(frozen=True)
class ProviderObservation:
    """One provider invocation, shared by the persisted row and the fleet record."""

    invocation: Literal["chat", "analysis"]
    provider: str
    model: str
    tokens_in: int | None
    tokens_out: int | None
    cost_usd: float | None
    latency_ms: int
    trace_url: str | None = None
    request_id: str | None = None


@dataclass
class UsageCapture:
    """Correlation context for one chat request and the provider invocations it caused."""

    request_id: str
    provider: str | None = None
    model: str | None = None
    observations: list[ProviderObservation] = field(default_factory=list)

    def token_totals(self) -> tuple[int | None, int | None]:
        """Summed input/output tokens, or ``(None, None)`` if any invocation lacked usage."""
        if not self.observations or any(
            obs.tokens_in is None or obs.tokens_out is None for obs in self.observations
        ):
            return None, None
        return (
            sum(obs.tokens_in or 0 for obs in self.observations),
            sum(obs.tokens_out or 0 for obs in self.observations),
        )

    def total_cost_usd(self) -> float | None:
        """Summed cost, or ``None`` if any invocation's cost is unknown."""
        if not self.observations or any(obs.cost_usd is None for obs in self.observations):
            return None
        return round(sum(obs.cost_usd or 0.0 for obs in self.observations), 6)


_ACTIVE_CAPTURE: ContextVar[UsageCapture | None] = ContextVar("active_usage_capture", default=None)


@contextmanager
def capture_provider_usage(capture: UsageCapture) -> Iterator[UsageCapture]:
    """Make ``capture`` the correlation context for provider invocations in this block."""
    token = _ACTIVE_CAPTURE.set(capture)
    try:
        yield capture
    finally:
        _ACTIVE_CAPTURE.reset(token)


class _UnsetCost:
    pass


_UNSET_COST = _UnsetCost()


def record_provider_invocation(
    db_conn: Any | None,
    *,
    invocation: Literal["chat", "analysis"],
    usage: ProviderUsage | None,
    latency_ms: int,
    trace_url: str | None = None,
    provider: str | None = None,
    model: str | None = None,
) -> ProviderObservation:
    """Persist one observed provider invocation and register it for the fleet record.

    A failure to persist is logged and never raised: usage accounting must not break the
    user's answer.
    """
    capture = _ACTIVE_CAPTURE.get()
    resolved_provider = provider or (capture.provider if capture else None) or UNKNOWN_PROVIDER
    resolved_model = model or (capture.model if capture else None) or UNKNOWN_PROVIDER
    reported = usage or ProviderUsage()
    request_id = capture.request_id if capture else None
    observation = ProviderObservation(
        invocation=invocation,
        provider=resolved_provider,
        model=resolved_model,
        tokens_in=reported.tokens_in,
        tokens_out=reported.tokens_out,
        cost_usd=observed_cost_usd(
            resolved_model,
            reported.tokens_in,
            reported.tokens_out,
            reported.cost_usd,
            tokens_cached_in=reported.tokens_cached_in,
        ),
        latency_ms=latency_ms,
        trace_url=trace_url,
        request_id=request_id,
    )
    try:
        persist_observed_provider_usage(
            db_conn,
            invocation=invocation,
            provider=resolved_provider,
            model=resolved_model,
            tokens_in=reported.tokens_in,
            tokens_out=reported.tokens_out,
            latency_ms=latency_ms,
            trace_url=trace_url,
            reported_cost_usd=reported.cost_usd,
            resolved_cost_usd=observation.cost_usd,
            request_id=request_id,
        )
    except Exception:
        logger.warning("Failed to persist LLM usage for %s invocation", invocation, exc_info=True)
    if capture is not None:
        capture.observations.append(observation)
    return observation


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
    reported_cost_usd: float | None = None,
    resolved_cost_usd: float | None | _UnsetCost = _UNSET_COST,
    request_id: str | None = None,
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
        reported_cost_usd=reported_cost_usd,
        resolved_cost_usd=resolved_cost_usd,
        request_id=request_id,
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
    reported_cost_usd: float | None = None,
    resolved_cost_usd: float | None | _UnsetCost = _UNSET_COST,
    request_id: str | None = None,
) -> None:
    """Insert one ``api_usage`` row.

    An owned connection (``db_conn is None``) is committed (SQLite; Postgres connections
    from ``connect_db`` autocommit) and closed. A borrowed connection is left open and its
    transaction is left to the caller.
    """
    conn = db_conn or connect_db()
    owns_connection = db_conn is None
    try:
        endpoint = f"{provider}/{model}"
        if request_id is not None:
            endpoint = f"{endpoint} request_id={request_id}"
        if trace_url is not None:
            endpoint = f"{endpoint} {trace_url}"
        usage_bytes = observed_usage_bytes(tokens_in, tokens_out)
        if isinstance(resolved_cost_usd, _UnsetCost):
            cost_usd = observed_cost_usd(model, tokens_in, tokens_out, reported_cost_usd)
        elif resolved_cost_usd is None:
            cost_usd = None
        else:
            cost_usd = observed_cost_usd(model, tokens_in, tokens_out, resolved_cost_usd)
        ph = get_placeholder(conn)
        ensure_api_usage_schema(conn)
        conn.execute(
            f"INSERT INTO api_usage(source, endpoint, status, bytes, latency_ms, cost_usd) VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
            ("langchain", endpoint, 200, usage_bytes, latency_ms, cost_usd),
        )
        if owns_connection and is_sqlite(conn):
            conn.commit()
    finally:
        if owns_connection:
            conn.close()
