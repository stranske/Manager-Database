"""Centralized LangSmith fleet record emission for Manager-Database AI surfaces.

The shared `langsmith-fleet/v1` schema is owned by `stranske/Workflows#2150`;
this module emits Manager-Database-specific records for the chat-api surface so
the Workflows dashboard can correlate endpoint behavior, model/provider quality,
latency, cost, request context, and feedback across calls.

Records intentionally avoid raw user questions, prompt bodies, response
payloads, raw session identifiers, and credential material. They keep only
stable IDs (hashed where derived from user-bearing inputs), counts, durations,
statuses, and bounded domain metadata.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Final, Literal

SCHEMA_VERSION: Final = "langsmith-fleet/v1"
REPO: Final = "stranske/Manager-Database"
SURFACE: Final = "chat-api"
GITHUB_ISSUE: Final = "stranske/Manager-Database#1048"
ARTIFACT_NAME: Final = "langsmith-fleet.ndjson"
DEFAULT_PROJECT: Final = "manager-database"
ENV_LANGSMITH_KEY: Final = "LANGSMITH_API_KEY"
ENV_LANGCHAIN_PROJECT: Final = "LANGCHAIN_PROJECT"
ENV_LANGSMITH_PROJECT: Final = "LANGSMITH_PROJECT"
ENV_LANGCHAIN_TRACING_V2: Final = "LANGCHAIN_TRACING_V2"
ENV_LANGCHAIN_API_KEY: Final = "LANGCHAIN_API_KEY"
ENV_FLEET_PATH: Final = "MANAGER_DATABASE_LANGSMITH_FLEET_PATH"

CHAT_OPERATION: Final = "chat-turn"
FEEDBACK_OPERATION: Final = "chat-feedback"

CHAIN_TO_WORKFLOW_TAG: Final[Mapping[str, str]] = {
    "filing_summary": "filing-summary",
    "holdings_analysis": "holdings-analysis",
    "nl_query": "nl-query",
    "rag_search": "rag-search",
}

Status = Literal["success", "error", "fallback", "no_secret"]


@dataclass(frozen=True, slots=True)
class TokenUsage:
    """Token accounting captured from a chain response when available."""

    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None


@dataclass(frozen=True, slots=True)
class ChatFleetContext:
    """Shared correlation context for one chat-api turn."""

    run_id: str
    request_id: str
    endpoint: str
    chain: str
    session_id: str | None = None
    provider: str | None = None
    model: str | None = None
    trace_id: str | None = None
    trace_url: str | None = None
    github_pr: str | None = None
    recorded_at: str | None = None


def ensure_langsmith_project_defaults() -> bool:
    """Apply Manager-Database LangSmith defaults when a key is present.

    Returns ``True`` when ``LANGSMITH_API_KEY`` is set; mirrors
    ``llm.tracing.maybe_enable_langsmith_tracing`` but is safe to call without
    mutating module-level cache state (so fleet emission stays deterministic in
    tests that wipe environment between cases).
    """

    api_key = os.environ.get(ENV_LANGSMITH_KEY, "").strip()
    if not api_key:
        return False
    os.environ.setdefault(ENV_LANGCHAIN_TRACING_V2, "true")
    os.environ.setdefault(ENV_LANGCHAIN_PROJECT, DEFAULT_PROJECT)
    os.environ.setdefault(ENV_LANGSMITH_PROJECT, DEFAULT_PROJECT)
    os.environ.setdefault(ENV_LANGCHAIN_API_KEY, api_key)
    return True


def build_chat_fleet_record(
    *,
    context: ChatFleetContext,
    latency_ms: int,
    http_status: int,
    error_category: str | None = None,
    fallback_reason: str | None = None,
    rate_limited: bool = False,
    token_usage: TokenUsage | None = None,
    cost_usd: float | None = None,
    response_id: str | None = None,
    evaluation_score: float | None = None,
    artifact_ref: str | None = None,
) -> dict[str, Any]:
    """Build one Workflows-compatible ``langsmith-fleet/v1`` record for a chat turn."""

    tracing_enabled = ensure_langsmith_project_defaults()
    status = _resolve_chat_status(
        http_status=http_status,
        error_category=error_category,
        fallback_reason=fallback_reason,
        rate_limited=rate_limited,
        tracing_enabled=tracing_enabled,
    )
    workflow_tag = CHAIN_TO_WORKFLOW_TAG.get(context.chain, context.chain)

    domain: dict[str, Any] = {
        "endpoint": context.endpoint,
        "chain": context.chain,
        "workflow": workflow_tag,
        "request_id_hash": _hash_identifier(context.request_id),
        "session_id_hash": (
            _hash_identifier(context.session_id) if context.session_id else None
        ),
        "latency_ms": int(latency_ms),
        "http_status": int(http_status),
        "rate_limited": bool(rate_limited),
        "fallback_state": fallback_reason or "none",
        "error_state": error_category or "none",
        "input_tokens": token_usage.input_tokens if token_usage else None,
        "output_tokens": token_usage.output_tokens if token_usage else None,
        "total_tokens": token_usage.total_tokens if token_usage else None,
        "cost_usd": cost_usd,
        "evaluation_score": evaluation_score,
        "response_id": response_id,
    }
    return _record(
        context=context,
        operation=CHAT_OPERATION,
        status=status,
        recorded_at=context.recorded_at or _utc_timestamp(),
        domain=domain,
        error_code=error_category,
        artifact_ref=artifact_ref,
    )


def build_feedback_fleet_record(
    *,
    response_id: str,
    feedback_id: int | str,
    rating: int,
    chain: str | None = None,
    endpoint: str = "/api/chat/feedback",
    session_id: str | None = None,
    forwarded_to_langsmith: bool | None = None,
    recorded_at: str | None = None,
) -> dict[str, Any]:
    """Build a feedback-correlation fleet record joinable on ``response_id``."""

    tracing_enabled = ensure_langsmith_project_defaults()
    if forwarded_to_langsmith is False:
        status: Status = "fallback"
    elif tracing_enabled:
        status = "success"
    else:
        status = "no_secret"

    context = ChatFleetContext(
        run_id=response_id,
        request_id=response_id,
        endpoint=endpoint,
        chain=chain or "feedback",
        session_id=session_id,
        recorded_at=recorded_at,
    )
    workflow_tag = CHAIN_TO_WORKFLOW_TAG.get(chain or "", "feedback")
    domain: dict[str, Any] = {
        "endpoint": endpoint,
        "chain": chain,
        "workflow": workflow_tag,
        "feedback_id": str(feedback_id),
        "rating": int(rating),
        "response_id": response_id,
        "session_id_hash": (
            _hash_identifier(session_id) if session_id else None
        ),
        "forwarded_to_langsmith": (
            None if forwarded_to_langsmith is None else bool(forwarded_to_langsmith)
        ),
    }
    return _record(
        context=context,
        operation=FEEDBACK_OPERATION,
        status=status,
        recorded_at=context.recorded_at or _utc_timestamp(),
        domain=domain,
        error_code=None,
        artifact_ref=None,
    )


def default_fleet_artifact_path() -> Path:
    """Return the default NDJSON artifact path for Manager-Database fleet records."""

    override = os.environ.get(ENV_FLEET_PATH, "").strip()
    if override:
        return Path(override).expanduser()
    root = _project_root(Path(__file__).resolve())
    return root / "artifacts" / "langsmith" / ARTIFACT_NAME


def append_fleet_records(
    path: Path,
    records: Iterable[Mapping[str, Any]],
    *,
    retention_limit: int = 2_000,
) -> Path:
    """Append fleet records as NDJSON with bounded local retention.

    The on-disk artifact is consumed by the Workflows fleet dashboard ingestion.
    Retention is bounded so a single repo never produces an unbounded artifact
    in CI; the most recent ``retention_limit`` lines are preserved.
    """

    if retention_limit < 1:
        raise ValueError("retention_limit must be >= 1")
    materialized = [dict(record) for record in records]
    if not materialized:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl_module: ModuleType | None = None
        with suppress(ImportError):
            import fcntl as fcntl_module
        if fcntl_module is not None:
            with suppress(OSError):
                fcntl_module.flock(handle.fileno(), fcntl_module.LOCK_EX)
        for record in materialized:
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        handle.seek(0)
        lines = handle.read().splitlines()
        if len(lines) > retention_limit:
            trimmed = lines[-retention_limit:]
            temp_path = path.with_suffix(path.suffix + ".tmp")
            temp_path.write_text("\n".join(trimmed) + "\n", encoding="utf-8")
            temp_path.replace(path)
        if fcntl_module is not None:
            with suppress(Exception):
                fcntl_module.flock(handle.fileno(), fcntl_module.LOCK_UN)
    return path


def record_chat_event(
    *,
    context: ChatFleetContext,
    latency_ms: int,
    http_status: int,
    error_category: str | None = None,
    fallback_reason: str | None = None,
    rate_limited: bool = False,
    token_usage: TokenUsage | None = None,
    cost_usd: float | None = None,
    response_id: str | None = None,
    evaluation_score: float | None = None,
    artifact_path: Path | None = None,
) -> dict[str, Any]:
    """Build and persist a chat-turn fleet record; return the persisted record."""

    record = build_chat_fleet_record(
        context=context,
        latency_ms=latency_ms,
        http_status=http_status,
        error_category=error_category,
        fallback_reason=fallback_reason,
        rate_limited=rate_limited,
        token_usage=token_usage,
        cost_usd=cost_usd,
        response_id=response_id,
        evaluation_score=evaluation_score,
    )
    target = artifact_path or default_fleet_artifact_path()
    append_fleet_records(target, [record])
    return record


def record_feedback_event(
    *,
    response_id: str,
    feedback_id: int | str,
    rating: int,
    chain: str | None = None,
    endpoint: str = "/api/chat/feedback",
    session_id: str | None = None,
    forwarded_to_langsmith: bool | None = None,
    artifact_path: Path | None = None,
) -> dict[str, Any]:
    """Build and persist a feedback fleet record; return the persisted record."""

    record = build_feedback_fleet_record(
        response_id=response_id,
        feedback_id=feedback_id,
        rating=rating,
        chain=chain,
        endpoint=endpoint,
        session_id=session_id,
        forwarded_to_langsmith=forwarded_to_langsmith,
    )
    target = artifact_path or default_fleet_artifact_path()
    append_fleet_records(target, [record])
    return record


def _resolve_chat_status(
    *,
    http_status: int,
    error_category: str | None,
    fallback_reason: str | None,
    rate_limited: bool,
    tracing_enabled: bool,
) -> Status:
    if rate_limited or (error_category and http_status >= 400):
        # 4xx with an explicit error category is a real error from the caller's
        # perspective even though the provider may not have been invoked.
        if rate_limited and http_status == 429:
            return "fallback"
        return "error"
    if http_status >= 500 or (error_category and error_category != "none"):
        return "error"
    if fallback_reason:
        return "fallback"
    if not tracing_enabled:
        return "no_secret"
    return "success"


def _record(
    *,
    context: ChatFleetContext,
    operation: str,
    status: Status,
    recorded_at: str,
    domain: Mapping[str, Any],
    error_code: str | None,
    artifact_ref: str | None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "repo": REPO,
        "surface": SURFACE,
        "operation": operation,
        "run_id": context.run_id,
        "status": status,
        "github_issue": GITHUB_ISSUE,
        "recorded_at": recorded_at,
        "domain": {k: v for k, v in domain.items() if v is not None},
    }
    if context.github_pr:
        record["github_pr"] = context.github_pr
    if context.provider:
        record["provider"] = context.provider
    if context.model:
        record["model"] = context.model
    if context.trace_id:
        record["trace_id"] = context.trace_id
    if context.trace_url:
        record["trace_url"] = context.trace_url
    if artifact_ref:
        record["artifact_ref"] = artifact_ref
    if error_code and status == "error":
        record["error_category"] = error_code
    return record


def _hash_identifier(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _project_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").exists() or (candidate / ".git").exists():
            return candidate
    return Path.cwd()


def _utc_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
