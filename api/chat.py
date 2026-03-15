"""Minimal FastAPI app providing /chat and health endpoints."""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import sqlite3
import time
import uuid
from collections import defaultdict, deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Lock
from typing import Any, cast

import boto3
from botocore.config import Config as BotoConfig
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Histogram, generate_latest
from pydantic import BaseModel, ConfigDict, Field

from adapters.base import connect_db
from api.activism import router as activism_router
from api.alerts import router as alerts_router
from api.data import router as data_router
from api.managers import router as managers_router
from api.memory_profiler import start_memory_profiler, stop_memory_profiler
from api.search import SearchEntityType, SearchResult, universal_search
from api.signals import router as signals_router
from llm.tracing import maybe_enable_langsmith_tracing

app = FastAPI()
# Tag manager endpoints so they group clearly in the Swagger UI.
app.include_router(managers_router, tags=["Managers"])
app.include_router(data_router, tags=["Data"])
app.include_router(alerts_router, tags=["Alerts"])
app.include_router(activism_router, tags=["Activism"])
app.include_router(signals_router, tags=["Signals"])
_APP_EXECUTOR = ThreadPoolExecutor(max_workers=4)
# Allow concurrent health checks without serializing every dependency probe.
_HEALTH_EXECUTOR = ThreadPoolExecutor(max_workers=3)


@dataclass(frozen=True)
class HealthClock:
    """Centralize time sources to enable deterministic health check tests."""

    perf_counter: Callable[[], float]
    monotonic: Callable[[], float]
    sleep: Callable[[float], None]


HEALTH_CLOCK = HealthClock(time.perf_counter, time.monotonic, time.sleep)
APP_START_TIME = HEALTH_CLOCK.monotonic()
HEALTH_CHECK_DURATION = Histogram(
    "health_check_duration_seconds",
    "Health check latency by endpoint.",
    labelnames=("endpoint",),
)
logger = logging.getLogger(__name__)


def get_health_executor():
    """Return a working health executor, creating a new one if needed."""
    global _HEALTH_EXECUTOR
    if _HEALTH_EXECUTOR._shutdown:
        _HEALTH_EXECUTOR = ThreadPoolExecutor(max_workers=3)
    return _HEALTH_EXECUTOR


class CircuitBreaker:
    """Track consecutive failures and open after a threshold."""

    def __init__(
        self,
        failure_threshold: int = 3,
        reset_timeout_s: float = 30.0,
        monotonic_fn: Callable[[], float] | None = None,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._reset_timeout_s = reset_timeout_s
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        # Allow tests to inject a deterministic clock without patching globals.
        self._monotonic = monotonic_fn or HEALTH_CLOCK.monotonic
        self._lock = Lock()

    def is_open(self) -> bool:
        """Return True when the breaker is open and requests should be skipped."""
        with self._lock:
            if self._opened_at is None:
                return False
            if (self._monotonic() - self._opened_at) >= self._reset_timeout_s:
                self._opened_at = None
                self._consecutive_failures = 0
                return False
            return True

    def record_success(self) -> None:
        """Reset the breaker after a successful call."""
        with self._lock:
            self._consecutive_failures = 0
            self._opened_at = None

    def record_failure(self) -> None:
        """Track a failure and open the breaker if needed."""
        with self._lock:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._failure_threshold:
                if self._opened_at is None:
                    self._opened_at = self._monotonic()


def _circuit_breaker_reset_seconds() -> float:
    """Return the circuit breaker reset timeout in seconds."""
    return max(float(os.getenv("HEALTH_CIRCUIT_RESET_S", "30")), 1.0)


_MINIO_CIRCUIT = CircuitBreaker(reset_timeout_s=_circuit_breaker_reset_seconds())
_REDIS_CIRCUIT = CircuitBreaker(reset_timeout_s=_circuit_breaker_reset_seconds())
_HEALTH_RETRY_BACKOFFS = (0.1, 0.2, 0.4)


# Keep APP_EXECUTOR for backwards compatibility
APP_EXECUTOR = _APP_EXECUTOR


class ChatResponse(BaseModel):
    """Response payload for chat responses."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"answer": ("Context: The latest holdings update was filed on 2024-06-30.")}
            ]
        }
    )
    answer: str = Field(..., description="Generated answer based on stored documents")
    chain_used: str = Field(
        "legacy_search",
        description="Identifier of the chain or handler used to produce the answer",
    )
    sources: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Supporting references used to generate the answer",
    )
    sql: str | None = Field(None, description="Generated SQL, when applicable")
    trace_url: str | None = Field(None, description="Optional trace URL for observability")
    latency_ms: int = Field(0, description="End-to-end request latency in milliseconds")
    response_id: str = Field(..., description="Stable identifier used for feedback submission")


class ChatRequest(BaseModel):
    """Request payload for the research assistant chat endpoint."""

    question: str
    chain: str | None = None
    context: dict[str, Any] | None = None


class FeedbackRequest(BaseModel):
    """User feedback captured for a chat response."""

    response_id: str
    rating: int = Field(..., ge=1, le=5)
    comment: str | None = None


class HoldingsAnalysisRequest(BaseModel):
    """Request payload for direct holdings analysis endpoint."""

    question: str = "Analyze current holdings concentration and crowding."
    context: dict[str, Any] | None = None


class HealthDbResponse(BaseModel):
    """Response payload for database health checks."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"healthy": True, "latency_ms": 42},
                {"healthy": False, "latency_ms": 5000},
            ]
        }
    )
    healthy: bool = Field(..., description="Whether the database is reachable")
    latency_ms: int = Field(..., description="Observed database ping latency in milliseconds")


class HealthDetailedResponse(BaseModel):
    """Response payload for detailed health checks."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "healthy": True,
                    "uptime_s": 3600,
                    "components": {
                        "app": {"healthy": True, "uptime_s": 3600},
                        "database": {"healthy": True, "latency_ms": 42},
                        "minio": {"healthy": True, "latency_ms": 58},
                        "redis": {"healthy": True, "latency_ms": 12, "enabled": True},
                    },
                }
            ]
        }
    )
    healthy: bool = Field(..., description="Whether all components are healthy")
    uptime_s: int = Field(..., description="Application uptime in seconds")
    components: dict[str, dict[str, int | bool]] = Field(
        ..., description="Per-component health details"
    )


def _format_validation_errors(exc: RequestValidationError) -> list[dict[str, str]]:
    """Normalize validation errors into a concise field/message list."""
    errors: list[dict[str, str]] = []
    for err in exc.errors():
        raw_loc = [str(part) for part in err.get("loc", [])]
        loc = [part for part in raw_loc if part != "body"]
        if loc:
            field = loc[-1]
        elif raw_loc and raw_loc[0] == "body":
            field = "body"
        else:
            field = "unknown"
        errors.append({"field": field, "message": err.get("msg", "Invalid value")})
    return errors


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(
    _request: Request, exc: RequestValidationError
) -> JSONResponse:
    # Return 400s with field-level messages for API clients.
    errors = _format_validation_errors(exc)
    return JSONResponse(status_code=400, content={"errors": errors, "error": errors})


# OpenAPI metadata keeps /docs clear about chat behavior.
@app.get(
    "/chat",
    response_model=ChatResponse,
    summary="Answer a chat query",
    description=(
        "Search stored documents using the provided question and return a concise "
        "answer composed from the matching context."
    ),
)
def chat(
    q: str = Query(
        ...,
        description="User question",
        examples=cast(
            Any,
            {
                "basic": {
                    "summary": "Holdings question",
                    "value": "What is the latest holdings update?",
                }
            },
        ),
    ),
):
    """Return a naive answer built from stored documents."""
    # Import here to avoid loading embedding models during unrelated endpoints/tests.
    from embeddings import search_documents

    hits = search_documents(q)
    if not hits:
        answer = "No documents found."
    else:
        snippets = []
        for hit in hits:
            kind = hit.get("kind") or "note"
            filename = hit.get("filename") or "unknown"
            manager_name = hit.get("manager_name") or "unassigned"
            snippets.append(f"[{kind} | {filename} | manager: {manager_name}] {hit['content']}")
        answer = "Context: " + " ".join(snippets)
    return {
        "answer": answer,
        "latency_ms": 0,
        "chain_used": "legacy_search",
        "sources": [],
        "sql": None,
        "trace_url": None,
        "response_id": str(uuid.uuid4()),
    }


_SEARCH_ENTITY_TYPE_QUERY = Query(
    None,
    description="Optional result type filter",
)


@app.get(
    "/api/search",
    response_model=list[SearchResult],
    summary="Universal search across managers, filings, holdings, news, and documents",
    description=(
        "Run a unified search query and return ranked results across supported entity types."
    ),
)
async def search_api(
    q: str = Query(
        ...,
        min_length=1,
        description="Search query text",
        examples=cast(
            Any,
            {
                "manager": {
                    "summary": "Manager query",
                    "value": "Elliott",
                }
            },
        ),
    ),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results to return"),
    entity_type: SearchEntityType | None = _SEARCH_ENTITY_TYPE_QUERY,
) -> list[SearchResult]:
    conn = connect_db()
    try:
        return universal_search(q, conn, limit=limit, entity_type=entity_type)
    finally:
        conn.close()


VALID_CHAIN_NAMES = {
    "auto",
    "filing_summary",
    "holdings_analysis",
    "nl_query",
    "rag_search",
}
DIRECT_CHAIN_PATHS = {
    "filing_summary": ("chains.filing_summary", "FilingSummaryChain"),
    "holdings_analysis": ("chains.holdings_analysis", "HoldingsAnalysisChain"),
    "nl_query": ("chains.nl_query", "NLQueryChain"),
    "rag_search": ("chains.rag_search", "RAGSearchChain"),
}

_CHAT_RATE_LIMIT_PER_MINUTE = 10
_CHAT_RATE_LIMIT_WINDOW_SECONDS = 60.0


class InMemoryChatRateLimiter:
    """Simple in-memory session limiter for chat endpoints."""

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def check_and_record(self, session_id: str, now: float | None = None) -> bool:
        """Return True when request can proceed and persist request timestamp."""
        current_time = now if now is not None else time.time()
        window_start = current_time - self._window_seconds
        with self._lock:
            history = self._requests[session_id]
            while history and history[0] <= window_start:
                history.popleft()
            if len(history) >= self._max_requests:
                return False
            history.append(current_time)
            return True

    def clear(self) -> None:
        """Reset limiter state; used by tests."""
        with self._lock:
            self._requests.clear()


CHAT_RATE_LIMITER = InMemoryChatRateLimiter(
    max_requests=_CHAT_RATE_LIMIT_PER_MINUTE,
    window_seconds=_CHAT_RATE_LIMIT_WINDOW_SECONDS,
)


def _chat_session_id(request: Request | None) -> str:
    """Derive a stable session key from headers/cookies or client host."""
    if request is None:
        return "unknown"
    header_value = request.headers.get("x-session-id")
    if header_value:
        return f"header:{header_value}"
    cookie_value = request.cookies.get("session_id")
    if cookie_value:
        return f"cookie:{cookie_value}"
    if request.client and request.client.host:
        return f"client:{request.client.host}"
    return "unknown"


def _enforce_chat_rate_limit(request: Request | None) -> None:
    """Raise 429 when request count exceeds session budget."""
    session_id = _chat_session_id(request)
    if not CHAT_RATE_LIMITER.check_and_record(session_id):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")


class _PromptInjectionError(Exception):
    """Fallback PromptInjectionError when llm.injection is unavailable."""

    def __init__(self, reasons: list[str] | str) -> None:
        if isinstance(reasons, str):
            self.reasons = [reasons]
        else:
            self.reasons = reasons
        super().__init__(", ".join(self.reasons))


def _load_prompt_injection_error_class() -> type[Exception]:
    """Load PromptInjectionError from the expected module when available."""
    try:
        module = importlib.import_module("llm.injection")
        return module.PromptInjectionError
    except Exception:
        return _PromptInjectionError


PROMPT_INJECTION_ERROR = _load_prompt_injection_error_class()


def _build_chat_client_info():
    """Build chat client metadata from available provider modules."""
    llm_client_module = None
    try:
        llm_client_module = importlib.import_module("llm.client")
    except ModuleNotFoundError as exc:
        # Fall back only when the llm client module is not installed.
        if exc.name not in {"llm", "llm.client"}:
            raise

    if llm_client_module is not None:
        build_fn = getattr(llm_client_module, "build_chat_client", None)
        if callable(build_fn):
            return build_fn()
        return None

    from tools.langchain_client import build_chat_client

    return build_chat_client()


def _classify_intent(question: str) -> str:
    """Classify user intent or use a deterministic fallback classifier."""
    try:
        module = importlib.import_module("chains.intent")
        classify_intent = module.classify_intent
        chain_name = classify_intent(question)
        if chain_name in DIRECT_CHAIN_PATHS:
            return chain_name
    except Exception:
        pass

    lowered = question.lower()
    if "sql" in lowered or "query" in lowered or "database" in lowered:
        return "nl_query"
    if "filing" in lowered or "13f" in lowered:
        return "filing_summary"
    if "holding" in lowered or "position" in lowered:
        return "holdings_analysis"
    return "rag_search"


class _FallbackFilingSummaryChain:
    def run(self, *, question: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
        filing_id = (context or {}).get("filing_id")
        detail = f" for filing {filing_id}" if filing_id else ""
        return {"answer": f"Filing summary{detail}: {question}", "sources": []}


class _FallbackHoldingsAnalysisChain:
    def run(self, *, question: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"answer": f"Holdings analysis: {question}", "sources": context or {}}


class _FallbackNLQueryChain:
    def run(self, *, question: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
        sql = "SELECT manager_name, filing_date FROM filings ORDER BY filing_date DESC LIMIT 10;"
        return {"answer": f"Query analysis: {question}", "sql": sql, "sources": context or {}}


class _FallbackRAGSearchChain:
    def run(self, *, question: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"answer": f"Search results for: {question}", "sources": context or {}}


FALLBACK_CHAINS = {
    "filing_summary": _FallbackFilingSummaryChain,
    "holdings_analysis": _FallbackHoldingsAnalysisChain,
    "nl_query": _FallbackNLQueryChain,
    "rag_search": _FallbackRAGSearchChain,
}


def _build_chain(chain_name: str, client_info: Any):
    """Instantiate a chain by name, falling back to local stubs when missing."""
    module_path, class_name = DIRECT_CHAIN_PATHS[chain_name]
    try:
        module = importlib.import_module(module_path)
        chain_cls = getattr(module, class_name)
    except Exception:
        chain_cls = FALLBACK_CHAINS[chain_name]

    try:
        return chain_cls(client_info.client if client_info else None)
    except Exception:
        try:
            return chain_cls(client_info) if client_info else chain_cls()
        except Exception:
            return chain_cls()


def _normalize_sources(raw_sources: Any) -> list[dict[str, Any]]:
    if isinstance(raw_sources, list):
        normalized: list[dict[str, Any]] = []
        for source in raw_sources:
            if isinstance(source, dict):
                normalized.append(source)
            else:
                normalized.append({"description": str(source)})
        return normalized
    if isinstance(raw_sources, dict):
        return [raw_sources]
    return []


def _extract_chain_payload(result: Any) -> tuple[str, list[dict[str, Any]], str | None, str | None]:
    """Extract answer metadata from dict/object/string chain responses."""
    if isinstance(result, str):
        return result, [], None, None
    if isinstance(result, dict):
        answer = str(result.get("answer") or result.get("response") or "")
        sources = _normalize_sources(result.get("sources"))
        sql = result.get("sql")
        trace_url = result.get("trace_url")
        return answer, sources, sql, trace_url

    answer = str(getattr(result, "answer", "") or getattr(result, "response", ""))
    sources = _normalize_sources(getattr(result, "sources", []))
    sql = getattr(result, "sql", None)
    trace_url = getattr(result, "trace_url", None)
    return answer, sources, sql, trace_url


def _response_id_from_trace_url(trace_url: str | None) -> str:
    if trace_url:
        stripped = trace_url.rstrip("/")
        if "/" in stripped:
            candidate = stripped.rsplit("/", 1)[-1]
            if candidate:
                return candidate
    return str(uuid.uuid4())


def _placeholder(conn: Any) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def _ensure_chat_feedback_table(conn: Any) -> None:
    if isinstance(conn, sqlite3.Connection):
        conn.execute("""CREATE TABLE IF NOT EXISTS chat_feedback (
                feedback_id INTEGER PRIMARY KEY AUTOINCREMENT,
                response_id TEXT NOT NULL,
                rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
        return

    conn.execute("""CREATE TABLE IF NOT EXISTS chat_feedback (
            feedback_id bigserial PRIMARY KEY,
            response_id text NOT NULL,
            rating int NOT NULL CHECK (rating BETWEEN 1 AND 5),
            comment text,
            created_at timestamptz DEFAULT now()
        )""")


def _store_feedback(feedback: FeedbackRequest) -> int:
    conn = connect_db()
    try:
        _ensure_chat_feedback_table(conn)
        ph = _placeholder(conn)
        if isinstance(conn, sqlite3.Connection):
            cursor = conn.execute(
                f"INSERT INTO chat_feedback(response_id, rating, comment) VALUES ({ph}, {ph}, {ph})",
                (feedback.response_id, feedback.rating, feedback.comment),
            )
            conn.commit()
            return int(cursor.lastrowid or 0)

        row = conn.execute(
            f"INSERT INTO chat_feedback(response_id, rating, comment) VALUES ({ph}, {ph}, {ph}) RETURNING feedback_id",
            (feedback.response_id, feedback.rating, feedback.comment),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        conn.close()


def _attach_langsmith_feedback(feedback: FeedbackRequest) -> None:
    if not maybe_enable_langsmith_tracing():
        return
    try:
        from langsmith import Client as LangSmithClient
    except Exception:
        return

    try:
        LangSmithClient().create_feedback(
            run_id=feedback.response_id,
            key="user_rating",
            score=feedback.rating,
            comment=feedback.comment,
        )
    except Exception:
        logger.warning(
            "Failed to attach feedback to LangSmith",
            extra={"response_id": feedback.response_id},
        )


async def _run_chain(
    chain_name: str, question: str, context: dict[str, Any] | None, client_info: Any
):
    """Execute a chain and return normalized payload."""
    chain = _build_chain(chain_name, client_info)
    if hasattr(chain, "run"):
        result = chain.run(question=question, context=context)
    elif hasattr(chain, "invoke"):
        result = chain.invoke({"question": question, "context": context or {}})
    else:
        raise RuntimeError(f"Chain '{chain_name}' does not expose run/invoke")

    if asyncio.iscoroutine(result):
        result = await result

    return _extract_chain_payload(result)


def _resolve_chain_name(requested_chain: str, question: str) -> str:
    """Resolve requested/auto chain to a concrete chain implementation name."""
    if requested_chain != "auto":
        return requested_chain
    classified_chain = _classify_intent(question)
    if classified_chain in DIRECT_CHAIN_PATHS:
        return classified_chain
    return "rag_search"


@app.post("/api/chat", response_model=ChatResponse)
async def chat_api(request: ChatRequest, raw_request: Request) -> ChatResponse:
    """Main chat endpoint with automatic or explicit chain routing."""
    started = time.perf_counter()
    try:
        _enforce_chat_rate_limit(raw_request)
        client_info = _build_chat_client_info()
        if not client_info:
            raise HTTPException(
                status_code=503,
                detail="No LLM provider configured. Set OPENAI_API_KEY or ANTHROPIC_API_KEY.",
            )

        requested_chain = (request.chain or "auto").strip().lower()
        if requested_chain not in VALID_CHAIN_NAMES:
            raise HTTPException(status_code=400, detail="Invalid chain")
        chain_used = _resolve_chain_name(requested_chain, request.question)

        answer, sources, sql, trace_url = await _run_chain(
            chain_used, request.question, request.context, client_info
        )
        latency_ms = int((time.perf_counter() - started) * 1000)
        return ChatResponse(
            answer=answer,
            chain_used=chain_used,
            sources=sources,
            sql=sql,
            trace_url=trace_url,
            latency_ms=latency_ms,
            response_id=_response_id_from_trace_url(trace_url),
        )
    except PROMPT_INJECTION_ERROR as exc:  # type: ignore[misc]
        reasons = getattr(exc, "reasons", [str(exc)])
        if isinstance(reasons, list):
            reason_text = ", ".join(str(reason) for reason in reasons)
        else:
            reason_text = str(reasons)
        raise HTTPException(
            status_code=400,
            detail=f"Input rejected: {reason_text}",
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Chat error: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Research assistant error. Check server logs.",
        ) from exc


@app.post("/api/chat/filing-summary", response_model=ChatResponse)
async def filing_summary(filing_id: int, raw_request: Request) -> ChatResponse:
    """Direct filing summary endpoint."""
    return await chat_api(
        ChatRequest(
            question=f"Summarize filing {filing_id}",
            chain="filing_summary",
            context={"filing_id": filing_id},
        ),
        raw_request,
    )


@app.post("/api/chat/holdings-analysis", response_model=ChatResponse)
async def holdings_analysis(request: HoldingsAnalysisRequest, raw_request: Request) -> ChatResponse:
    """Direct holdings analysis endpoint."""
    return await chat_api(
        ChatRequest(
            question=request.question,
            chain="holdings_analysis",
            context=request.context,
        ),
        raw_request,
    )


@app.post("/api/chat/query", response_model=ChatResponse)
async def nl_query(question: str, raw_request: Request) -> ChatResponse:
    """Direct NL-to-SQL query endpoint."""
    return await chat_api(ChatRequest(question=question, chain="nl_query"), raw_request)


@app.post("/api/chat/search", response_model=ChatResponse)
async def rag_search(question: str, raw_request: Request) -> ChatResponse:
    """Direct RAG search endpoint."""
    return await chat_api(ChatRequest(question=question, chain="rag_search"), raw_request)


@app.post("/api/chat/feedback")
async def submit_feedback(feedback: FeedbackRequest) -> dict[str, Any]:
    """Persist user feedback and forward it to LangSmith when available."""
    feedback_id = _store_feedback(feedback)
    _attach_langsmith_feedback(feedback)
    return {"ok": True, "feedback_id": feedback_id}


def _health_payload() -> dict[str, int | bool]:
    """Build the base app health payload."""
    # Use monotonic time to avoid issues if the system clock changes.
    uptime_s = int(HEALTH_CLOCK.monotonic() - APP_START_TIME)
    return {"healthy": True, "uptime_s": uptime_s}


def _format_dependency_error(exc: Exception) -> str:
    """Return a short reason string for failed dependency checks."""
    message = str(exc).strip()
    return (message or exc.__class__.__name__)[:200]


def _health_summary_timeout_seconds() -> float:
    """Return the timeout budget for /health dependency checks."""
    timeout = float(os.getenv("HEALTH_SUMMARY_TIMEOUT_S", "0.2"))
    return max(min(timeout, 0.2), 0.05)


async def _run_dependency_check(
    func,
    timeout_seconds: float,
    *args,
    circuit_breaker: CircuitBreaker | None = None,
    enabled: bool = True,
    include_enabled: bool = False,
) -> tuple[dict[str, int | bool], str | None]:
    """Run a dependency check and return its payload plus failure reason."""
    if not enabled:
        payload = {"healthy": True, "latency_ms": 0}
        if include_enabled:
            payload["enabled"] = False
        return payload, None
    if circuit_breaker and circuit_breaker.is_open():
        payload = {"healthy": False, "latency_ms": 0, "circuit_open": True}
        if include_enabled:
            payload["enabled"] = True
        return payload, "circuit_open"
    start = HEALTH_CLOCK.perf_counter()
    try:
        # Keep retries inside the per-check timeout budget for responsiveness.
        await _run_health_check_with_retries(func, timeout_seconds, *args)
        latency_ms = int((HEALTH_CLOCK.perf_counter() - start) * 1000)
        if circuit_breaker:
            circuit_breaker.record_success()
        payload = {"healthy": True, "latency_ms": latency_ms}
        if include_enabled:
            payload["enabled"] = True
        return payload, None
    except TimeoutError:
        latency_ms = int(min(timeout_seconds, HEALTH_CLOCK.perf_counter() - start) * 1000)
        if circuit_breaker:
            circuit_breaker.record_failure()
        payload = {"healthy": False, "latency_ms": latency_ms}
        if include_enabled:
            payload["enabled"] = True
        return payload, "timeout"
    except Exception as exc:
        latency_ms = int((HEALTH_CLOCK.perf_counter() - start) * 1000)
        if circuit_breaker:
            circuit_breaker.record_failure()
        payload = {"healthy": False, "latency_ms": latency_ms}
        if include_enabled:
            payload["enabled"] = True
        return payload, _format_dependency_error(exc)


async def _run_health_summary_checks(
    timeout_budget: float,
    db_timeout_seconds: float,
    minio_timeout_seconds: float,
    redis_timeout_seconds: float,
    redis_url: str | None,
) -> dict[str, tuple[dict[str, int | bool], str | None]]:
    """Run summary dependency checks with an overall time budget."""
    tasks = {
        "database": asyncio.create_task(
            _run_dependency_check(_ping_db, db_timeout_seconds, db_timeout_seconds)
        ),
        "minio": asyncio.create_task(
            _run_dependency_check(
                _ping_minio,
                minio_timeout_seconds,
                minio_timeout_seconds,
                circuit_breaker=_MINIO_CIRCUIT,
            )
        ),
        "redis": asyncio.create_task(
            _run_dependency_check(
                _ping_redis,
                redis_timeout_seconds,
                redis_url,
                redis_timeout_seconds,
                circuit_breaker=_REDIS_CIRCUIT,
                enabled=bool(redis_url),
                include_enabled=True,
            )
        ),
    }
    task_names = {task: name for name, task in tasks.items()}
    start = HEALTH_CLOCK.perf_counter()
    # Allow a small buffer beyond the budget to avoid false timeouts in the executor.
    overall_timeout = min(timeout_budget + 0.05, 0.2)
    done, pending = await asyncio.wait(tasks.values(), timeout=overall_timeout)
    elapsed_ms = int(min(overall_timeout, HEALTH_CLOCK.perf_counter() - start) * 1000)
    for task in pending:
        task.cancel()
        name = task_names.get(task)
        if name == "minio":
            _MINIO_CIRCUIT.record_failure()
        elif name == "redis" and redis_url:
            _REDIS_CIRCUIT.record_failure()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    results: dict[str, tuple[dict[str, int | bool], str | None]] = {}
    for name, task in tasks.items():
        if task in done:
            results[name] = task.result()
        else:
            payload = {"healthy": False, "latency_ms": elapsed_ms}
            if name == "redis":
                payload["enabled"] = bool(redis_url)
            results[name] = (payload, "timeout")
    return results


@app.get("/health")
async def health_app():
    """Return application health and dependency status."""
    start = HEALTH_CLOCK.perf_counter()
    try:
        app_payload = _health_payload()
        timeout_budget = _health_summary_timeout_seconds()
        db_timeout_seconds = min(_db_timeout_seconds(), timeout_budget)
        minio_timeout_seconds = min(_minio_timeout_seconds(), timeout_budget)
        redis_timeout_seconds = min(_redis_timeout_seconds(), timeout_budget)
        redis_url = os.getenv("REDIS_URL")
        results = await _run_health_summary_checks(
            timeout_budget,
            db_timeout_seconds,
            minio_timeout_seconds,
            redis_timeout_seconds,
            redis_url,
        )
        db_payload, db_reason = results["database"]
        minio_payload, minio_reason = results["minio"]
        redis_payload, redis_reason = results["redis"]
        components = {
            "app": {"healthy": app_payload["healthy"], "uptime_s": app_payload["uptime_s"]},
            "database": db_payload,
            "minio": minio_payload,
            "redis": redis_payload,
        }
        failed_checks = {}
        if not db_payload["healthy"]:
            failed_checks["database"] = db_reason or "unhealthy"
        if not minio_payload["healthy"]:
            failed_checks["minio"] = minio_reason or "unhealthy"
        if redis_payload.get("enabled", True) and not redis_payload["healthy"]:
            failed_checks["redis"] = redis_reason or "unhealthy"
        redis_healthy = redis_payload["healthy"] if redis_payload.get("enabled", True) else True
        healthy = (
            app_payload["healthy"]
            and db_payload["healthy"]
            and minio_payload["healthy"]
            and redis_healthy
        )
        payload = {
            "healthy": healthy,
            "uptime_s": app_payload["uptime_s"],
            "components": components,
            "failed_checks": failed_checks,
        }
        status_code = 200 if healthy else 503
        return JSONResponse(status_code=status_code, content=payload)
    finally:
        # Track summary health latency for dashboards and alerts.
        HEALTH_CHECK_DURATION.labels(endpoint="health").observe(HEALTH_CLOCK.perf_counter() - start)


@app.get("/health/live")
def health_live():
    """Alias liveness endpoint for standard probes."""
    return _health_payload()


@app.get("/healthz")
def healthz():
    """Alias liveness endpoint for common probe conventions."""
    # Keep probe aliases routed through the same liveness payload.
    return _health_payload()


@app.get("/livez")
def health_livez():
    """Alias live probe endpoint for common probe conventions."""
    return _health_payload()


@app.on_event("startup")
async def _configure_default_executor() -> None:
    """Install a known-good default executor for sync endpoints."""
    asyncio.get_running_loop().set_default_executor(_APP_EXECUTOR)
    await start_memory_profiler(app)


def _db_timeout_seconds() -> float:
    """Return the DB health timeout in seconds."""
    # Cap health checks to 5s so monitoring calls never stall longer.
    return min(float(os.getenv("DB_HEALTH_TIMEOUT_S", "5")), 5.0)


def _ping_db(timeout_seconds: float) -> None:
    """Run a lightweight DB query to verify connectivity."""
    # Pass a connect timeout so the ping doesn't hang on slow networks.
    conn = connect_db(connect_timeout=timeout_seconds)
    try:
        conn.execute("SELECT 1")
    finally:
        conn.close()


def _minio_timeout_seconds() -> float:
    """Return the MinIO health timeout in seconds."""
    return min(float(os.getenv("MINIO_HEALTH_TIMEOUT_S", "5")), 5.0)


def _minio_client(timeout_seconds: float):
    """Create a MinIO client with aggressive timeouts for health checks."""
    config = BotoConfig(
        connect_timeout=timeout_seconds,
        read_timeout=timeout_seconds,
        retries={"max_attempts": 0},
    )
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=config,
    )


def _redis_timeout_seconds() -> float:
    """Return the Redis health timeout in seconds."""
    return min(float(os.getenv("REDIS_HEALTH_TIMEOUT_S", "2")), 5.0)


def _ping_redis(redis_url: str, timeout_seconds: float) -> None:
    """Run a lightweight Redis ping to verify cache connectivity."""
    # Import lazily so Redis remains optional outside of cache deployments.
    import redis

    client = redis.Redis.from_url(redis_url, socket_timeout=timeout_seconds)
    client.ping()


def _ping_minio(timeout_seconds: float) -> None:
    """Run a lightweight MinIO API call to verify connectivity."""
    client = _minio_client(timeout_seconds)
    client.list_buckets()


async def _run_health_check_with_retries(
    func,
    timeout_seconds: float,
    *args,
    sleep_fn: Callable[[float], None] | None = None,
    perf_counter_fn: Callable[[], float] | None = None,
) -> None:
    """Run a health check with exponential backoff for flaky dependencies."""

    def _run_with_retries_sync() -> None:
        # Resolve injected timing functions once so retries stay consistent.
        resolved_sleep = sleep_fn or HEALTH_CLOCK.sleep
        resolved_perf_counter = perf_counter_fn or HEALTH_CLOCK.perf_counter
        deadline = resolved_perf_counter() + timeout_seconds
        for backoff in _HEALTH_RETRY_BACKOFFS:
            try:
                func(*args)
                return
            except Exception:
                if resolved_perf_counter() + backoff >= deadline:
                    raise
                resolved_sleep(backoff)
        func(*args)

    await asyncio.wait_for(
        asyncio.get_running_loop().run_in_executor(get_health_executor(), _run_with_retries_sync),
        timeout=timeout_seconds,
    )


@app.get(
    "/health/db",
    response_model=HealthDbResponse,
    summary="Check database connectivity",
    description=(
        "Run a lightweight database ping and return the health status with observed latency."
    ),
    responses={
        503: {
            "model": HealthDbResponse,
            "description": "Database unavailable",
            "content": {
                "application/json": {
                    "examples": {
                        "timeout": {
                            "summary": "Timed out ping",
                            "value": {"healthy": False, "latency_ms": 5000},
                        }
                    }
                }
            },
        }
    },
)
async def health_db():
    """Return database connectivity status and latency."""
    start = HEALTH_CLOCK.perf_counter()
    timeout_seconds = _db_timeout_seconds()
    try:
        # Use a dedicated executor to avoid relying on the loop default executor.
        await _run_health_check_with_retries(_ping_db, timeout_seconds, timeout_seconds)
        latency_ms = int((HEALTH_CLOCK.perf_counter() - start) * 1000)
        payload = {"healthy": True, "latency_ms": latency_ms}
        return JSONResponse(status_code=200, content=payload)
    except TimeoutError:
        # Fail fast to keep the endpoint under the timeout budget.
        latency_ms = int(min(timeout_seconds, HEALTH_CLOCK.perf_counter() - start) * 1000)
        payload = {"healthy": False, "latency_ms": latency_ms}
        return JSONResponse(status_code=503, content=payload)
    except Exception:
        latency_ms = int((HEALTH_CLOCK.perf_counter() - start) * 1000)
        payload = {"healthy": False, "latency_ms": latency_ms}
        return JSONResponse(status_code=503, content=payload)
    finally:
        # Record total DB check duration even when the ping fails.
        HEALTH_CHECK_DURATION.labels(endpoint="health_db").observe(
            HEALTH_CLOCK.perf_counter() - start
        )


@app.get("/health/ready")
async def health_ready():
    """Return readiness status combining app and database checks."""
    start = HEALTH_CLOCK.perf_counter()
    # Reuse the existing health endpoints to keep readiness logic consistent.
    app_payload = _health_payload()
    try:
        db_response = await health_db()
        db_payload = json.loads(db_response.body)
        healthy = app_payload["healthy"] and db_payload["healthy"]
        payload = {
            "healthy": healthy,
            "uptime_s": app_payload["uptime_s"],
            "db_latency_ms": db_payload["latency_ms"],
        }
        status_code = 200 if healthy else 503
        return JSONResponse(status_code=status_code, content=payload)
    finally:
        # Capture readiness latency for alerting dashboards.
        HEALTH_CHECK_DURATION.labels(endpoint="health_ready").observe(
            HEALTH_CLOCK.perf_counter() - start
        )


@app.get("/readyz")
async def health_readyz():
    """Alias readiness endpoint for common probe conventions."""
    # Reuse the main readiness check so probes stay in sync.
    return await health_ready()


@app.get(
    "/health/detailed",
    response_model=HealthDetailedResponse,
    summary="Return detailed health status",
    description="Return per-component health status, including database connectivity.",
    responses={
        503: {
            "model": HealthDetailedResponse,
            "description": "One or more components are unavailable",
        }
    },
)
async def health_detailed():
    """Return detailed health status for app and database components."""
    start = HEALTH_CLOCK.perf_counter()
    # Reuse existing health payloads to keep liveness logic consistent.
    app_payload = _health_payload()
    try:
        db_response = await health_db()
        db_payload = json.loads(db_response.body)
        if _MINIO_CIRCUIT.is_open():
            minio_payload = {"healthy": False, "latency_ms": 0, "circuit_open": True}
        else:
            minio_start = HEALTH_CLOCK.perf_counter()
            minio_timeout_seconds = _minio_timeout_seconds()
            try:
                await _run_health_check_with_retries(
                    _ping_minio, minio_timeout_seconds, minio_timeout_seconds
                )
                minio_latency_ms = int((HEALTH_CLOCK.perf_counter() - minio_start) * 1000)
                _MINIO_CIRCUIT.record_success()
                minio_payload = {"healthy": True, "latency_ms": minio_latency_ms}
            except TimeoutError:
                minio_latency_ms = int(minio_timeout_seconds * 1000)
                _MINIO_CIRCUIT.record_failure()
                minio_payload = {"healthy": False, "latency_ms": minio_latency_ms}
            except Exception:
                minio_latency_ms = int((HEALTH_CLOCK.perf_counter() - minio_start) * 1000)
                _MINIO_CIRCUIT.record_failure()
                minio_payload = {"healthy": False, "latency_ms": minio_latency_ms}
        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            if _REDIS_CIRCUIT.is_open():
                redis_payload = {
                    "healthy": False,
                    "latency_ms": 0,
                    "enabled": True,
                    "circuit_open": True,
                }
            else:
                redis_start = HEALTH_CLOCK.perf_counter()
                redis_timeout_seconds = _redis_timeout_seconds()
                try:
                    await _run_health_check_with_retries(
                        _ping_redis, redis_timeout_seconds, redis_url, redis_timeout_seconds
                    )
                    redis_latency_ms = int((HEALTH_CLOCK.perf_counter() - redis_start) * 1000)
                    _REDIS_CIRCUIT.record_success()
                    redis_payload = {
                        "healthy": True,
                        "latency_ms": redis_latency_ms,
                        "enabled": True,
                    }
                except TimeoutError:
                    redis_latency_ms = int(redis_timeout_seconds * 1000)
                    _REDIS_CIRCUIT.record_failure()
                    redis_payload = {
                        "healthy": False,
                        "latency_ms": redis_latency_ms,
                        "enabled": True,
                    }
                except Exception:
                    redis_latency_ms = int((HEALTH_CLOCK.perf_counter() - redis_start) * 1000)
                    _REDIS_CIRCUIT.record_failure()
                    redis_payload = {
                        "healthy": False,
                        "latency_ms": redis_latency_ms,
                        "enabled": True,
                    }
        else:
            # Skip cache probes when Redis isn't configured in this environment.
            redis_payload = {"healthy": True, "latency_ms": 0, "enabled": False}
        components = {
            "app": {"healthy": app_payload["healthy"], "uptime_s": app_payload["uptime_s"]},
            "database": {
                "healthy": db_payload["healthy"],
                "latency_ms": db_payload["latency_ms"],
            },
            "minio": minio_payload,
            "redis": redis_payload,
        }
        redis_healthy = redis_payload["healthy"] if redis_payload["enabled"] else True
        healthy = (
            app_payload["healthy"]
            and db_payload["healthy"]
            and minio_payload["healthy"]
            and redis_healthy
        )
        payload = {
            "healthy": healthy,
            "uptime_s": app_payload["uptime_s"],
            "components": components,
        }
        status_code = 200 if healthy else 503
        return JSONResponse(status_code=status_code, content=payload)
    finally:
        # Track full detailed health check latency for dashboards.
        HEALTH_CHECK_DURATION.labels(endpoint="health_detailed").observe(
            HEALTH_CLOCK.perf_counter() - start
        )


@app.get("/metrics")
def metrics() -> Response:
    """Return Prometheus metrics for scraping."""
    # Prometheus expects the text exposition format on this endpoint.
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.on_event("shutdown")
def _shutdown_executors() -> None:
    """Release the health check executors on app shutdown."""
    _APP_EXECUTOR.shutdown(wait=False, cancel_futures=True)
    _HEALTH_EXECUTOR.shutdown(wait=False, cancel_futures=True)


@app.on_event("shutdown")
async def _shutdown_memory_profiler() -> None:
    """Stop the optional memory profiler task."""
    await stop_memory_profiler(app)


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (health)
# - [ ] summary is concise and imperative
