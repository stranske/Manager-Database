"""Focused contract coverage for chat request helpers."""

import asyncio
import json
import sqlite3
import sys
from datetime import date
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api import chat


def test_validation_error_formatter_keeps_field_context():
    error = SimpleNamespace(
        errors=lambda: [
            {"loc": ("body", "question"), "msg": "Field required"},
            {"loc": ("body",), "msg": "Invalid body"},
            {"loc": (), "msg": "Unknown failure"},
        ]
    )

    assert chat._format_validation_errors(error) == [
        {"field": "question", "message": "Field required"},
        {"field": "body", "message": "Invalid body"},
        {"field": "unknown", "message": "Unknown failure"},
    ]


def test_legacy_chat_formats_search_hits_and_empty_results(monkeypatch):
    monkeypatch.setattr(
        "embeddings.search_documents",
        lambda _query: [
            {"content": "Alpha", "kind": "filing", "filename": "alpha.txt", "manager_name": "A"},
            {"content": "Beta"},
        ],
    )

    payload = chat.chat("latest")

    assert payload["answer"] == (
        "Context: [filing | alpha.txt | manager: A] Alpha "
        "[note | unknown | manager: unassigned] Beta"
    )
    monkeypatch.setattr("embeddings.search_documents", lambda _query: [])
    assert chat.chat("missing")["answer"] == "No documents found."


def test_search_api_closes_connection_after_delegating(monkeypatch):
    seen = {}

    class Connection:
        def close(self):
            seen["closed"] = True

    connection = Connection()
    monkeypatch.setattr(chat, "connect_db", lambda: connection)
    monkeypatch.setattr(
        chat,
        "universal_search",
        lambda query, conn, **kwargs: seen.update(query=query, conn=conn, **kwargs) or ["result"],
    )

    assert asyncio.run(chat.search_api("Elliott", limit=3, entity_type=None)) == ["result"]
    assert seen == {
        "query": "Elliott",
        "conn": connection,
        "limit": 3,
        "entity_type": None,
        "closed": True,
    }


def test_rate_limiter_prunes_expired_entries_and_none_requests_are_anonymous():
    limiter = chat.InMemoryChatRateLimiter(max_requests=1, window_seconds=10)

    assert limiter.check_and_record("session", now=10)
    assert not limiter.check_and_record("session", now=11)
    assert limiter.check_and_record("session", now=20)
    assert chat._chat_session_id(None) == "unknown"
    assert chat._chat_client_id(None) == "client:unknown"


def test_fallback_chains_preserve_explicit_context_fields():
    holdings = chat._FallbackHoldingsAnalysisChain().run(
        "Analyze",
        manager_ids=[7],
        cusips=["037833100"],
        date_range=(date(2024, 1, 1), date(2024, 3, 31)),
        as_of=date(2024, 4, 1),
    )

    assert holdings["sources"] == [
        {
            "manager_ids": [7],
            "cusips": ["037833100"],
            "date_range": ["2024-01-01", "2024-03-31"],
            "as_of": "2024-04-01",
        }
    ]
    assert chat._FallbackNLQueryChain().run(question="q", context={})["sources"] == {}


def test_feedback_storage_creates_sqlite_table_and_rejects_missing_non_sqlite_table(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "feedback.db"
    monkeypatch.setattr(chat, "connect_db", lambda: sqlite3.connect(db_path))

    assert (
        chat._store_feedback(chat.FeedbackRequest(response_id="run-1", rating=5, comment="great"))
        == 1
    )
    with sqlite3.connect(db_path) as connection:
        assert connection.execute(
            "SELECT response_id, rating, comment FROM chat_feedback"
        ).fetchall() == [("run-1", 5, "great")]

    monkeypatch.setattr(chat, "is_sqlite", lambda _conn: False)
    monkeypatch.setattr(chat, "table_exists", lambda _conn, _name: False)
    with pytest.raises(RuntimeError, match="run migrations"):
        chat._ensure_chat_feedback_table(object())


def test_feedback_route_maps_missing_storage_to_service_unavailable(monkeypatch):
    def missing_storage(_feedback):
        raise RuntimeError("run migrations")

    monkeypatch.setattr(chat, "_enforce_chat_rate_limit", lambda _request: None)
    monkeypatch.setattr(chat, "_store_feedback", missing_storage)
    with pytest.raises(HTTPException) as error:
        asyncio.run(chat.submit_feedback(chat.FeedbackRequest(response_id="run-2", rating=3), None))
    assert error.value.status_code == 503
    assert error.value.detail == "run migrations"


def test_chat_chain_health_accepts_explicit_fallback(monkeypatch):
    monkeypatch.setenv("CHAT_CHAIN_FALLBACK_MODE", "offline")
    monkeypatch.delenv("LLM_ZONE", raising=False)

    assert chat._chat_chain_health_payload() == (
        {"healthy": True, "available": True, "fallback_enabled": True},
        None,
    )


def test_optional_dependency_clients_use_configured_timeouts(monkeypatch):
    captured = {}

    def client(*service, **kwargs):
        captured["service"] = service
        captured.update(kwargs)
        return SimpleNamespace(list_buckets=lambda: captured.setdefault("minio_pinged", True))

    class RedisClient:
        def ping(self):
            captured["redis_pinged"] = True

    class Redis:
        @staticmethod
        def from_url(url, socket_timeout):
            captured["redis_url"] = url
            captured["redis_timeout"] = socket_timeout
            return RedisClient()

    monkeypatch.setattr(chat.boto3, "client", client)
    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(Redis=Redis))
    chat._ping_minio(1.25)
    chat._ping_redis("redis://cache", 2.5)

    assert captured["service"] == ("s3",)
    assert captured["config"].connect_timeout == 1.25
    assert captured["config"].read_timeout == 1.25
    assert captured["minio_pinged"] is True
    assert captured["redis_url"] == "redis://cache"
    assert captured["redis_timeout"] == 2.5
    assert captured["redis_pinged"] is True


def test_health_detailed_reports_timeout_for_optional_dependencies(monkeypatch):
    # Pretty JSON is semantically identical to the default compact serializer.
    monkeypatch.setattr(
        chat.JSONResponse, "render", lambda _self, content: json.dumps(content, indent=2).encode()
    )

    async def healthy_db():
        return chat.JSONResponse(status_code=200, content={"healthy": True, "latency_ms": 1})

    async def timed_check(func, *_args):
        if func in {chat._ping_minio, chat._ping_redis}:
            raise TimeoutError

    monkeypatch.setattr(chat, "health_db", healthy_db)
    monkeypatch.setattr(chat, "_run_health_check_with_retries", timed_check)
    monkeypatch.setattr(chat, "_MINIO_CIRCUIT", chat.CircuitBreaker())
    monkeypatch.setattr(chat, "_REDIS_CIRCUIT", chat.CircuitBreaker())
    monkeypatch.setattr(chat, "_chat_chain_health_payload", lambda: ({"healthy": True}, None))
    monkeypatch.setenv("REDIS_URL", "redis://cache")

    payload = asyncio.run(chat.health_detailed())

    assert payload.status_code == 503
    body = json.loads(payload.body)
    assert body["components"]["minio"]["healthy"] is False
    assert body["components"]["redis"]["healthy"] is False


def test_error_categories_and_metrics_response(monkeypatch):
    monkeypatch.setattr(chat, "generate_latest", lambda: b"metric 1\n")

    assert [chat._error_category_from_http(code) for code in (429, 503, 400, 500, 200)] == [
        "rate_limited",
        "provider_unavailable",
        "client_error",
        "server_error",
        None,
    ]
    assert chat.metrics().body == b"metric 1\n"


def test_circuit_breaker_opens_resets_and_success_clears_state():
    now = [10.0]
    breaker = chat.CircuitBreaker(
        failure_threshold=2, reset_timeout_s=5, monotonic_fn=lambda: now[0]
    )

    assert not breaker.is_open()
    breaker.record_failure()
    assert not breaker.is_open()
    breaker.record_failure()
    assert breaker.is_open()

    breaker.record_success()
    assert not breaker.is_open()
    breaker.record_failure()
    breaker.record_failure()
    now[0] = 15.0
    assert not breaker.is_open()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, 7.5), ("0", 7.5), ("-1", 7.5), ("nan", 7.5), ("3.25", 3.25)],
)
def test_health_timeout_uses_only_finite_positive_environment_values(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv("CHAT_TEST_TIMEOUT", raising=False)
    else:
        monkeypatch.setenv("CHAT_TEST_TIMEOUT", raw)

    assert chat._health_timeout_seconds("CHAT_TEST_TIMEOUT", 7.5) == expected


def test_signed_session_cookie_requires_matching_secret(monkeypatch):
    monkeypatch.setenv("CHAT_SESSION_COOKIE_SECRET", "secret")
    signed = chat._sign_chat_session_id("session-17", "secret")

    assert chat._verified_chat_session_cookie(signed) == "session-17"
    assert chat._verified_chat_session_cookie("session-17.invalid") is None
    assert chat._verified_chat_session_cookie("missing-separator") is None
    monkeypatch.delenv("CHAT_SESSION_COOKIE_SECRET")
    assert chat._verified_chat_session_cookie(signed) is None


def test_normalizers_drop_invalid_values_and_parse_dates():
    assert chat._normalize_manager_ids(["1", None, "bad", 2]) == [1, 2]
    assert chat._normalize_cusips([" abc ", "", None, 17]) == ["ABC", "17"]
    assert chat._parse_date_range({"start": "2024-01-01", "end": "2024-03-31"}) == (
        date(2024, 1, 1),
        date(2024, 3, 31),
    )
    assert chat._parse_date_range(["2024-01-01", "not-a-date"]) is None
    assert chat._parse_date_range("2024-01-01") is None


def test_normalize_chain_context_resolves_manager_name_and_valid_filters(monkeypatch):
    monkeypatch.setattr(chat, "_manager_ids_for_name", lambda _conn, _name: [4])
    context = chat._normalize_chain_context(
        {
            "manager_name": " Northstar ",
            "cusips": [" 123456789 "],
            "date_range": ["2024-01-01", "2024-03-31"],
        },
        object(),
    )

    assert context == {
        "manager_name": " Northstar ",
        "manager_ids": [4],
        "cusips": ["123456789"],
        "date_range": {"start": "2024-01-01", "end": "2024-03-31"},
    }
    assert chat._validated_holdings_filters(context) == (
        [4],
        ["123456789"],
        (date(2024, 1, 1), date(2024, 3, 31)),
    )


@pytest.mark.parametrize(
    ("context", "detail"),
    [
        ({"manager_ids": ["bad"]}, "Manager filter"),
        ({"cusips": [""]}, "CUSIP filter"),
        ({"date_range": ["2024-04-01", "2024-01-01"]}, "Date range"),
    ],
)
def test_validated_holdings_filters_reject_invalid_explicit_filters(context, detail):
    with pytest.raises(HTTPException, match=detail):
        chat._validated_holdings_filters(context)


def test_classify_intent_uses_deterministic_fallback_when_import_is_unavailable(monkeypatch):
    def unavailable(_name):
        raise ModuleNotFoundError("chains.intent")

    monkeypatch.setattr(chat.importlib, "import_module", unavailable)

    assert chat._classify_intent("write a SQL query") == "nl_query"
    assert chat._classify_intent("summarize the 13F filing") == "filing_summary"
    assert chat._classify_intent("analyze the holding") == "holdings_analysis"
    assert chat._classify_intent("what changed?") == "rag_search"


def test_display_formatters_include_only_meaningful_optional_fields():
    filing = chat._format_filing_summary_payload(
        {
            "manager_name": "Northstar",
            "filing_date": "2024-03-31",
            "total_positions": 2,
            "total_aum_estimate": 12,
            "key_positions": [{"issuer": "Alpha"}, "unusable"],
            "notable_changes": ["Added Alpha"],
            "risk_flags": ["Concentrated"],
        }
    )
    holdings = chat._format_holdings_analysis_payload(
        {
            "thesis": "Concentrated portfolio.",
            "top_positions": [{"cusip": "123456789"}],
            "period_changes": [{"summary": "Increased"}],
            "concentration_metrics": {"top_10": 0.7},
        }
    )

    assert "Top positions: Alpha" in filing["answer"]
    assert "Notable changes: Added Alpha" in filing["answer"]
    assert "Risk flags: Concentrated" in filing["answer"]
    assert "Top positions: 123456789" in holdings["answer"]
    assert "Period changes: Increased" in holdings["answer"]
    assert "Concentration metrics" in holdings["answer"]


@pytest.mark.parametrize(
    ("trace_url", "expected"),
    [
        (None, None),
        ("", None),
        ("https://trace.invalid/r/run-17/", "run-17"),
        ("trace-id", None),
    ],
)
def test_response_id_is_extracted_only_from_path_like_trace_urls(trace_url, expected):
    assert chat._response_id_from_trace_url(trace_url) == expected


def test_dependency_error_and_timeout_bounds_are_stable(monkeypatch):
    monkeypatch.setenv("HEALTH_SUMMARY_TIMEOUT_S", "9")
    assert chat._health_summary_timeout_seconds() == 0.2
    monkeypatch.setenv("HEALTH_SUMMARY_TIMEOUT_S", "0.01")
    assert chat._health_summary_timeout_seconds() == 0.05
    assert (
        chat._format_dependency_error(RuntimeError("  failed dependency  ")) == "failed dependency"
    )
    assert chat._format_dependency_error(ValueError()) == "ValueError"


def test_validation_exception_handler_returns_field_errors():
    error = SimpleNamespace(errors=lambda: [{"loc": ("body", "question"), "msg": "Field required"}])

    response = asyncio.run(chat._validation_exception_handler(None, error))

    assert response.status_code == 400
    assert json.loads(response.body) == {
        "errors": [{"field": "question", "message": "Field required"}],
        "error": [{"field": "question", "message": "Field required"}],
    }


def test_signed_session_rejects_empty_parts_and_session_id_uses_verified_cookie(monkeypatch):
    monkeypatch.setenv("CHAT_SESSION_COOKIE_SECRET", "secret")
    monkeypatch.setattr(chat, "_chat_session_cookie_name", lambda: "chat_session")

    assert chat._verified_chat_session_cookie(".deadbeef") is None
    assert chat._verified_chat_session_cookie("session.") is None

    signed = chat._sign_chat_session_id("session-42", "secret")
    request = SimpleNamespace(
        client=SimpleNamespace(host="10.0.0.8"),
        cookies={"chat_session": signed},
    )
    assert chat._chat_session_id(request) == "client:10.0.0.8:session:session-42"


def test_prompt_injection_error_fallback_and_client_info_edge_cases(monkeypatch):
    assert chat._PromptInjectionError("blocked").reasons == ["blocked"]
    assert chat._PromptInjectionError(["a", "b"]).reasons == ["a", "b"]

    def missing_injection(_name):
        raise ModuleNotFoundError("llm.injection")

    monkeypatch.setattr(chat.importlib, "import_module", missing_injection)
    assert chat._load_prompt_injection_error_class() is chat._PromptInjectionError

    def unexpected_module(_name):
        raise ModuleNotFoundError("other.module", name="other.module")

    monkeypatch.setattr(chat.importlib, "import_module", unexpected_module)
    with pytest.raises(ModuleNotFoundError):
        chat._build_chat_client_info()

    monkeypatch.setattr(
        chat.importlib,
        "import_module",
        lambda _name: SimpleNamespace(build_chat_client=None),
    )
    assert chat._build_chat_client_info() is None


def test_manager_ids_for_name_skips_unparseable_rows(monkeypatch):
    class Conn:
        def execute(self, *_args, **_kwargs):
            return SimpleNamespace(fetchall=lambda: [(None,), ("bad",), (7,)])

    monkeypatch.setattr(chat, "_manager_id_column", lambda _conn: "manager_id")
    monkeypatch.setattr(chat, "get_placeholder", lambda _conn: "?")

    assert chat._manager_ids_for_name(Conn(), "Northstar") == [7]


def test_build_chain_uses_explicit_fallback_after_module_missing(monkeypatch):
    monkeypatch.setattr(chat, "_chat_chain_fallback_enabled", lambda: True)

    def missing_module(_name):
        raise ModuleNotFoundError("chains.missing")

    monkeypatch.setattr(chat.importlib, "import_module", missing_module)
    chain = chat._build_chain("rag_search", None)

    assert isinstance(chain, chat._FallbackRAGSearchChain)
    assert chain.run(question="q", context={"k": 1})["sources"] == {"k": 1}

    monkeypatch.setattr(chat, "_chat_chain_fallback_enabled", lambda: False)
    with pytest.raises(chat.ChainUnavailableError, match="rag_search"):
        chat._build_chain("rag_search", None)


def test_feedback_storage_uses_returning_for_non_sqlite(monkeypatch):
    seen = {}

    class Conn:
        def execute(self, query, params):
            seen["query"] = query
            seen["params"] = params
            return SimpleNamespace(fetchone=lambda: (41,))

        def close(self):
            seen["closed"] = True

    monkeypatch.setattr(chat, "connect_db", lambda: Conn())
    monkeypatch.setattr(chat, "_ensure_chat_feedback_table", lambda _conn: None)
    monkeypatch.setattr(chat, "get_placeholder", lambda _conn: "%s")
    monkeypatch.setattr(chat, "is_sqlite", lambda _conn: False)

    assert (
        chat._store_feedback(chat.FeedbackRequest(response_id="run-9", rating=4, comment=None))
        == 41
    )
    assert "RETURNING feedback_id" in seen["query"]
    assert seen["params"] == ("run-9", 4, None)
    assert seen["closed"] is True


def test_attach_langsmith_feedback_handles_import_and_api_failures(monkeypatch):
    monkeypatch.setattr(chat, "maybe_enable_langsmith_tracing", lambda: True)
    monkeypatch.setitem(sys.modules, "langsmith", None)
    # Force ImportError path by making the import fail.
    real_import = __import__

    def fail_langsmith(name, *args, **kwargs):
        if name == "langsmith" or name.startswith("langsmith."):
            raise ImportError("no langsmith")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fail_langsmith)
    assert (
        chat._attach_langsmith_feedback(
            chat.FeedbackRequest(response_id="run-a", rating=1, comment=None)
        )
        is False
    )

    calls = []

    class BoomClient:
        def create_feedback(self, **_kwargs):
            calls.append(_kwargs)
            raise RuntimeError("langsmith down")

    monkeypatch.setattr(
        "builtins.__import__",
        lambda name, *args, **kwargs: (
            SimpleNamespace(Client=BoomClient)
            if name == "langsmith"
            else real_import(name, *args, **kwargs)
        ),
    )
    # Re-import path uses `from langsmith import Client`.
    monkeypatch.setitem(sys.modules, "langsmith", SimpleNamespace(Client=BoomClient))
    assert (
        chat._attach_langsmith_feedback(
            chat.FeedbackRequest(response_id="run-b", rating=2, comment="nope")
        )
        is False
    )
    assert len(calls) == 1


def test_chat_api_rejects_invalid_chain_and_non_list_injection_reasons(monkeypatch):
    monkeypatch.setattr(chat, "_enforce_chat_rate_limit", lambda _request: None)
    monkeypatch.setattr(chat, "_chat_zone_disabled", lambda: False)
    monkeypatch.setattr(
        chat,
        "_build_chat_client_info",
        lambda: SimpleNamespace(provider="openai", model="gpt"),
    )

    request = chat.ChatRequest(question="hello", chain="not-a-chain")
    raw = SimpleNamespace(url=SimpleNamespace(path="/api/chat"), cookies={}, client=None)
    with pytest.raises(HTTPException) as invalid_chain:
        asyncio.run(chat.chat_api(request, raw))
    assert invalid_chain.value.status_code == 400
    assert invalid_chain.value.detail == "Invalid chain"

    class InjectionError(Exception):
        def __init__(self):
            self.reasons = "single-reason"
            super().__init__(self.reasons)

    monkeypatch.setattr(chat, "PROMPT_INJECTION_ERROR", InjectionError)
    monkeypatch.setattr(
        chat,
        "_resolve_chain_name",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(InjectionError()),
    )
    with pytest.raises(HTTPException) as injection:
        asyncio.run(chat.chat_api(chat.ChatRequest(question="hello", chain="auto"), raw))
    assert injection.value.status_code == 400
    assert injection.value.detail == "Input rejected: single-reason"


def test_dependency_check_include_enabled_for_circuit_timeout_and_error(monkeypatch):
    open_breaker = chat.CircuitBreaker(failure_threshold=1, reset_timeout_s=60)
    open_breaker.record_failure()

    payload, reason = asyncio.run(
        chat._run_dependency_check(
            lambda: None,
            0.05,
            circuit_breaker=open_breaker,
            enabled=True,
            include_enabled=True,
        )
    )
    assert payload == {
        "healthy": False,
        "latency_ms": 0,
        "circuit_open": True,
        "enabled": True,
    }
    assert reason == "circuit_open"

    async def timed_out(*_args, **_kwargs):
        raise TimeoutError

    monkeypatch.setattr(chat, "_run_health_check_with_retries", timed_out)
    closed = chat.CircuitBreaker(failure_threshold=3, reset_timeout_s=60)
    payload, reason = asyncio.run(
        chat._run_dependency_check(
            lambda: None,
            0.05,
            circuit_breaker=closed,
            include_enabled=True,
        )
    )
    assert payload["healthy"] is False
    assert payload["enabled"] is True
    assert reason == "timeout"

    async def boom(*_args, **_kwargs):
        raise RuntimeError("probe failed")

    monkeypatch.setattr(chat, "_run_health_check_with_retries", boom)
    payload, reason = asyncio.run(
        chat._run_dependency_check(
            lambda: None,
            0.05,
            circuit_breaker=chat.CircuitBreaker(failure_threshold=3, reset_timeout_s=60),
            include_enabled=True,
        )
    )
    assert payload["healthy"] is False
    assert payload["enabled"] is True
    assert reason == "probe failed"


def test_health_endpoints_surface_chat_chain_failure_reason(monkeypatch):
    monkeypatch.setattr(
        chat.JSONResponse, "render", lambda _self, content: json.dumps(content).encode()
    )
    monkeypatch.setattr(
        chat,
        "_health_payload",
        lambda: {"healthy": True, "uptime_s": 1},
    )
    monkeypatch.setattr(chat, "_health_summary_timeout_seconds", lambda: 0.2)
    monkeypatch.setattr(chat, "_db_timeout_seconds", lambda: 0.1)
    monkeypatch.setattr(chat, "_minio_timeout_seconds", lambda: 0.1)
    monkeypatch.setattr(chat, "_redis_timeout_seconds", lambda: 0.1)
    monkeypatch.delenv("REDIS_URL", raising=False)

    async def summary_checks(*_args):
        return {
            "database": ({"healthy": True, "latency_ms": 1}, None),
            "minio": ({"healthy": True, "latency_ms": 1}, None),
            "redis": ({"healthy": True, "latency_ms": 0, "enabled": False}, None),
        }

    monkeypatch.setattr(chat, "_run_health_summary_checks", summary_checks)
    monkeypatch.setattr(
        chat,
        "_chat_chain_health_payload",
        lambda: (
            {"healthy": False, "available": False, "fallback_enabled": False},
            "rag_search: missing",
        ),
    )

    summary = asyncio.run(chat.health_app())
    assert summary.status_code == 503
    summary_body = json.loads(summary.body)
    assert summary_body["failed_checks"]["chat_chains"] == "rag_search: missing"

    async def healthy_db():
        return chat.JSONResponse(status_code=200, content={"healthy": True, "latency_ms": 1})

    async def ok_check(*_args, **_kwargs):
        return None

    monkeypatch.setattr(chat, "health_db", healthy_db)
    monkeypatch.setattr(chat, "_run_health_check_with_retries", ok_check)
    monkeypatch.setattr(chat, "_MINIO_CIRCUIT", chat.CircuitBreaker())
    monkeypatch.setattr(chat, "_REDIS_CIRCUIT", chat.CircuitBreaker())

    detailed = asyncio.run(chat.health_detailed())
    assert detailed.status_code == 503
    detailed_body = json.loads(detailed.body)
    assert detailed_body["components"]["chat_chains"]["reason_present"] is True
