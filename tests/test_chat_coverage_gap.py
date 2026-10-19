"""Focused contract coverage for chat request helpers."""

import asyncio
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

    monkeypatch.setattr(chat, "is_sqlite", lambda _conn: False)
    monkeypatch.setattr(chat, "table_exists", lambda _conn, _name: False)
    with pytest.raises(RuntimeError, match="run migrations"):
        chat._ensure_chat_feedback_table(object())


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
    body = payload.body.decode()
    assert '"minio":{"healthy":false' in body
    assert '"redis":{"healthy":false' in body


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
