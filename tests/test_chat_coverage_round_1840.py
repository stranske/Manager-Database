"""Autopilot round 1840: additional api/chat.py branch coverage."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api import chat


def test_resolve_chain_name_auto_filing_requires_context_filing_id():
    assert chat._resolve_chain_name("auto", "summarize the 13F filing", {}) == "rag_search"
    assert (
        chat._resolve_chain_name("auto", "summarize the 13F filing", {"filing_id": 9})
        == "filing_summary"
    )
    assert chat._resolve_chain_name("nl_query", "anything", None) == "nl_query"


def test_context_filter_requested_detects_empty_and_non_empty_values():
    assert not chat._context_filter_requested({}, "manager_ids")
    assert chat._context_filter_requested({"manager_ids": []}, "manager_ids") is False
    assert chat._context_filter_requested({"manager_ids": [1]}, "manager_ids") is True
    assert chat._context_filter_requested({"manager_id": 2}, "manager_id") is True
    assert chat._context_filter_requested({"note": "x"}, "manager_ids") is False


def test_normalize_sources_and_extract_chain_payload_object_paths():
    assert chat._normalize_sources("plain") == []
    assert chat._normalize_sources({"k": "v"}) == [{"k": "v"}]
    assert chat._normalize_sources([1, {"a": 1}]) == [{"description": "1"}, {"a": 1}]

    class ChainResult:
        answer = "done"
        response = "ignored"
        sources = {"doc": 1}
        sql = "SELECT 1"
        trace_url = "https://trace/r/run-99"

    assert chat._extract_chain_payload("text-only") == ("text-only", [], None, None)
    assert chat._extract_chain_payload({"answer": "a", "sources": []}) == ("a", [], None, None)
    assert chat._extract_chain_payload(ChainResult()) == (
        "done",
        [{"doc": 1}],
        "SELECT 1",
        "https://trace/r/run-99",
    )


def test_get_health_executor_recreates_after_shutdown(monkeypatch):
    original = chat._HEALTH_EXECUTOR
    try:
        original.shutdown(wait=False, cancel_futures=True)
        replacement = chat.get_health_executor()
        assert replacement is not original
        assert not replacement._shutdown
    finally:
        from concurrent.futures import ThreadPoolExecutor

        chat._HEALTH_EXECUTOR = ThreadPoolExecutor(max_workers=3)


def test_record_chat_fleet_safe_swallows_observability_failures(monkeypatch):
    def boom(**_kwargs):
        raise RuntimeError("fleet down")

    monkeypatch.setattr(chat, "record_chat_event", boom)
    chat._record_chat_fleet_safe(
        request_id="req",
        endpoint="/api/chat",
        chain="rag_search",
        session_id="s",
        provider="openai",
        model="gpt",
        latency_ms=1,
        http_status=200,
    )


@pytest.mark.asyncio
async def test_run_chain_rag_campaign_snapshot_unavailable(monkeypatch):
    class RAGChain:
        def run(self, *, question, context):
            return {"campaign_snapshot_status": "unavailable", "answer": "ignored"}

        db = SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(chat, "_build_chain", lambda *_args, **_kwargs: RAGChain())
    monkeypatch.setattr(chat, "connect_db", lambda: SimpleNamespace(close=lambda: None))

    answer, sources, sql, trace = await chat._run_chain(
        "rag_search", "campaign scope", {}, SimpleNamespace(client=None)
    )
    assert "No materialized campaign snapshot" in answer
    assert sources == []
    assert sql is None
    assert trace is None


@pytest.mark.asyncio
async def test_run_chain_rejects_invalid_holdings_as_of(monkeypatch):
    monkeypatch.setattr(chat, "connect_db", lambda: SimpleNamespace(close=lambda: None))
    with pytest.raises(HTTPException, match="as_of must be a valid ISO"):
        await chat._run_chain(
            "holdings_analysis",
            "positions",
            {"as_of": "not-a-date"},
            None,
        )


@pytest.mark.asyncio
async def test_run_chain_requires_filing_id_for_filing_summary(monkeypatch):
    monkeypatch.setattr(chat, "connect_db", lambda: SimpleNamespace(close=lambda: None))

    class FilingChain:
        def run(self, _filing_id):
            return {"answer": "unused"}

        db = SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(chat, "_build_chain", lambda *_a, **_k: FilingChain())
    with pytest.raises(HTTPException, match="filing_summary requires context.filing_id"):
        await chat._run_chain("filing_summary", "q", {}, None)


@pytest.mark.asyncio
async def test_run_chain_awaits_coroutine_results(monkeypatch):
    monkeypatch.setattr(chat, "connect_db", lambda: SimpleNamespace(close=lambda: None))

    class AsyncChain:
        async def run(self, *, question, context):
            return {"answer": f"async:{question}", "sources": []}

        db = SimpleNamespace(close=lambda: (_ for _ in ()).throw(RuntimeError("close failed")))

    monkeypatch.setattr(chat, "_build_chain", lambda *_a, **_k: AsyncChain())
    answer, sources, sql, trace = await chat._run_chain(
        "nl_query", "hello", {}, SimpleNamespace(client=None)
    )
    assert answer == "async:hello"
    assert sources == []
    assert sql is None
    assert trace is None


def test_build_chain_uses_fallback_after_generic_import_error(monkeypatch):
    monkeypatch.setattr(chat, "_chat_chain_fallback_enabled", lambda: True)

    def boom(_name):
        raise ImportError("broken chain")

    monkeypatch.setattr(chat.importlib, "import_module", boom)
    chain = chat._build_chain("nl_query", None)
    assert isinstance(chain, chat._FallbackNLQueryChain)


def test_chat_chain_health_when_llm_zone_disabled(monkeypatch):
    monkeypatch.setenv("LLM_ZONE", "disabled")
    payload, reason = chat._chat_chain_health_payload()
    assert payload == {"healthy": True, "available": True, "fallback_enabled": False}
    assert reason is None


def test_enforce_chat_rate_limit_raises_when_budget_exceeded():
    limiter = chat.InMemoryChatRateLimiter(max_requests=1, window_seconds=60)
    ip_limiter = chat.InMemoryChatRateLimiter(max_requests=1, window_seconds=60)
    original_limiter, original_ip = chat.CHAT_RATE_LIMITER, chat.CHAT_IP_RATE_LIMITER
    try:
        chat.CHAT_RATE_LIMITER = limiter
        chat.CHAT_IP_RATE_LIMITER = ip_limiter
        request = SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"), cookies={})
        chat._enforce_chat_rate_limit(request)
        with pytest.raises(HTTPException) as first:
            chat._enforce_chat_rate_limit(request)
        assert first.value.status_code == 429
    finally:
        chat.CHAT_RATE_LIMITER = original_limiter
        chat.CHAT_IP_RATE_LIMITER = original_ip


@pytest.mark.asyncio
async def test_direct_chat_wrappers_delegate_to_chat_api(monkeypatch):
    captured = {}

    async def fake_chat_api(request, raw_request):
        captured["request"] = request
        captured["raw"] = raw_request
        return chat.ChatResponse(
            answer="ok",
            chain_used=request.chain or "auto",
            sources=[],
            sql=None,
            trace_url=None,
            latency_ms=0,
            response_id="rid",
        )

    monkeypatch.setattr(chat, "chat_api", fake_chat_api)
    raw = SimpleNamespace(cookies={}, client=None)
    await chat.filing_summary(42, raw)
    assert captured["request"].chain == "filing_summary"
    assert captured["request"].context == {"filing_id": 42}

    await chat.holdings_analysis(
        chat.HoldingsAnalysisRequest(question="analyze", context={"manager_ids": [1]}), raw
    )
    assert captured["request"].chain == "holdings_analysis"

    await chat.nl_query("sql please", raw)
    assert captured["request"].chain == "nl_query"

    await chat.rag_search("find docs", raw)
    assert captured["request"].chain == "rag_search"


def test_resolve_chain_name_falls_back_when_classifier_returns_unknown(monkeypatch):
    monkeypatch.setattr(chat, "_classify_intent", lambda _q: "mystery_chain")
    assert chat._resolve_chain_name("auto", "anything", {}) == "rag_search"


def test_enforce_chat_rate_limit_session_budget_after_ip_passes():
    ip_limiter = chat.InMemoryChatRateLimiter(max_requests=10, window_seconds=60)
    session_limiter = chat.InMemoryChatRateLimiter(max_requests=1, window_seconds=60)
    original_limiter, original_ip = chat.CHAT_RATE_LIMITER, chat.CHAT_IP_RATE_LIMITER
    try:
        chat.CHAT_IP_RATE_LIMITER = ip_limiter
        chat.CHAT_RATE_LIMITER = session_limiter
        request = SimpleNamespace(client=SimpleNamespace(host="10.0.0.9"), cookies={})
        chat._enforce_chat_rate_limit(request)
        with pytest.raises(HTTPException) as exc:
            chat._enforce_chat_rate_limit(request)
        assert exc.value.status_code == 429
        assert exc.value.detail == "Rate limit exceeded"
    finally:
        chat.CHAT_RATE_LIMITER = original_limiter
        chat.CHAT_IP_RATE_LIMITER = original_ip


@pytest.mark.asyncio
async def test_run_chain_holdings_analysis_formats_payload_with_as_of(monkeypatch):
    monkeypatch.setattr(chat, "connect_db", lambda: SimpleNamespace(close=lambda: None))
    seen = {}

    class HoldingsChain:
        def run(self, question, **kwargs):
            seen["question"] = question
            seen["kwargs"] = kwargs
            return {
                "thesis": "Concentrated book.",
                "top_positions": [{"cusip": "037833100"}],
                "period_changes": [],
                "concentration_metrics": {"top_10": 0.55},
            }

        db = SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(chat, "_build_chain", lambda *_a, **_k: HoldingsChain())
    answer, sources, sql, trace = await chat._run_chain(
        "holdings_analysis",
        "analyze positions",
        {
            "manager_ids": [3],
            "cusips": ["037833100"],
            "as_of": "2024-06-15T12:00:00Z",
        },
        SimpleNamespace(client=None),
    )
    assert seen["question"] == "analyze positions"
    assert seen["kwargs"]["manager_ids"] == [3]
    assert seen["kwargs"]["cusips"] == ["037833100"]
    assert seen["kwargs"]["as_of"].isoformat().startswith("2024-06-15")
    assert "Concentrated book." in answer
    assert "037833100" in answer
    assert sql is None
    assert trace is None


@pytest.mark.asyncio
async def test_run_chain_rejects_non_string_holdings_as_of(monkeypatch):
    monkeypatch.setattr(chat, "connect_db", lambda: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(
        chat,
        "_normalize_chain_context",
        lambda context, _conn: {"as_of": 20240615},
    )
    with pytest.raises(HTTPException, match="as_of must be a valid ISO"):
        await chat._run_chain("holdings_analysis", "positions", {"as_of": 20240615}, None)


@pytest.mark.asyncio
async def test_run_chain_uses_invoke_when_run_missing(monkeypatch):
    monkeypatch.setattr(chat, "connect_db", lambda: SimpleNamespace(close=lambda: None))

    class InvokeOnlyChain:
        def invoke(self, payload):
            return {
                "answer": f"invoked:{payload['question']}",
                "sources": [{"context": payload["context"]}],
                "sql": "SELECT 1",
                "trace_url": "https://trace/r/invoke-1",
            }

        db = None

    monkeypatch.setattr(chat, "_build_chain", lambda *_a, **_k: InvokeOnlyChain())
    answer, sources, sql, trace = await chat._run_chain(
        "nl_query", "count managers", {"scope": "all"}, None
    )
    assert answer == "invoked:count managers"
    assert sources == [{"context": {"scope": "all"}}]
    assert sql == "SELECT 1"
    assert trace == "https://trace/r/invoke-1"


@pytest.mark.asyncio
async def test_run_chain_raises_when_chain_lacks_run_and_invoke(monkeypatch):
    monkeypatch.setattr(chat, "connect_db", lambda: SimpleNamespace(close=lambda: None))

    class BrokenChain:
        db = None

    monkeypatch.setattr(chat, "_build_chain", lambda *_a, **_k: BrokenChain())
    with pytest.raises(RuntimeError, match="does not expose run/invoke"):
        await chat._run_chain("rag_search", "q", {}, None)


@pytest.mark.asyncio
async def test_run_chain_filing_summary_formats_structured_payload(monkeypatch):
    monkeypatch.setattr(chat, "connect_db", lambda: SimpleNamespace(close=lambda: None))

    class FilingChain:
        def run(self, filing_id):
            assert filing_id == 77
            return {
                "manager_name": "Northstar",
                "filing_date": "2024-03-31",
                "total_positions": 2,
                "total_aum_estimate": 12,
                "key_positions": [{"issuer": "Alpha"}],
                "notable_changes": ["Added Alpha"],
                "risk_flags": ["Concentrated"],
            }

        db = SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(chat, "_build_chain", lambda *_a, **_k: FilingChain())
    answer, sources, sql, trace = await chat._run_chain(
        "filing_summary", "summarize", {"filing_id": 77}, None
    )
    assert "Northstar" in answer
    assert "Added Alpha" in answer
    assert sources == []
    assert sql is None
    assert trace is None


@pytest.mark.asyncio
async def test_chat_api_disabled_zone_returns_notice(monkeypatch):
    monkeypatch.setattr(chat, "_enforce_chat_rate_limit", lambda _r: None)
    monkeypatch.setattr(chat, "_chat_zone_disabled", lambda: True)
    fleet = []
    monkeypatch.setattr(chat, "_record_chat_fleet_safe", lambda **kwargs: fleet.append(kwargs))

    raw = SimpleNamespace(url=SimpleNamespace(path="/api/chat"), cookies={}, client=None)
    response = await chat.chat_api(chat.ChatRequest(question="hello"), raw)
    assert response.chat_disabled is True
    assert response.chain_used == "disabled"
    assert "disabled" in response.answer.lower()
    assert fleet and fleet[0]["chain"] == "disabled" and fleet[0]["http_status"] == 200


@pytest.mark.asyncio
async def test_chat_api_requires_llm_client(monkeypatch):
    monkeypatch.setattr(chat, "_enforce_chat_rate_limit", lambda _r: None)
    monkeypatch.setattr(chat, "_chat_zone_disabled", lambda: False)
    monkeypatch.setattr(chat, "_build_chat_client_info", lambda: None)

    raw = SimpleNamespace(url=SimpleNamespace(path="/api/chat"), cookies={}, client=None)
    with pytest.raises(HTTPException) as exc:
        await chat.chat_api(chat.ChatRequest(question="hello"), raw)
    assert exc.value.status_code == 503
    assert "No LLM provider" in exc.value.detail


@pytest.mark.asyncio
async def test_chat_api_success_path_records_fleet(monkeypatch):
    monkeypatch.setattr(chat, "_enforce_chat_rate_limit", lambda _r: None)
    monkeypatch.setattr(chat, "_chat_zone_disabled", lambda: False)
    run_calls = []

    async def fake_run_chain(chain_name, question, context, client_info):
        run_calls.append((chain_name, question, context, client_info))
        return ("answer text", [{"doc": 1}], "SELECT 1", "https://trace/r/run-88")

    client_info = SimpleNamespace(provider="openai", model="gpt-test", client=None)
    monkeypatch.setattr(chat, "_build_chat_client_info", lambda: client_info)
    monkeypatch.setattr(chat, "_run_chain", fake_run_chain)
    fleet = []
    monkeypatch.setattr(chat, "_record_chat_fleet_safe", lambda **kwargs: fleet.append(kwargs))

    raw = SimpleNamespace(url=SimpleNamespace(path="/api/chat"), cookies={}, client=None)
    response = await chat.chat_api(chat.ChatRequest(question="sql please", chain="nl_query"), raw)
    assert run_calls == [
        ("nl_query", "sql please", None, client_info),
    ]
    assert response.answer == "answer text"
    assert response.chain_used == "nl_query"
    assert response.sql == "SELECT 1"
    assert response.trace_url == "https://trace/r/run-88"
    assert response.response_id == "run-88"
    assert fleet and fleet[0]["http_status"] == 200 and fleet[0]["chain"] == "nl_query"


async def _async_result(value):
    return value


@pytest.mark.asyncio
async def test_chat_api_maps_chain_unavailable_to_503(monkeypatch):
    monkeypatch.setattr(chat, "_enforce_chat_rate_limit", lambda _r: None)
    monkeypatch.setattr(chat, "_chat_zone_disabled", lambda: False)
    monkeypatch.setattr(
        chat,
        "_build_chat_client_info",
        lambda: SimpleNamespace(provider="openai", model="gpt", client=None),
    )

    async def boom(*_a, **_k):
        raise chat.ChainUnavailableError("rag_search missing")

    monkeypatch.setattr(chat, "_run_chain", boom)
    fleet = []
    monkeypatch.setattr(chat, "_record_chat_fleet_safe", lambda **kwargs: fleet.append(kwargs))

    raw = SimpleNamespace(url=SimpleNamespace(path="/api/chat"), cookies={}, client=None)
    with pytest.raises(HTTPException) as exc:
        await chat.chat_api(chat.ChatRequest(question="find docs", chain="rag_search"), raw)
    assert exc.value.status_code == 503
    assert "rag_search missing" in exc.value.detail
    assert fleet and fleet[0]["error_category"] == "chain_unavailable"


@pytest.mark.asyncio
async def test_chat_api_maps_unexpected_errors_to_500(monkeypatch):
    monkeypatch.setattr(chat, "_enforce_chat_rate_limit", lambda _r: None)
    monkeypatch.setattr(chat, "_chat_zone_disabled", lambda: False)
    monkeypatch.setattr(
        chat,
        "_build_chat_client_info",
        lambda: SimpleNamespace(provider="openai", model="gpt", client=None),
    )

    async def boom(*_a, **_k):
        raise RuntimeError("boom")

    monkeypatch.setattr(chat, "_run_chain", boom)
    fleet = []
    monkeypatch.setattr(chat, "_record_chat_fleet_safe", lambda **kwargs: fleet.append(kwargs))

    raw = SimpleNamespace(url=SimpleNamespace(path="/api/chat"), cookies={}, client=None)
    with pytest.raises(HTTPException) as exc:
        await chat.chat_api(chat.ChatRequest(question="find docs", chain="rag_search"), raw)
    assert exc.value.status_code == 500
    assert fleet and fleet[0]["http_status"] == 500


@pytest.mark.asyncio
async def test_submit_feedback_persists_and_forwards(monkeypatch):
    monkeypatch.setattr(chat, "_enforce_chat_rate_limit", lambda _r: None)
    stored = []
    forwarded = []

    def store_feedback(fb):
        stored.append(fb)
        return 99

    def attach_feedback(fb):
        forwarded.append(fb)
        return True

    monkeypatch.setattr(chat, "_store_feedback", store_feedback)
    monkeypatch.setattr(chat, "_attach_langsmith_feedback", attach_feedback)
    events = []
    monkeypatch.setattr(chat, "record_feedback_event", lambda **kwargs: events.append(kwargs))

    request = chat.FeedbackRequest(response_id="run-z", rating=5, comment="great")
    result = await chat.submit_feedback(
        request,
        SimpleNamespace(cookies={}, client=None),
    )
    assert result == {"ok": True, "feedback_id": 99}
    assert stored == [request]
    assert forwarded == [request]
    assert events == [
        {
            "response_id": "run-z",
            "feedback_id": 99,
            "rating": 5,
            "session_id": "client:unknown",
            "forwarded_to_langsmith": True,
        }
    ]


def test_build_chain_instantiates_imported_classes(monkeypatch):
    constructed = {}

    class FakeFiling:
        def __init__(self, *, client_info, db_conn):
            constructed["filing"] = (client_info, db_conn)

    class FakeHoldings:
        def __init__(self, *, client_info, db_conn):
            constructed["holdings"] = (client_info, db_conn)

    class FakeNL:
        def __init__(self, *, llm, db_conn):
            constructed["nl"] = (llm, db_conn)

    class Conn:
        def close(self):
            constructed["closed"] = True

    modules = {
        "chains.filing_summary": SimpleNamespace(FilingSummaryChain=FakeFiling),
        "chains.holdings_analysis": SimpleNamespace(HoldingsAnalysisChain=FakeHoldings),
        "chains.nl_query": SimpleNamespace(NLQueryChain=FakeNL),
    }

    def fake_import(name):
        if name in modules:
            return modules[name]
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(chat.importlib, "import_module", fake_import)
    conn = Conn()
    monkeypatch.setattr(chat, "connect_db", lambda: conn)
    client = SimpleNamespace(client="llm-client")

    assert isinstance(chat._build_chain("filing_summary", client), FakeFiling)
    assert isinstance(chat._build_chain("holdings_analysis", client), FakeHoldings)
    assert isinstance(chat._build_chain("nl_query", client), FakeNL)
    assert constructed["filing"] == (client, conn)
    assert constructed["holdings"] == (client, conn)
    assert constructed["nl"] == ("llm-client", conn)
