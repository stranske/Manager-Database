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
