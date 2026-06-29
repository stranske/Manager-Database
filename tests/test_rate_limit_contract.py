from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import api.chat as chat_api_module
from api import data as data_api

RATE_LIMIT_HEADERS = {
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "retry-after",
}

REPO_ROOT = Path(__file__).resolve().parents[1]


async def _request(
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    json: Any | None = None,
    params: dict[str, Any] | None = None,
):
    await cast(Any, chat_api_module.app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, chat_api_module.app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.request(
                method,
                path,
                headers=headers,
                json=json,
                params=params,
            )
    finally:
        await cast(Any, chat_api_module.app.router).shutdown()


def _assert_no_rate_limit_headers(response: httpx.Response) -> None:
    present = RATE_LIMIT_HEADERS.intersection(response.headers.keys())
    assert present == set()


@pytest.fixture(autouse=True)
def _clear_chat_rate_limiter():
    chat_api_module.CHAT_RATE_LIMITER.clear()
    yield
    chat_api_module.CHAT_RATE_LIMITER.clear()


@pytest.mark.parametrize(
    ("method", "path", "kwargs"),
    [
        ("GET", "/chat", {"params": {"q": "missing docs"}}),
        ("POST", "/api/chat", {"json": {"question": "Summarize filings"}}),
        ("POST", "/api/chat/filing-summary", {"params": {"filing_id": 1}}),
        (
            "POST",
            "/api/chat/holdings-analysis",
            {"json": {"question": "Analyze holdings", "context": {}}},
        ),
        ("POST", "/api/chat/query", {"params": {"question": "List managers"}}),
        ("POST", "/api/chat/search", {"params": {"question": "Find filings"}}),
        ("GET", "/managers", {}),
        ("POST", "/api/managers/bulk", {"json": []}),
        ("GET", "/api/data", {}),
        ("GET", "/health/db", {}),
    ],
)
def test_documented_endpoints_do_not_emit_rate_limit_headers(
    method: str,
    path: str,
    kwargs: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "contract.db"))
    monkeypatch.setattr(chat_api_module, "_build_chat_client_info", lambda: None)

    async def _fake_fetch(_url: str) -> str:
        return "{}"

    monkeypatch.setenv("DATA_API_URL", "http://upstream.test/data")
    monkeypatch.setattr(data_api, "_fetch_upstream_payload", _fake_fetch)

    response = asyncio.run(_request(method, path, **kwargs))

    assert response.status_code != 429
    _assert_no_rate_limit_headers(response)


def test_chat_write_limiter_is_session_keyed_and_returns_bare_429(
    monkeypatch: pytest.MonkeyPatch,
):
    limiter = chat_api_module.InMemoryChatRateLimiter(max_requests=1, window_seconds=60)
    monkeypatch.setattr(chat_api_module, "CHAT_RATE_LIMITER", limiter)
    monkeypatch.setattr(chat_api_module, "_build_chat_client_info", lambda: None)

    first = asyncio.run(
        _request(
            "POST",
            "/api/chat",
            json={"question": "first"},
            headers={"x-session-id": "contract-session"},
        )
    )
    second = asyncio.run(
        _request(
            "POST",
            "/api/chat",
            json={"question": "second"},
            headers={"x-session-id": "contract-session"},
        )
    )

    assert first.status_code == 503
    assert second.status_code == 429
    assert second.json() == {"detail": "Rate limit exceeded"}
    _assert_no_rate_limit_headers(second)


def test_chat_limiter_rejects_rotating_client_session_headers(
    monkeypatch: pytest.MonkeyPatch,
):
    limiter = chat_api_module.InMemoryChatRateLimiter(max_requests=10, window_seconds=60)
    monkeypatch.setattr(chat_api_module, "CHAT_RATE_LIMITER", limiter)
    monkeypatch.setattr(chat_api_module, "_build_chat_client_info", lambda: None)

    responses = [
        asyncio.run(
            _request(
                "POST",
                "/api/chat",
                json={"question": f"request {index}"},
                headers={"x-session-id": f"rotating-{index}"},
            )
        )
        for index in range(11)
    ]

    assert [response.status_code for response in responses[:10]] == [503] * 10
    assert responses[10].status_code == 429
    assert responses[10].json() == {"detail": "Rate limit exceeded"}
    _assert_no_rate_limit_headers(responses[10])


def test_chat_rate_limit_env_overrides_are_used(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CHAT_RATE_LIMIT_PER_MINUTE", "2")
    monkeypatch.setenv("CHAT_RATE_LIMIT_WINDOW_SECONDS", "3.5")

    limiter = chat_api_module._build_chat_rate_limiter()

    assert limiter._max_requests == 2
    assert limiter._window_seconds == 3.5


def test_feedback_limiter_shares_documented_bare_429_shape(monkeypatch: pytest.MonkeyPatch):
    limiter = chat_api_module.InMemoryChatRateLimiter(max_requests=1, window_seconds=60)
    monkeypatch.setattr(chat_api_module, "CHAT_RATE_LIMITER", limiter)

    first = asyncio.run(
        _request(
            "POST",
            "/api/chat/feedback",
            json={"response_id": "trace-1", "rating": 5},
            headers={"x-session-id": "feedback-contract"},
        )
    )
    second = asyncio.run(
        _request(
            "POST",
            "/api/chat/feedback",
            json={"response_id": "trace-2", "rating": 4},
            headers={"x-session-id": "feedback-contract"},
        )
    )

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.json() == {"detail": "Rate limit exceeded"}
    _assert_no_rate_limit_headers(second)


def test_rate_limit_document_matches_shipped_header_contract():
    doc = (REPO_ROOT / "docs/api_rate_limiting.md").read_text(encoding="utf-8")

    assert "X-RateLimit" not in doc
    assert "Retry-After" not in doc
    assert "POST /api/chat" in doc
    assert "POST /api/chat/filing-summary" in doc
    assert "POST /api/chat/holdings-analysis" in doc
    assert "POST /api/chat/query" in doc
    assert "POST /api/chat/search" in doc
    assert "POST /api/chat/feedback" in doc


def test_api_design_guidelines_do_not_claim_global_rate_limiting():
    doc = (REPO_ROOT / "docs/api_design_guidelines.md").read_text(encoding="utf-8")
    normalized_doc = doc.lower()
    forbidden_global_claims = [
        "all api endpoints are subject to rate limit",
        "all api endpoints are rate limited",
        "all endpoints are subject to rate limit",
        "all endpoints are rate limited",
        "every api endpoint is rate limited",
        "every endpoint is rate limited",
    ]

    assert not any(claim in normalized_doc for claim in forbidden_global_claims), (
        "api_design_guidelines.md must delegate rate-limit scope to "
        "api_rate_limiting.md instead of claiming all endpoints are limited."
    )
    assert "api_rate_limiting.md" in doc
