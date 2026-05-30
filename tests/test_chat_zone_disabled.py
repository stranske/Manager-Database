"""Tests for the LLM data-zone switch on the chat boundary (issue #1085).

The Research chat path is the single LLM boundary. When the deployment runs in
an internal/on-prem zone with no authorized provider, the boundary must close
cleanly (a structured 200 notice) instead of raising the bare 503 it used to,
while deterministic surfaces stay unaffected.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import api.chat as chat_api_module


@pytest.fixture(autouse=True)
def _reset_chat_rate_limiter():
    chat_api_module.CHAT_RATE_LIMITER.clear()
    yield
    chat_api_module.CHAT_RATE_LIMITER.clear()


async def _request(
    method: str,
    path: str,
    *,
    json_body: dict | None = None,
    headers: dict | None = None,
):
    await cast(Any, chat_api_module.app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, chat_api_module.app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.request(method, path, json=json_body, headers=headers)
    finally:
        await cast(Any, chat_api_module.app.router).shutdown()


def test_chat_disabled_zone_returns_notice_not_503(monkeypatch):
    monkeypatch.setenv("LLM_ZONE", "disabled")
    # With the zone closed, the provider boundary must never be probed — that is
    # exactly the call that raised 503 before this switch existed.
    monkeypatch.setattr(
        chat_api_module,
        "_build_chat_client_info",
        lambda: pytest.fail("LLM boundary probed while zone disabled"),
    )

    response = asyncio.run(
        _request(
            "POST",
            "/api/chat",
            json_body={"question": "Summarize the latest filing"},
            headers={"x-session-id": "zone-disabled-1"},
        )
    )

    assert response.status_code == 200
    body = response.json()
    assert body["chat_disabled"] is True
    assert body["chain_used"] == "disabled"
    assert "disabled" in body["answer"].lower()


def test_chat_disabled_zone_still_uses_rate_limit(monkeypatch):
    limiter = chat_api_module.InMemoryChatRateLimiter(max_requests=1, window_seconds=60)
    monkeypatch.setattr(chat_api_module, "CHAT_RATE_LIMITER", limiter)
    monkeypatch.setenv("LLM_ZONE", "disabled")

    first = asyncio.run(
        _request(
            "POST",
            "/api/chat",
            json_body={"question": "first"},
            headers={"x-session-id": "zone-disabled-limit"},
        )
    )
    second = asyncio.run(
        _request(
            "POST",
            "/api/chat",
            json_body={"question": "second"},
            headers={"x-session-id": "zone-disabled-limit"},
        )
    )

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.json() == {"detail": "Rate limit exceeded"}


def test_chat_without_zone_still_requires_provider(monkeypatch):
    # Default zone: behaviour is unchanged — no provider still yields 503.
    monkeypatch.delenv("LLM_ZONE", raising=False)
    monkeypatch.setattr(chat_api_module, "_build_chat_client_info", lambda: None)

    response = asyncio.run(
        _request(
            "POST",
            "/api/chat",
            json_body={"question": "anything"},
            headers={"x-session-id": "zone-default-1"},
        )
    )

    assert response.status_code == 503


def test_deterministic_surface_unaffected_when_zone_disabled(monkeypatch):
    # The deterministic API surfaces still serve while the chat boundary is closed.
    monkeypatch.setenv("LLM_ZONE", "disabled")
    monkeypatch.setattr(chat_api_module, "_ping_db", lambda _timeout: None)
    monkeypatch.setattr(chat_api_module, "_ping_minio", lambda _timeout: None)

    response = asyncio.run(_request("GET", "/openapi.json"))

    assert response.status_code == 200
    assert "/api/chat" in response.json()["paths"]

    health_response = asyncio.run(_request("GET", "/health/detailed"))
    assert health_response.status_code == 200
    assert health_response.json()["healthy"] is True


def test_manager_route_unaffected_when_zone_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("LLM_ZONE", "disabled")
    monkeypatch.setenv("DB_PATH", str(tmp_path / "managers.db"))

    create_response = asyncio.run(
        _request(
            "POST",
            "/managers",
            json_body={
                "name": "Elliott Investment Management L.P.",
                "cik": "0001791786",
                "jurisdictions": ["us"],
                "tags": ["activist"],
            },
        )
    )
    list_response = asyncio.run(_request("GET", "/managers"))

    assert create_response.status_code == 201
    assert list_response.status_code == 200
    assert list_response.json()["items"][0]["name"] == "Elliott Investment Management L.P."


def test_chat_zone_disabled_helper_reads_env(monkeypatch):
    monkeypatch.setenv("LLM_ZONE", "Disabled")
    assert chat_api_module._chat_zone_disabled() is True
    monkeypatch.setenv("LLM_ZONE", "enabled")
    assert chat_api_module._chat_zone_disabled() is False
    monkeypatch.delenv("LLM_ZONE", raising=False)
    assert chat_api_module._chat_zone_disabled() is False
