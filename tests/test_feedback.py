from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

import api.chat as chat_api_module


async def _request(
    method: str,
    path: str,
    *,
    json: dict | None = None,
    headers: dict[str, str] | None = None,
):
    await cast(Any, chat_api_module.app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, chat_api_module.app))
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, path, json=json, headers=headers)
    finally:
        await cast(Any, chat_api_module.app.router).shutdown()


@pytest.fixture
def feedback_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "feedback.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    return db_path


@pytest.fixture(autouse=True)
def clear_rate_limiter() -> None:
    chat_api_module.CHAT_RATE_LIMITER.clear()


def test_feedback_endpoint_stores_rating(feedback_db: Path):
    response = asyncio.run(
        _request(
            "POST",
            "/api/chat/feedback",
            json={"response_id": "trace-123", "rating": 5, "comment": "useful"},
        )
    )

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True

    import sqlite3

    conn = sqlite3.connect(feedback_db)
    try:
        row = conn.execute("SELECT response_id, rating, comment FROM chat_feedback").fetchone()
    finally:
        conn.close()

    assert row == ("trace-123", 5, "useful")


def test_feedback_endpoint_attaches_langsmith_feedback_when_available(
    feedback_db: Path, monkeypatch: pytest.MonkeyPatch
):
    calls: dict[str, Any] = {}

    class FakeClient:
        def create_feedback(self, **kwargs: Any) -> None:
            calls.update(kwargs)

    monkeypatch.setattr(chat_api_module, "maybe_enable_langsmith_tracing", lambda: True)
    fake_langsmith = types.ModuleType("langsmith")
    fake_langsmith_any = cast(Any, fake_langsmith)
    fake_langsmith_any.Client = lambda: FakeClient()
    monkeypatch.setitem(sys.modules, "langsmith", fake_langsmith)

    response = asyncio.run(
        _request("POST", "/api/chat/feedback", json={"response_id": "trace-456", "rating": 1})
    )

    assert response.status_code == 200
    assert calls["run_id"] == "trace-456"
    assert calls["score"] == 1


def test_feedback_endpoint_enforces_rate_limit(monkeypatch: pytest.MonkeyPatch):
    limiter = chat_api_module.InMemoryChatRateLimiter(max_requests=1, window_seconds=60)
    monkeypatch.setattr(chat_api_module, "CHAT_RATE_LIMITER", limiter)

    first = asyncio.run(
        _request(
            "POST",
            "/api/chat/feedback",
            json={"response_id": "trace-rate-1", "rating": 5},
            headers={"x-session-id": "session-rate-limit"},
        )
    )
    second = asyncio.run(
        _request(
            "POST",
            "/api/chat/feedback",
            json={"response_id": "trace-rate-2", "rating": 4},
            headers={"x-session-id": "session-rate-limit"},
        )
    )

    assert first.status_code == 200
    assert second.status_code == 429


def test_store_feedback_requires_migrated_postgres_table(monkeypatch: pytest.MonkeyPatch):
    class FakeCursor:
        def __init__(self, row: tuple[Any, ...] | None):
            self._row = row

        def fetchone(self) -> tuple[Any, ...] | None:
            return self._row

    class FakeConn:
        def execute(self, query: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
            _ = params
            if "to_regclass" in query:
                return FakeCursor((None,))
            raise AssertionError(f"Unexpected query: {query}")

        def close(self) -> None:
            return None

    monkeypatch.setattr(chat_api_module, "connect_db", lambda: FakeConn())

    with pytest.raises(RuntimeError, match="chat_feedback table missing"):
        chat_api_module._store_feedback(
            chat_api_module.FeedbackRequest(response_id="trace-pg", rating=5, comment=None)
        )
