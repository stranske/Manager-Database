from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

import api.chat as chat_api_module


async def _request(method: str, path: str, *, json: dict | None = None):
    await chat_api_module.app.router.startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, chat_api_module.app))
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, path, json=json)
    finally:
        await chat_api_module.app.router.shutdown()


@pytest.fixture
def feedback_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "feedback.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    return db_path


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
