import asyncio
import sys
from pathlib import Path
from typing import Any, cast

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api import data as data_api
from api.chat import app


async def _get_data():
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get("/api/data")
    finally:
        await cast(Any, app.router).shutdown()


def _patch_upstream(monkeypatch, text: str) -> None:
    monkeypatch.setenv("DATA_API_URL", "http://upstream.test/data")

    async def _fake_fetch(_url: str) -> str:
        return text

    monkeypatch.setattr(data_api, "_fetch_upstream_payload", _fake_fetch)


def test_api_data_malformed_json_returns_400(monkeypatch):
    _patch_upstream(monkeypatch, "{")
    resp = asyncio.run(_get_data())
    assert resp.status_code == 400
    payload = resp.json()
    assert "error" in payload


def test_api_data_empty_response_returns_400(monkeypatch):
    _patch_upstream(monkeypatch, "")
    resp = asyncio.run(_get_data())
    assert resp.status_code == 400
    payload = resp.json()
    assert "error" in payload


def test_api_data_invalid_structure_returns_400(monkeypatch):
    _patch_upstream(monkeypatch, "[]")
    resp = asyncio.run(_get_data())
    assert resp.status_code == 400
    payload = resp.json()
    assert "error" in payload


def test_api_data_missing_upstream_url_returns_500(monkeypatch):
    monkeypatch.delenv("DATA_API_URL", raising=False)

    response = asyncio.run(_get_data())

    assert response.status_code == 500
    assert response.json() == {"error": "DATA_API_URL is not configured."}


def test_api_data_upstream_request_failure_returns_502(monkeypatch):
    upstream_url = "http://upstream.test/data"
    monkeypatch.setenv("DATA_API_URL", upstream_url)

    async def _failing_fetch(url: str) -> str:
        raise httpx.RequestError("offline", request=httpx.Request("GET", url))

    monkeypatch.setattr(data_api, "_fetch_upstream_payload", _failing_fetch)

    response = asyncio.run(_get_data())

    assert response.status_code == 502
    assert response.json() == {"error": "Upstream API request failed."}
