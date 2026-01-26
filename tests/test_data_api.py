import asyncio
import sys
from pathlib import Path

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api import data as data_api
from api.chat import app


async def _get_data():
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get("/api/data")
    finally:
        await app.router.shutdown()


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
