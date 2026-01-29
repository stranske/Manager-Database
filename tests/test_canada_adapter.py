from contextlib import asynccontextmanager

import httpx
import pytest

from adapters import canada


@pytest.mark.asyncio
async def test_list_new_filings_returns_items(monkeypatch):
    payload = {"items": [{"id": "1"}, {"id": "2"}]}

    @asynccontextmanager
    async def dummy_tracked_call(*_args, **_kwargs):
        def _log(_resp):
            return None

        yield _log

    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *args, **kwargs):
            return httpx.Response(200, request=httpx.Request("GET", "x"), json=payload)

    monkeypatch.setattr(canada.httpx, "AsyncClient", DummyClient)
    monkeypatch.setattr(canada, "tracked_call", dummy_tracked_call)

    # Ensure adapter maps JSON payloads into a list of filing items.
    assert await canada.list_new_filings("0001", "2024-01-01") == payload["items"]


@pytest.mark.asyncio
async def test_download_returns_bytes(monkeypatch):
    pdf_bytes = b"%PDF-1.4\n%fake\n%%EOF"

    @asynccontextmanager
    async def dummy_tracked_call(*_args, **_kwargs):
        def _log(_resp):
            return None

        yield _log

    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *args, **kwargs):
            return httpx.Response(200, request=httpx.Request("GET", "x"), content=pdf_bytes)

    monkeypatch.setattr(canada.httpx, "AsyncClient", DummyClient)
    monkeypatch.setattr(canada, "tracked_call", dummy_tracked_call)

    assert await canada.download({"id": "filing-1"}) == pdf_bytes


@pytest.mark.asyncio
async def test_parse_returns_raw_length():
    raw = b"file contents"

    # Coverage for the minimal parse path.
    assert await canada.parse(raw) == [{"raw_bytes": len(raw)}]
