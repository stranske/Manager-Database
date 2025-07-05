import json
from pathlib import Path

import httpx
import pytest

import adapters.edgar as edgar


@pytest.mark.asyncio
async def test_list_new_filings_filters(monkeypatch):
    data = json.loads(Path("tests/data/submissions.json").read_text())

    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *args, **kwargs):
            return httpx.Response(200, request=httpx.Request("GET", "x"), json=data)

    monkeypatch.setattr(edgar.httpx, "AsyncClient", DummyClient)
    filings = await edgar.list_new_filings("0", "2024-05-05")
    assert len(filings) == 1
    assert filings[0]["accession"] == "3"


@pytest.mark.asyncio
async def test_download_success(monkeypatch):
    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *args, **kwargs):
            return httpx.Response(
                200, text="<xml>ok</xml>", request=httpx.Request("GET", "x")
            )

    monkeypatch.setattr(edgar.httpx, "AsyncClient", DummyClient)
    text = await edgar.download({"accession": "1", "cik": "0"})
    assert "ok" in text
