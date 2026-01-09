import sys
from pathlib import Path

import httpx
import logging
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import adapters.edgar as edgar


@pytest.mark.asyncio
async def test_parse_sample_xml():
    raw = Path("tests/data/sample_13f.xml").read_text()
    rows = await edgar.parse(raw)
    assert rows == [
        {
            "nameOfIssuer": "Example Corp",
            "cusip": "123456789",
            "value": 1000,
            "sshPrnamt": 100,
        }
    ]


@pytest.mark.asyncio
async def test_download_handles_429(monkeypatch, caplog):
    attempts = {"count": 0}

    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *a, **k):
            attempts["count"] += 1
            return httpx.Response(429, request=httpx.Request("GET", "x"))

    monkeypatch.setattr(edgar.httpx, "AsyncClient", DummyClient)
    caplog.set_level(logging.ERROR, logger="adapters.edgar")
    with pytest.raises(httpx.HTTPStatusError):
        await edgar.list_new_filings("0000000000", "2024-01-01")
    assert attempts["count"] == 3
    assert any("EDGAR request failed after retries" in msg for msg in caplog.messages)
