import logging
import sys
from pathlib import Path

import httpx
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


@pytest.mark.asyncio
async def test_list_new_filings_filters_13f(monkeypatch):
    payload = {
        "filings": {
            "recent": {
                "form": ["13F-HR", "10-K"],
                "filingDate": ["2024-02-01", "2024-02-02"],
                "accessionNumber": ["0001-01", "0002-02"],
            }
        }
    }

    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *a, **k):
            return httpx.Response(200, request=httpx.Request("GET", "x"), json=payload)

    monkeypatch.setattr(edgar.httpx, "AsyncClient", DummyClient)
    filings = await edgar.list_new_filings("1234", "2024-01-01")

    assert filings == [
        {"accession": "0001-01", "cik": "1234", "filed": "2024-02-01"}
    ]


@pytest.mark.asyncio
async def test_list_new_filings_raises_when_empty(monkeypatch):
    payload = {"filings": {"recent": {"form": [], "filingDate": [], "accessionNumber": []}}}

    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *a, **k):
            return httpx.Response(200, request=httpx.Request("GET", "x"), json=payload)

    monkeypatch.setattr(edgar.httpx, "AsyncClient", DummyClient)
    # No 13F-HR filings should trigger the adapter guardrail.
    with pytest.raises(UserWarning):
        await edgar.list_new_filings("1234", "2024-01-01")


@pytest.mark.asyncio
async def test_download_returns_text(monkeypatch):
    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *a, **k):
            return httpx.Response(
                200, request=httpx.Request("GET", "x"), text="<xml></xml>"
            )

    monkeypatch.setattr(edgar.httpx, "AsyncClient", DummyClient)

    assert await edgar.download({"accession": "0001-01", "cik": "1234"}) == "<xml></xml>"


@pytest.mark.asyncio
async def test_request_with_retry_recovers_from_request_error(monkeypatch, caplog):
    attempts = {"count": 0}

    class DummyClient:
        async def get(self, *a, **k):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise httpx.RequestError("boom", request=httpx.Request("GET", "x"))
            return httpx.Response(200, request=httpx.Request("GET", "x"))

    async def _noop_sleep(_delay):
        return None

    monkeypatch.setattr(edgar.asyncio, "sleep", _noop_sleep)
    caplog.set_level(logging.WARNING, logger="adapters.edgar")

    resp = await edgar._request_with_retry(
        DummyClient(), "http://x", headers={"User-Agent": "ua"}, source="edgar"
    )

    assert resp.status_code == 200
    assert attempts["count"] == 3
    assert any("EDGAR request failed; retrying" in msg for msg in caplog.messages)
