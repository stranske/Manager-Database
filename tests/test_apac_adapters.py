from contextlib import asynccontextmanager

import httpx
import pytest

from adapters import asic, mas


@asynccontextmanager
async def dummy_tracked_call(*_args, **_kwargs):
    def _log(_resp):
        return None

    yield _log


@pytest.mark.asyncio
async def test_mas_lists_metadata_records(monkeypatch):
    payload = {
        "result": {"records": [{"_id": "mas-1", "date": "2024-03-01", "entity": "Example Capital"}]}
    }

    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *args, **kwargs):
            return httpx.Response(200, request=httpx.Request("GET", "x"), json=payload)

    monkeypatch.setattr(mas.httpx, "AsyncClient", DummyClient)
    monkeypatch.setattr(mas, "tracked_call", dummy_tracked_call)

    records = await mas.list_new_filings("MAS-123", "2024-01-01")

    assert records == [
        {
            "id": "mas-1",
            "date": "2024-03-01",
            "raw": {"_id": "mas-1", "date": "2024-03-01", "entity": "Example Capital"},
        }
    ]


@pytest.mark.asyncio
async def test_mas_parse_returns_unsupported_status():
    result = await mas.parse(b"metadata")

    assert result == [
        {
            "status": "unsupported",
            "source": "sg",
            "filing_type": "mas_metadata",
            "errors": ["mas_filing_document_endpoint_not_configured"],
            "raw_bytes": len(b"metadata"),
        }
    ]


@pytest.mark.asyncio
async def test_asic_lists_register_snapshot(monkeypatch):
    search_payload = {
        "result": {
            "results": [
                {
                    "resources": [
                        {
                            "format": "CSV",
                            "url": "https://data.gov.au/example/asic-companies.csv",
                        }
                    ]
                }
            ]
        }
    }
    requests: list[str] = []

    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, *args, **kwargs):
            requests.append(url)
            if url == asic.BASE_URL:
                return httpx.Response(
                    200,
                    request=httpx.Request("GET", url),
                    json=search_payload,
                )
            return httpx.Response(
                200,
                request=httpx.Request("GET", url),
                text="ACN,Name\n123,Example Pty Ltd\n",
            )

    monkeypatch.setattr(asic.httpx, "AsyncClient", DummyClient)
    monkeypatch.setattr(asic, "tracked_call", dummy_tracked_call)

    records = await asic.list_new_filings("ASIC-123", "2024-01-01")

    assert records == [
        {
            "id": "ASIC-123",
            "date": "2024-01-01",
            "raw": "ACN,Name\n123,Example Pty Ltd\n",
        }
    ]
    assert requests == [
        asic.BASE_URL,
        "https://data.gov.au/example/asic-companies.csv",
    ]


@pytest.mark.asyncio
async def test_asic_parse_returns_register_count_and_unsupported_status():
    result = await asic.parse(b"ACN,Name\n123,Example Pty Ltd\n")

    assert result == [
        {
            "status": "unsupported",
            "source": "au",
            "filing_type": "asic_register_snapshot",
            "errors": ["asic_filing_documents_paywalled"],
            "record_count": 1,
            "raw_bytes": len(b"ACN,Name\n123,Example Pty Ltd\n"),
        }
    ]
