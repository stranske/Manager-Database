import contextlib

import pytest

from adapters import uk


def _pdf_bytes_with_text(lines: list[str]) -> bytes:
    # Minimal PDF-like bytes with literal strings for parser extraction.
    joined = "\n".join(f"({line}) Tj" for line in lines)
    content = f"BT\n{joined}\nET\n"
    return (
        b"%PDF-1.4\n"
        b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n"
        b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n"
        b"3 0 obj << /Type /Page /Parent 2 0 R /Contents 4 0 R >> endobj\n"
        + f"4 0 obj << /Length {len(content)} >> stream\n".encode("ascii")
        + content.encode("ascii")
        + b"endstream endobj\n%%EOF"
    )


@pytest.mark.asyncio
async def test_parse_uk_pdf_with_labels():
    raw = _pdf_bytes_with_text(
        [
            "Company name: Example Holdings Limited",
            "Filing date: 31 January 2024",
            "Filing type: Confirmation statement",
        ]
    )

    parsed = await uk.parse(raw)

    assert parsed["company_name"] == "Example Holdings Limited"
    assert parsed["filing_date"] == "2024-01-31"
    assert parsed["filing_type"] == "Confirmation statement"


@pytest.mark.asyncio
async def test_parse_uk_pdf_falls_back_on_type_phrase():
    raw = _pdf_bytes_with_text(
        [
            "Company name: Northwind Trading Ltd",
            "Filing date: 2024-02-20",
            "This filing is a confirmation statement for the period.",
        ]
    )

    parsed = await uk.parse(raw)

    assert parsed["company_name"] == "Northwind Trading Ltd"
    assert parsed["filing_date"] == "2024-02-20"
    assert parsed["filing_type"] == "confirmation statement"


@pytest.mark.asyncio
async def test_parse_uk_pdf_handles_non_pdf_bytes():
    parsed = await uk.parse(b"not a pdf")

    assert parsed["company_name"] is None
    assert parsed["filing_date"] is None
    assert parsed["filing_type"] is None
    assert "error" in parsed


@pytest.mark.asyncio
async def test_parse_uk_pdf_handles_missing_text():
    raw = b"%PDF-1.4\n1 0 obj <<>> endobj\n%%EOF"

    parsed = await uk.parse(raw)

    assert parsed["company_name"] is None
    assert parsed["filing_date"] is None
    assert parsed["filing_type"] is None
    assert "error" in parsed


@pytest.mark.asyncio
async def test_list_new_filings_filters_by_date(monkeypatch):
    async def fake_get(*_args, **_kwargs):
        return DummyResponse(
            {
                "items": [
                    {"transaction_id": "t1", "date": "2024-01-02"},
                    {"transaction_id": "t2", "date": "2023-12-31"},
                ]
            }
        )

    @contextlib.asynccontextmanager
    async def fake_tracked_call(*_args, **_kwargs):
        # Replace tracked_call to avoid touching the database in unit tests.
        def _log(_resp):
            return None

        yield _log

    monkeypatch.setattr(uk.httpx.AsyncClient, "get", fake_get)
    monkeypatch.setattr(uk, "tracked_call", fake_tracked_call)

    results = await uk.list_new_filings("12345678", "2024-01-01")

    assert results == [{"transaction_id": "t1", "company_number": "12345678", "date": "2024-01-02"}]


@pytest.mark.asyncio
async def test_download_returns_pdf_bytes(monkeypatch):
    async def fake_get(*_args, **_kwargs):
        return DummyResponse({}, content=b"%PDF-1.4 data")

    @contextlib.asynccontextmanager
    async def fake_tracked_call(*_args, **_kwargs):
        def _log(_resp):
            return None

        yield _log

    monkeypatch.setattr(uk.httpx.AsyncClient, "get", fake_get)
    monkeypatch.setattr(uk, "tracked_call", fake_tracked_call)

    payload = await uk.download({"transaction_id": "tx-1"})

    assert payload.startswith(b"%PDF-1.4")


class DummyResponse:
    # Simple response stub for AsyncClient.get in adapter tests.
    def __init__(self, json_payload, *, content: bytes = b""):
        self._json_payload = json_payload
        self.content = content
        self.status_code = 200

    def json(self):
        return self._json_payload

    def raise_for_status(self):
        return None
