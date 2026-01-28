from contextlib import asynccontextmanager

import httpx
import pytest

from adapters import uk


def _make_pdf_bytes(*lines: str) -> bytes:
    content = "\n".join(f"({line})" for line in lines)
    return f"%PDF-1.4\n{content}\n%%EOF".encode("latin-1")


@pytest.mark.asyncio
async def test_parse_confirmation_statement_extracts_fields():
    raw = _make_pdf_bytes(
        "Confirmation Statement CS01",
        "Company Name: Example Widgets Ltd",
        "Company number: 01234567",
        "Date of filing: 12/10/2024",
    )

    result = await uk.parse(raw)

    assert result["filing_type"] == "confirmation_statement"
    assert result["company_name"] == "Example Widgets Ltd"
    assert result["company_number"] == "01234567"
    assert result["filing_date"] == "2024-10-12"
    assert result["errors"] == []


@pytest.mark.asyncio
async def test_parse_annual_return_parses_named_date():
    raw = _make_pdf_bytes(
        "Annual Return AR01",
        "Name of company",
        "Northern Tools PLC",
        "Made up date: 7 October 2023",
    )

    result = await uk.parse(raw)

    assert result["filing_type"] == "annual_return"
    assert result["company_name"] == "Northern Tools PLC"
    assert result["filing_date"] == "2023-10-07"
    assert result["errors"] == []


@pytest.mark.asyncio
async def test_parse_date_label_variants():
    # Ensure label variants from Companies House forms are recognized.
    cs01 = _make_pdf_bytes(
        "Confirmation Statement CS01",
        "Company Name: Example Widgets Ltd",
        "Confirmation date: 01/10/2024",
    )
    ar01 = _make_pdf_bytes(
        "Annual Return AR01",
        "Company Name: Example Widgets Ltd",
        "Date of this return: 01/11/2024",
    )

    cs01_result = await uk.parse(cs01)
    ar01_result = await uk.parse(ar01)

    assert cs01_result["filing_date"] == "2024-10-01"
    assert ar01_result["filing_date"] == "2024-11-01"


@pytest.mark.asyncio
async def test_parse_empty_pdf_returns_error():
    result = await uk.parse(b"")

    assert result["company_name"] is None
    assert result["filing_date"] is None
    assert result["filing_type"] == "unsupported"
    assert result["errors"] == ["empty_pdf"]


@pytest.mark.asyncio
async def test_parse_unreadable_pdf_returns_error():
    result = await uk.parse(b"%PDF-1.4\n%%EOF")

    assert result["company_name"] is None
    assert result["filing_date"] is None
    assert result["filing_type"] == "unsupported"
    assert result["errors"] == ["unreadable_pdf"]


@pytest.mark.asyncio
async def test_parse_non_pdf_bytes_returns_error():
    # Non-PDF bytes should be treated as unreadable input.
    result = await uk.parse(b"not a pdf")

    assert result["company_name"] is None
    assert result["filing_date"] is None
    assert result["filing_type"] == "unsupported"
    assert result["errors"] == ["unreadable_pdf"]


@pytest.mark.asyncio
async def test_parse_unsupported_filing_type_marks_error():
    raw = _make_pdf_bytes(
        "Change of Accounting Reference Date",
        "Company Name: Horizon Labs Ltd",
        "Date of filing: 2024-03-05",
    )

    result = await uk.parse(raw)

    assert result["filing_type"] == "unsupported"
    assert result["errors"] == ["unsupported_filing_type"]


@pytest.mark.asyncio
async def test_list_new_filings_filters_and_maps(monkeypatch):
    payload = {
        "items": [
            {"transaction_id": "t1", "date": "2024-01-02T10:00:00Z"},
            {"transaction_id": "t2", "date": "2023-12-31T09:00:00Z"},
        ]
    }

    @asynccontextmanager
    async def dummy_tracked_call(*args, **kwargs):
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

    monkeypatch.setattr(uk.httpx, "AsyncClient", DummyClient)
    monkeypatch.setattr(uk, "tracked_call", dummy_tracked_call)

    filings = await uk.list_new_filings("12345678", "2024-01-01")

    assert filings == [
        {
            "transaction_id": "t1",
            "company_number": "12345678",
            "date": "2024-01-02",
        }
    ]


@pytest.mark.asyncio
async def test_download_returns_pdf_bytes(monkeypatch):
    pdf_bytes = b"%PDF-1.4\n%fake\n%%EOF"

    @asynccontextmanager
    async def dummy_tracked_call(*args, **kwargs):
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

    monkeypatch.setattr(uk.httpx, "AsyncClient", DummyClient)
    monkeypatch.setattr(uk, "tracked_call", dummy_tracked_call)

    result = await uk.download({"transaction_id": "t1"})

    assert result == pdf_bytes


def test_unescape_pdf_string_and_date_helpers_cover_branches():
    assert uk._unescape_pdf_string(r"Hello\040World\041") == "Hello World!"
    assert uk._unescape_pdf_string(r"Line\(") == "Line("
    assert uk._unescape_pdf_string(r"Value\053") == "Value+"
    assert uk._parse_date_from_line("Statement date: October 7, 2023") == "2023-10-07"
    assert uk._parse_date_from_line("Bad date 32/13/2023") is None
    assert uk._parse_date_from_line("Short date 07/10/23") == "2023-10-07"
    assert uk._format_named_date("7", "Oct", "2023") == "2023-10-07"
    assert uk._find_company_number(["Company 12345678"]) == "12345678"
