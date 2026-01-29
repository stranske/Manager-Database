import zlib
from contextlib import asynccontextmanager

import httpx
import pytest

from adapters import uk


def _make_pdf_bytes(*lines: str) -> bytes:
    content = "\n".join(f"({line})" for line in lines)
    return f"%PDF-1.4\n{content}\n%%EOF".encode("latin-1")


def _make_flate_pdf_bytes(*lines: str) -> bytes:
    content = "\n".join(f"({line})" for line in lines).encode("latin-1")
    compressed = zlib.compress(content)
    header = b"%PDF-1.4\n1 0 obj\n"
    header += b"<< /Length " + str(len(compressed)).encode("ascii") + b" /Filter /FlateDecode >>\n"
    return header + b"stream\n" + compressed + b"\nendstream\nendobj\n%%EOF"


def _make_flate_pdf_with_header(header: bytes, body: bytes) -> bytes:
    return b"%PDF-1.4\n1 0 obj\n" + header + b"\nstream\n" + body + b"\nendstream\nendobj\n%%EOF"


def _make_png_sub_predictor_stream(payload: bytes, columns: int) -> bytes:
    rows = [payload[i : i + columns] for i in range(0, len(payload), columns)]
    encoded = b"".join(b"\x01" + _png_sub_encode_row(row) for row in rows)
    return encoded


def _png_sub_encode_row(row: bytes) -> bytes:
    encoded = bytearray()
    prev = 0
    for value in row:
        encoded.append((value - prev) % 256)
        prev = value
    return bytes(encoded)


def _make_obj_stream_pdf_bytes(*lines: str) -> bytes:
    content = "\n".join(f"({line})" for line in lines).encode("latin-1")
    header = b"%PDF-1.4\n2 0 obj\n"
    header += b"<< /Type /ObjStm /Length " + str(len(content)).encode("ascii") + b" >>\n"
    return header + b"stream\n" + content + b"\nendstream\nendobj\n%%EOF"


def _single_result(results):
    # UK adapter returns a list to align with the shared adapter contract.
    assert len(results) == 1
    return results[0]


@pytest.mark.asyncio
async def test_parse_confirmation_statement_extracts_fields():
    raw = _make_pdf_bytes(
        "Confirmation Statement CS01",
        "Company Name: Example Widgets Ltd",
        "Company number: 01234567",
        "Date of filing: 12/10/2024",
    )

    result = _single_result(await uk.parse(raw))

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

    result = _single_result(await uk.parse(raw))

    assert result["filing_type"] == "annual_return"
    assert result["company_name"] == "Northern Tools PLC"
    assert result["filing_date"] == "2023-10-07"
    assert result["errors"] == []


@pytest.mark.asyncio
async def test_parse_company_name_in_full_label():
    raw = _make_pdf_bytes(
        "Confirmation Statement CS01",
        "Company name in full: Atlas Holdings Ltd",
        "Confirmation date: 02/01/2025",
    )

    result = _single_result(await uk.parse(raw))

    assert result["company_name"] == "Atlas Holdings Ltd"
    assert result["filing_type"] == "confirmation_statement"
    assert result["filing_date"] == "2025-01-02"


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

    cs01_result = _single_result(await uk.parse(cs01))
    ar01_result = _single_result(await uk.parse(ar01))

    assert cs01_result["filing_date"] == "2024-10-01"
    assert ar01_result["filing_date"] == "2024-11-01"


@pytest.mark.asyncio
async def test_parse_empty_pdf_returns_error():
    result = _single_result(await uk.parse(b""))

    assert result["company_name"] is None
    assert result["filing_date"] is None
    assert result["filing_type"] == "error"
    assert result["errors"] == ["empty_pdf"]
    assert result["status"] == "error"


@pytest.mark.asyncio
async def test_parse_unreadable_pdf_returns_error():
    result = _single_result(await uk.parse(b"%PDF-1.4\n%%EOF"))

    assert result["company_name"] is None
    assert result["filing_date"] is None
    assert result["filing_type"] == "error"
    assert result["errors"] == ["unreadable_pdf"]
    assert result["status"] == "error"


@pytest.mark.asyncio
async def test_parse_non_pdf_bytes_returns_error():
    # Non-PDF bytes should be treated as unreadable input.
    result = _single_result(await uk.parse(b"not a pdf"))

    assert result["company_name"] is None
    assert result["filing_date"] is None
    assert result["filing_type"] == "error"
    assert result["errors"] == ["unreadable_pdf"]
    assert result["status"] == "error"


@pytest.mark.asyncio
async def test_parse_unsupported_filing_type_marks_error():
    raw = _make_pdf_bytes(
        "Change of Accounting Reference Date",
        "Company Name: Horizon Labs Ltd",
        "Date of filing: 2024-03-05",
    )

    result = _single_result(await uk.parse(raw))

    assert result["filing_type"] == "unsupported"
    assert result["errors"] == ["unsupported_filing_type"]
    assert result["status"] == "error"


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
    assert uk._parse_date_from_line("Short date 07/10/00") == "2000-10-07"
    assert uk._parse_date_from_line("Short date 07/10/99") == "2099-10-07"
    assert uk._parse_date_from_line("Out of range 07/10/1999") is None
    assert uk._parse_date_from_line("Out of range 07/10/2100") is None
    assert uk._parse_date_from_line("Out of range 07/10/123") is None
    assert uk._format_named_date("7", "Oct", "2023") == "2023-10-07"
    assert uk._find_company_number(["Company 12345678"]) == "12345678"


def test_find_company_number_fallback_scan():
    lines = [
        "Registered in England and Wales",
        "Example Widgets Ltd",
        "01234567",
    ]
    assert uk._find_company_number(lines) == "01234567"


def test_find_company_number_skips_reference_tokens():
    lines = [
        "Confirmation Statement CS01",
        "Reference number: 12345678",
        "Document reference: AB123456",
    ]
    # Avoid false positives from reference codes when no company number exists.
    assert uk._find_company_number(lines) == ""


def test_find_company_number_prefers_standalone_token():
    lines = [
        "Reference number: 12345678",
        "Company details",
        "SC123456",
    ]
    # Standalone company number tokens should still be detected.
    assert uk._find_company_number(lines) == "SC123456"


def test_find_company_number_embedded_in_company_line():
    lines = [
        "Registered in England and Wales",
        "Company number is 01234567 for this filing",
    ]
    assert uk._find_company_number(lines) == "01234567"


def test_find_company_number_embedded_without_company_label():
    lines = [
        "Example Widgets Ltd",
        "Incorporated on 2010-01-01 with number SC123456 in Edinburgh",
    ]
    assert uk._find_company_number(lines) == "SC123456"


def test_find_company_number_ignores_disqualified_embedded_tokens():
    lines = [
        "Document reference 01234567",
        "Payment ref AB123456",
        "Submission form 76543210",
    ]
    assert uk._find_company_number(lines) == ""


def test_extract_pdf_text_handles_flate_stream():
    raw = _make_flate_pdf_bytes(
        "Confirmation Statement CS01",
        "Company Name: Example Widgets Ltd",
        "Company number: 01234567",
    )

    text = uk._extract_pdf_text(raw)

    assert "Example Widgets Ltd" in text
    assert "01234567" in text


def test_iter_pdf_streams_accepts_cr_line_endings():
    content = b"(Hello CR stream)"
    compressed = zlib.compress(content)
    header = b"<< /Length " + str(len(compressed)).encode("ascii") + b" /Filter /FlateDecode >>"
    raw = (
        b"%PDF-1.4\r1 0 obj\r" + header + b"\rstream\r" + compressed + b"\rendstream\rendobj\r%%EOF"
    )

    streams = uk._iter_pdf_streams(raw)

    assert len(streams) == 1
    assert streams[0][1] == compressed


def test_iter_pdf_streams_accepts_windows_line_endings():
    content = b"(Hello CRLF stream)"
    compressed = zlib.compress(content)
    header = b"<< /Length " + str(len(compressed)).encode("ascii") + b" /Filter /FlateDecode >>"
    raw = (
        b"%PDF-1.4\r\n1 0 obj\r\n"
        + header
        + b"\r\nstream\r\n"
        + compressed
        + b"\r\nendstream\r\nendobj\r\n%%EOF"
    )

    streams = uk._iter_pdf_streams(raw)

    assert len(streams) == 1
    assert streams[0][1] == compressed


def test_extract_pdf_text_handles_multiple_filters_with_flate():
    content = b"(Multi filter stream)"
    compressed = zlib.compress(content)
    header = (
        b"<< /Length "
        + str(len(compressed)).encode("ascii")
        + b" /Filter [/ASCII85Decode /FlateDecode] >>"
    )
    raw = _make_flate_pdf_with_header(header, compressed)

    text = uk._extract_pdf_text(raw)

    assert "Multi filter stream" in text


def test_extract_pdf_text_handles_raw_deflate_stream():
    content = b"(Raw deflate stream)"
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    compressed = compressor.compress(content) + compressor.flush()
    header = b"<< /Length " + str(len(compressed)).encode("ascii") + b" /Filter /FlateDecode >>"
    raw = _make_flate_pdf_with_header(header, compressed)

    text = uk._extract_pdf_text(raw)

    assert "Raw deflate stream" in text


def test_extract_pdf_text_handles_png_predictor_sub():
    payload = b"(Predictor Test)"
    columns = len(payload)
    encoded = _make_png_sub_predictor_stream(payload, columns)
    compressed = zlib.compress(encoded)
    header = (
        b"<< /Length "
        + str(len(compressed)).encode("ascii")
        + b" /Filter /FlateDecode /DecodeParms << /Predictor 12 /Columns "
        + str(columns).encode("ascii")
        + b" >> >>"
    )
    raw = _make_flate_pdf_with_header(header, compressed)

    text = uk._extract_pdf_text(raw)

    assert "Predictor Test" in text


def test_extract_pdf_text_handles_hex_strings():
    raw = b"%PDF-1.4\n<48656c6c6f20576f726c64>\n%%EOF"

    text = uk._extract_pdf_text(raw)

    assert "Hello World" in text


def test_extract_pdf_text_handles_object_streams():
    raw = _make_obj_stream_pdf_bytes(
        "Annual Return AR01",
        "Company Name: Orbit Labs PLC",
    )

    text = uk._extract_pdf_text(raw)

    assert "Orbit Labs PLC" in text
