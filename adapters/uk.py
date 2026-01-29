"""Companies House UK adapter.

Supported filing types:
- annual_return (AR01)
- confirmation_statement (CS01)

Parsed fields:
- company_name (string, label-derived)
- company_number (string, 6-8 alphanumeric)
- filing_date (YYYY-MM-DD)
"""

from __future__ import annotations

import re
import zlib
from datetime import datetime

import httpx

from .base import tracked_call

BASE_URL = "https://api.company-information.service.gov.uk"


async def list_new_filings(company_number: str, since: str):
    """List filing history items since a date (YYYY-MM-DD)."""
    url = f"{BASE_URL}/company/{company_number}/filing-history"
    params = {"category": "annual-return", "since": since}
    async with httpx.AsyncClient() as client:
        async with tracked_call("uk", url) as log:
            r = await client.get(url, params=params)
            log(r)
        r.raise_for_status()
        data = r.json()
    items = data.get("items", [])
    return [
        {
            "transaction_id": i.get("transaction_id"),
            "company_number": company_number,
            "date": i.get("date")[:10],
        }
        for i in items
        if i.get("date") and i.get("date")[:10] > since
    ]


async def download(filing: dict[str, str]):
    """Download the filing document."""
    url = f"{BASE_URL}/filing-history/{filing['transaction_id']}/document?format=pdf"
    async with httpx.AsyncClient() as client:
        async with tracked_call("uk", url) as log:
            r = await client.get(url)
            log(r)
        r.raise_for_status()
        return r.content


async def parse(raw: bytes):
    """Parse a UK Companies House filing PDF into key metadata.

    Returns a list of dicts to match the adapter contract used elsewhere.
    Results include a status field ("ok" or "error") alongside any errors.
    """
    if not raw:
        # Keep adapter output consistent: always return list-of-dicts.
        return [_error_result("empty_pdf")]

    if not _looks_like_pdf(raw):
        # Guard against non-PDF inputs to avoid misleading parsing output.
        # Keep adapter output consistent: always return list-of-dicts.
        return [_error_result("unreadable_pdf")]

    text = _extract_pdf_text(raw)
    if not text:
        # Keep adapter output consistent: always return list-of-dicts.
        return [_error_result("unreadable_pdf")]

    filing_type = _detect_filing_type(text)
    lines = _split_lines(text)
    company_name = _find_labeled_value(
        lines,
        # Companies House forms often use "Company name in full".
        labels=("company name in full", "company name", "name of company"),
    )
    company_number = _find_company_number(lines)
    filing_date = _find_filing_date(lines, text)

    errors: list[str] = []
    if filing_type == "unsupported":
        errors.append("unsupported_filing_type")

    status = "error" if errors else "ok"
    result = {
        "company_name": company_name or None,
        "filing_date": filing_date,
        "filing_type": filing_type,
        "company_number": company_number or None,
        "errors": errors,
        "status": status,
    }
    # Return a list for parity with other adapters and the ETL flow.
    return [result]


def _error_result(reason: str) -> dict[str, str | None | list[str]]:
    return {
        "company_name": None,
        "filing_date": None,
        "filing_type": "error",
        "company_number": None,
        "errors": [reason],
        "status": "error",
    }


def _looks_like_pdf(raw: bytes) -> bool:
    # PDF files should start with a %PDF header near the beginning of the byte stream.
    return b"%PDF" in raw[:1024]


def _extract_pdf_text(raw: bytes) -> str:
    """Extract rough text from PDF bytes by parsing literal and hex strings."""
    chunks: list[str] = []
    chunks.extend(_extract_strings_from_bytes(raw))

    for stream_dict, stream_data in _iter_pdf_streams(raw):
        decoded = _decode_pdf_stream(stream_dict, stream_data)
        if decoded is None:
            continue
        if _is_obj_stream(stream_dict) or _is_flate_stream(stream_dict):
            chunks.extend(_extract_strings_from_bytes(decoded))

    combined = "\n".join(chunk for chunk in chunks if chunk)
    return combined.strip()


def _extract_strings_from_bytes(raw: bytes) -> list[str]:
    try:
        # Use latin-1 to preserve byte values without throwing decode errors.
        decoded = raw.decode("latin-1", errors="ignore")
    except Exception:
        return []
    matches = re.findall(r"\((?:\\.|[^\\)])*\)", decoded)
    chunks = [_unescape_pdf_string(match[1:-1]) for match in matches]
    chunks.extend(_extract_hex_strings(raw))
    return [chunk for chunk in chunks if chunk]


def _extract_hex_strings(raw: bytes) -> list[str]:
    chunks: list[str] = []
    for match in re.finditer(rb"(?<!<)<([0-9A-Fa-f\s]+)>", raw):
        hex_bytes = re.sub(rb"\s+", b"", match.group(1))
        if not hex_bytes:
            continue
        if len(hex_bytes) % 2 == 1:
            hex_bytes += b"0"
        try:
            raw_bytes = bytes.fromhex(hex_bytes.decode("ascii"))
        except (ValueError, UnicodeDecodeError):
            continue
        chunks.append(_decode_pdf_hex_bytes(raw_bytes))
    return [chunk for chunk in chunks if chunk]


def _decode_pdf_hex_bytes(value: bytes) -> str:
    if value.startswith(b"\xfe\xff"):
        try:
            return value[2:].decode("utf-16-be", errors="ignore")
        except UnicodeDecodeError:
            return ""
    if value.startswith(b"\xff\xfe"):
        try:
            return value[2:].decode("utf-16-le", errors="ignore")
        except UnicodeDecodeError:
            return ""
    return value.decode("latin-1", errors="ignore")


def _iter_pdf_streams(raw: bytes) -> list[tuple[bytes, bytes]]:
    streams: list[tuple[bytes, bytes]] = []
    pattern = re.compile(
        rb"<<(?P<dict>.*?)>>\s*stream(?:\r\n|\r|\n)(?P<data>.*?)(?:\r\n|\r|\n)?endstream",
        re.DOTALL,
    )
    for match in pattern.finditer(raw):
        streams.append((match.group("dict"), match.group("data")))
    return streams


def _decode_pdf_stream(stream_dict: bytes, stream_data: bytes) -> bytes | None:
    filters = _parse_filters(stream_dict)
    if filters:
        data = stream_data
        applied = False
        for flt in filters:
            if flt == b"/FlateDecode":
                data = _flate_decode(data, stream_dict)
                if data is None:
                    return None
                applied = True
            else:
                continue
        return data if applied else None
    if _is_obj_stream(stream_dict):
        return stream_data
    return None


def _is_flate_stream(stream_dict: bytes) -> bool:
    return b"/FlateDecode" in stream_dict or any(
        flt == b"/FlateDecode" for flt in _parse_filters(stream_dict)
    )


def _is_obj_stream(stream_dict: bytes) -> bool:
    return b"/ObjStm" in stream_dict


def _parse_filters(stream_dict: bytes) -> list[bytes]:
    list_match = re.search(rb"/Filter\s*\[(?P<filters>.*?)\]", stream_dict, re.DOTALL)
    if list_match:
        return [
            b"/" + name for name in re.findall(rb"/([A-Za-z0-9]+)", list_match.group("filters"))
        ]
    single_match = re.search(rb"/Filter\s*/([A-Za-z0-9]+)", stream_dict)
    if single_match:
        return [b"/" + single_match.group(1)]
    return []


def _flate_decode(data: bytes, stream_dict: bytes) -> bytes | None:
    try:
        decoded = zlib.decompress(data)
    except zlib.error:
        try:
            decoded = zlib.decompress(data, wbits=-zlib.MAX_WBITS)
        except zlib.error:
            return None
    predictor, columns = _parse_decode_params(stream_dict)
    return _apply_predictor(decoded, predictor, columns)


def _parse_decode_params(stream_dict: bytes) -> tuple[int | None, int | None]:
    predictor = None
    columns = None
    predictor_match = re.search(rb"/Predictor\s+(\d+)", stream_dict)
    if predictor_match:
        predictor = int(predictor_match.group(1))
    columns_match = re.search(rb"/Columns\s+(\d+)", stream_dict)
    if columns_match:
        columns = int(columns_match.group(1))
    return predictor, columns


def _apply_predictor(data: bytes, predictor: int | None, columns: int | None) -> bytes:
    if not predictor or predictor == 1:
        return data
    if not columns or columns <= 0:
        return data
    if predictor == 2:
        return _apply_tiff_predictor(data, columns)
    if 10 <= predictor <= 15:
        return _apply_png_predictor(data, columns)
    return data


def _apply_tiff_predictor(data: bytes, columns: int) -> bytes:
    output = bytearray()
    for offset in range(0, len(data), columns):
        row = bytearray(data[offset : offset + columns])
        for idx in range(1, len(row)):
            row[idx] = (row[idx] + row[idx - 1]) % 256
        output.extend(row)
    return bytes(output)


def _apply_png_predictor(data: bytes, columns: int) -> bytes:
    row_length = columns + 1
    output = bytearray()
    prev_row = bytearray(columns)
    for offset in range(0, len(data), row_length):
        row = data[offset : offset + row_length]
        if not row:
            break
        filter_type = row[0]
        raw = bytearray(row[1:])
        recon = bytearray(raw)
        if filter_type == 0:
            pass
        elif filter_type == 1:
            for i in range(len(raw)):
                left = recon[i - 1] if i > 0 else 0
                recon[i] = (raw[i] + left) % 256
        elif filter_type == 2:
            for i in range(len(raw)):
                recon[i] = (raw[i] + prev_row[i]) % 256
        elif filter_type == 3:
            for i in range(len(raw)):
                left = recon[i - 1] if i > 0 else 0
                up = prev_row[i]
                recon[i] = (raw[i] + ((left + up) // 2)) % 256
        elif filter_type == 4:
            for i in range(len(raw)):
                left = recon[i - 1] if i > 0 else 0
                up = prev_row[i]
                up_left = prev_row[i - 1] if i > 0 else 0
                recon[i] = (raw[i] + _paeth_predictor(left, up, up_left)) % 256
        else:
            return data
        output.extend(recon)
        prev_row = recon
    return bytes(output)


def _paeth_predictor(left: int, up: int, up_left: int) -> int:
    p = left + up - up_left
    pa = abs(p - left)
    pb = abs(p - up)
    pc = abs(p - up_left)
    if pa <= pb and pa <= pc:
        return left
    if pb <= pc:
        return up
    return up_left


def _unescape_pdf_string(value: str) -> str:
    result: list[str] = []
    i = 0
    while i < len(value):
        ch = value[i]
        if ch != "\\":
            result.append(ch)
            i += 1
            continue
        i += 1
        if i >= len(value):
            break
        nxt = value[i]
        if nxt in "nrtbf":
            mapping = {
                "n": "\n",
                "r": "\r",
                "t": "\t",
                "b": "\b",
                "f": "\f",
            }
            result.append(mapping.get(nxt, nxt))
            i += 1
            continue
        if nxt in "\\()":
            result.append(nxt)
            i += 1
            continue
        if nxt.isdigit():
            octal = nxt
            i += 1
            for _ in range(2):
                if i < len(value) and value[i].isdigit():
                    octal += value[i]
                    i += 1
                else:
                    break
            try:
                result.append(chr(int(octal, 8)))
            except ValueError:
                pass
            continue
        result.append(nxt)
        i += 1
    return "".join(result)


def _split_lines(text: str) -> list[str]:
    return [segment.strip() for segment in re.split(r"[\r\n]+", text) if segment.strip()]


def _detect_filing_type(text: str) -> str:
    lowered = text.lower()
    if "confirmation statement" in lowered or "cs01" in lowered:
        return "confirmation_statement"
    if "annual return" in lowered or "ar01" in lowered:
        return "annual_return"
    return "unsupported"


def _find_labeled_value(lines: list[str], labels: tuple[str, ...]) -> str:
    for idx, line in enumerate(lines):
        lowered = line.lower()
        for label in labels:
            if label in lowered:
                value = _value_after_label(line, label)
                if value:
                    return value
                for next_line in lines[idx + 1 :]:
                    if next_line.strip():
                        return next_line.strip()
    return ""


def _value_after_label(line: str, label: str) -> str:
    pattern = re.compile(re.escape(label) + r"\s*[:\-]\s*(.+)", re.IGNORECASE)
    match = pattern.search(line)
    if match:
        return match.group(1).strip()
    return ""


def _find_company_number(lines: list[str]) -> str:
    label_value = _find_labeled_value(
        lines,
        labels=("company number", "company no", "company no.", "registration number"),
    )
    if label_value:
        return label_value.strip()
    for line in lines:
        match = re.search(r"\b[A-Z0-9]{6,8}\b", line)
        if match and ("company" in line.lower() or "number" in line.lower()):
            return match.group(0)
    for match in re.finditer(r"\b[A-Z0-9]{6,8}\b", " ".join(lines)):
        return match.group(0)
    return ""


def _find_filing_date(lines: list[str], text: str) -> str | None:
    # Include label variants seen on CS01/AR01 forms.
    labels = (
        "date of filing",
        "filing date",
        "made up to",
        "made up date",
        "statement date",
        "date of registration",
        "confirmation date",
        "date of this return",
    )
    for line in lines:
        lowered = line.lower()
        if any(label in lowered for label in labels):
            parsed = _parse_date_from_line(line)
            if parsed:
                return parsed
    return _parse_date_from_line(text)


def _parse_date_from_line(line: str) -> str | None:
    line = line.strip()
    iso_match = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", line)
    if iso_match:
        year, month, day = iso_match.groups()
        year = _normalize_year(year)
        if year is None:
            return None
        return _format_date(day, month, year)

    slash_match = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b", line)
    if slash_match:
        day, month, year = slash_match.groups()
        year = _normalize_year(year)
        if year is None:
            return None
        return _format_date(day, month, year)

    dmy_match = re.search(r"\b(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})\b", line)
    if dmy_match:
        day, month_name, year = dmy_match.groups()
        year = _normalize_year(year)
        if year is None:
            return None
        return _format_named_date(day, month_name, year)

    mdy_match = re.search(r"\b([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})\b", line)
    if mdy_match:
        month_name, day, year = mdy_match.groups()
        year = _normalize_year(year)
        if year is None:
            return None
        return _format_named_date(day, month_name, year)
    return None


def _normalize_year(year: str) -> str | None:
    if len(year) == 2:
        value = int(year)
        if 0 <= value <= 99:
            return f"20{value:02d}"
        return None
    if len(year) == 4:
        value = int(year)
        if 2000 <= value <= 2099:
            return year
        return None
    return None


def _format_date(day: str, month: str, year: str) -> str | None:
    try:
        parsed = datetime(int(year), int(month), int(day))
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d")


def _format_named_date(day: str, month_name: str, year: str) -> str | None:
    try:
        parsed = datetime.strptime(f"{day} {month_name} {year}", "%d %B %Y")
    except ValueError:
        try:
            parsed = datetime.strptime(f"{day} {month_name} {year}", "%d %b %Y")
        except ValueError:
            return None
    return parsed.strftime("%Y-%m-%d")
