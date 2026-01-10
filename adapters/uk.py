"""Companies House UK adapter.

Supported filing types:
- Confirmation statements
- Annual returns
- Accounts
- Incorporation documents

The PDF filings are text-based and typically embed labeled fields (e.g.
"Company name", "Filing date", "Date of filing", "Document type") inside
content streams as literal strings.
"""

from __future__ import annotations

import re
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
    url = f"{BASE_URL}/filing-history/{filing['transaction_id']}/document" "?format=pdf"
    async with httpx.AsyncClient() as client:
        async with tracked_call("uk", url) as log:
            r = await client.get(url)
            log(r)
        r.raise_for_status()
        return r.content


async def parse(raw: bytes) -> dict:
    """Parse UK Companies House filing PDF.

    Args:
        raw: Raw PDF bytes from download()

    Returns:
        Dict with company_name, filing_date, filing_type keys.
    """

    def extract_pdf_strings(payload: bytes) -> list[str]:
        # Basic literal-string extraction for text-based PDFs without dependencies.
        strings: list[str] = []
        current: list[str] = []
        in_string = False
        escaped = False
        for ch in payload.decode("latin-1", errors="ignore"):
            if not in_string:
                if ch == "(":
                    in_string = True
                    current = []
                continue
            if escaped:
                current.append(ch)
                escaped = False
                continue
            if ch == "\\":
                escaped = True
                continue
            if ch == ")":
                strings.append("".join(current))
                in_string = False
                continue
            current.append(ch)
        return strings

    def normalize_text(text: str) -> str:
        # Collapse whitespace so regex patterns behave predictably.
        return re.sub(r"\s+", " ", text).strip()

    def parse_date(text: str) -> str | None:
        iso_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", text)
        if iso_match:
            return iso_match.group(1)
        named_match = re.search(
            r"\b(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\b",
            text,
        )
        if not named_match:
            return None
        day, month_name, year = named_match.groups()
        try:
            parsed = datetime.strptime(f"{day} {month_name} {year}", "%d %B %Y")
        except ValueError:
            return None
        return parsed.strftime("%Y-%m-%d")

    base = {
        "company_name": None,
        "filing_date": None,
        "filing_type": None,
    }

    try:
        if not raw or not raw.startswith(b"%PDF"):
            return {**base, "error": "unsupported or empty PDF content"}

        strings = extract_pdf_strings(raw)
        if not strings:
            return {**base, "error": "no extractable text found in PDF"}

        text = normalize_text(" ".join(strings))
        company_name = None
        filing_date = None
        filing_type = None

        for chunk in strings:
            normalized = normalize_text(chunk)
            name_match = re.search(
                r"company name[:\s]+(.+)",
                normalized,
                re.IGNORECASE,
            )
            if name_match and not company_name:
                company_name = normalize_text(name_match.group(1))
            # Companies House PDFs use multiple labels for filing dates/types.
            date_match = re.search(
                r"(filing date|date of filing)[:\s]+(.+)",
                normalized,
                re.IGNORECASE,
            )
            if date_match and not filing_date:
                filing_date = parse_date(date_match.group(2))
            type_match = re.search(
                r"(filing type|document type|type of document)[:\s]+(.+)",
                normalized,
                re.IGNORECASE,
            )
            if type_match and not filing_type:
                filing_type = normalize_text(type_match.group(2))

        if not filing_type:
            # Fall back to known filing phrases if labels are absent.
            filing_type_candidates = [
                "confirmation statement",
                "annual return",
                "accounts",
                "incorporation",
            ]
            for candidate in filing_type_candidates:
                if candidate in text.lower():
                    filing_type = candidate
                    break

        if not filing_date:
            filing_date = parse_date(text)

        return {
            "company_name": company_name,
            "filing_date": filing_date,
            "filing_type": filing_type,
            "raw_bytes": len(raw),
        }
    except Exception as exc:  # pragma: no cover - defensive parsing
        return {**base, "error": f"failed to parse PDF: {exc}"}
