"""Companies House UK adapter.

Supported filing types:
- Annual returns
- Confirmation statements

"""

from __future__ import annotations

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
    # TODO: Implement actual PDF parsing
    return {"raw_bytes": len(raw), "parsed": False}
