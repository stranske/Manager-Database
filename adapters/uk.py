"""Companies House UK adapter."""

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


async def parse(raw: bytes):
    """Return placeholder parsed result for UK filings."""
    return [{"raw_bytes": len(raw)}]
