"""SEDAR+ Canada adapter."""

from __future__ import annotations

import httpx

from .base import tracked_call

BASE_URL = "https://www.sedarplus.com/api"


async def list_new_filings(cik: str, since: str):
    """List Canadian filings for an issuer."""
    url = f"{BASE_URL}/filings"
    params = {"cik": cik, "since": since}
    async with httpx.AsyncClient() as client:
        async with tracked_call("ca", url) as log:
            r = await client.get(url, params=params)
            log(r)
        r.raise_for_status()
        return r.json().get("items", [])


async def download(filing: dict[str, str]):
    """Download a filing PDF."""
    url = f"{BASE_URL}/filings/{filing['id']}/document"
    async with httpx.AsyncClient() as client:
        async with tracked_call("ca", url) as log:
            r = await client.get(url)
            log(r)
        r.raise_for_status()
        return r.content


async def parse(raw: bytes):
    """Return placeholder parsed output for Canadian filings."""
    return [{"raw_bytes": len(raw)}]
