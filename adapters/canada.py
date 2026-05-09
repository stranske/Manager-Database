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
    """Return a structured unsupported status for SEDAR+ filing content.

    The adapter can list and download SEDAR+ documents, but it does not yet
    implement a document parser. Returning an explicit skipped/error payload
    keeps ingestion deterministic without pretending raw bytes are parsed data.
    """
    return [
        {
            "status": "unsupported",
            "source": "ca",
            "filing_type": "sedar_document",
            "errors": ["sedar_parse_not_implemented"],
            "raw_bytes": len(raw),
        }
    ]
