"""ASIC Australia adapter.

ASIC's free public surface is a company-register snapshot, not filing PDFs.
This adapter keeps the jurisdiction in the ingest registry while returning
structured unsupported output for document parsing.
"""

from __future__ import annotations

import csv
from io import StringIO

import httpx

from .base import tracked_call

BASE_URL = "https://data.gov.au/data/dataset/asic-companies"


async def list_new_filings(identifier: str, since: str):
    """Return ASIC register metadata records for the configured identifier."""
    url = BASE_URL
    params = {"q": identifier, "since": since}
    async with httpx.AsyncClient() as client:
        async with tracked_call("au", url) as log:
            response = await client.get(url, params=params)
            log(response)
        response.raise_for_status()
    return [
        {
            "id": identifier,
            "date": since,
            "raw": response.text,
        }
    ]


async def download(filing: dict[str, object]):
    """Return the register snapshot text for audit storage."""
    return str(filing.get("raw", "")).encode("utf-8")


async def parse(raw: bytes):
    """Parse CSV-ish register text and mark filing documents unsupported."""
    text = raw.decode("utf-8", errors="ignore")
    rows = list(csv.DictReader(StringIO(text))) if text.strip() else []
    return [
        {
            "status": "unsupported",
            "source": "au",
            "filing_type": "asic_register_snapshot",
            "errors": ["asic_filing_documents_paywalled"],
            "record_count": len(rows),
            "raw_bytes": len(raw),
        }
    ]
