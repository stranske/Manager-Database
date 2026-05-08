"""Monetary Authority of Singapore adapter.

The MAS public data surface is useful for regulator datasets, but this project
does not yet have a filing-document endpoint equivalent to EDGAR/Companies
House. The adapter is registered so jurisdiction selection is explicit and
returns deterministic unsupported statuses instead of silent success.
"""

from __future__ import annotations

import httpx

from .base import tracked_call

BASE_URL = "https://eservices.mas.gov.sg/api/action/datastore/search.json"


async def list_new_filings(identifier: str, since: str):
    """List MAS records for a manager identifier.

    MAS does not expose manager filing PDFs through this endpoint, so the
    result is normalized into metadata records that parse as unsupported.
    """
    url = BASE_URL
    params = {"q": identifier, "since": since}
    async with httpx.AsyncClient() as client:
        async with tracked_call("sg", url) as log:
            response = await client.get(url, params=params)
            log(response)
        response.raise_for_status()
        payload = response.json()
    records = payload.get("result", {}).get("records", [])
    return [
        {
            "id": str(record.get("_id") or record.get("id") or identifier),
            "date": record.get("date") or record.get("updated_at"),
            "raw": record,
        }
        for record in records
        if isinstance(record, dict)
    ]


async def download(filing: dict[str, object]):
    """Return the MAS metadata record as bytes for audit storage."""
    return repr(filing.get("raw", filing)).encode("utf-8")


async def parse(raw: bytes):
    """Return an explicit unsupported result for MAS metadata records."""
    return [
        {
            "status": "unsupported",
            "source": "sg",
            "filing_type": "mas_metadata",
            "errors": ["mas_filing_document_endpoint_not_configured"],
            "raw_bytes": len(raw),
        }
    ]
