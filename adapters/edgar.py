"""Minimal EDGAR adapter for Stage1 Proof-of-Concept."""

from __future__ import annotations

import os
import logging
import asyncio
from xml.etree import ElementTree as ET

import httpx

from .base import tracked_call

USER_AGENT = os.getenv("EDGAR_UA", "manager-intel/0.1")
BASE_URL = "https://data.sec.gov"
logger = logging.getLogger(__name__)


async def _request_with_retry(
    client: httpx.AsyncClient,
    url: str,
    headers: dict[str, str],
    *,
    source: str,
    max_retries: int = 3,
) -> httpx.Response:
    for attempt in range(1, max_retries + 1):
        try:
            async with tracked_call(source, url) as log:
                response = await client.get(url, headers=headers)
                log(response)
            response.raise_for_status()
            return response
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            if attempt >= max_retries:
                logger.error(
                    "EDGAR request failed after retries",
                    extra={"url": url, "attempts": attempt},
                    exc_info=exc,
                )
                raise
            wait = 0.5 * attempt
            logger.warning(
                "EDGAR request failed; retrying",
                extra={"url": url, "attempt": attempt, "max_retries": max_retries},
            )
            await asyncio.sleep(wait)


async def list_new_filings(cik: str, since: str) -> list[dict[str, str]]:
    """Return 13F-HR filings for a CIK newer than ``since`` (YYYY-MM-DD)."""
    url = f"{BASE_URL}/submissions/CIK{cik.zfill(10)}.json"
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    async with httpx.AsyncClient() as client:
        r = await _request_with_retry(client, url, headers, source="edgar")
        data = r.json()
    filings = []
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    for form, filed, acc in zip(forms, dates, accessions, strict=False):
        if form == "13F-HR" and filed > since:
            filings.append({"accession": acc, "cik": cik, "filed": filed})
    if not filings:
        raise UserWarning("No 13F-HR filings found")
    return filings


async def download(filing: dict[str, str]) -> str:
    """Download a filing's primary document."""
    accession = filing["accession"].replace("-", "")
    cik = filing["cik"].zfill(10)
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/primary_doc.xml"
    headers = {"User-Agent": USER_AGENT}
    async with httpx.AsyncClient() as client:
        r = await _request_with_retry(client, url, headers, source="edgar")
        return r.text


async def parse(raw: str) -> list[dict[str, int | str]]:
    """Parse an XML 13F document into row dicts."""
    root = ET.fromstring(raw)
    rows: list[dict[str, int | str]] = []
    for info in root.findall(".//infoTable"):
        rows.append(
            {
                "nameOfIssuer": (info.findtext("nameOfIssuer") or ""),
                "cusip": (info.findtext("cusip") or ""),
                "value": int(info.findtext("value") or 0),
                "sshPrnamt": int(info.findtext("shrsOrPrnAmt/sshPrnamt") or 0),
            }
        )
    return rows
