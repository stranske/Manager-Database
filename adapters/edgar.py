"""Minimal EDGAR adapter for Stage1 Proof-of-Concept."""

from __future__ import annotations

import os
from xml.etree import ElementTree as ET

import httpx

from .base import tracked_call

USER_AGENT = os.getenv("EDGAR_UA", "manager-intel/0.1")
BASE_URL = "https://data.sec.gov"


async def list_new_filings(cik: str, since: str) -> list[dict[str, str]]:
    """Return 13F-HR filings for a CIK newer than ``since`` (YYYY-MM-DD)."""
    url = f"{BASE_URL}/submissions/CIK{cik.zfill(10)}.json"
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    async with httpx.AsyncClient() as client:
        async with tracked_call("edgar", url) as log:
            r = await client.get(url, headers=headers)
            log(r)
        if r.status_code == 429:
            raise httpx.HTTPStatusError(
                "Too Many Requests", request=r.request, response=r
            )
        r.raise_for_status()
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
        async with tracked_call("edgar", url) as log:
            r = await client.get(url, headers=headers)
            log(r)
        if r.status_code == 429:
            raise httpx.HTTPStatusError(
                "Too Many Requests", request=r.request, response=r
            )
        r.raise_for_status()
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
