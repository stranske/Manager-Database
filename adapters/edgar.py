"""EDGAR adapter for 13F and activism filing ingestion."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from xml.etree import ElementTree as ET

import httpx

from .base import tracked_call

USER_AGENT = os.getenv("EDGAR_UA", "manager-intel/0.1")
BASE_URL = "https://data.sec.gov"
EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
DEFAULT_FORM_TYPES = ["13F-HR"]
THIRTEEN_F_FORMS = {"13F-HR", "13F-HR/A", "13-F"}
ACTIVISM_FORMS = {"SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"}
logger = logging.getLogger(__name__)


async def _request_with_retry(
    client: httpx.AsyncClient,
    url: str,
    headers: dict[str, str],
    *,
    source: str,
    max_retries: int = 3,
    params: dict[str, str] | None = None,
) -> httpx.Response:
    for attempt in range(1, max_retries + 1):
        try:
            async with tracked_call(source, url) as log:
                request_kwargs = {"headers": headers}
                if params is not None:
                    request_kwargs["params"] = params
                response = await client.get(url, **request_kwargs)
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
    raise RuntimeError("EDGAR request retry loop ended unexpectedly")


def _normalize_form_type(form_type: str) -> str:
    normalized = re.sub(r"\s+", " ", form_type.strip().upper())
    if normalized == "13-F":
        return "13F-HR"
    return normalized


def _is_13f_form(form_type: str) -> bool:
    return _normalize_form_type(form_type) in THIRTEEN_F_FORMS


def _is_activism_form(form_type: str) -> bool:
    return _normalize_form_type(form_type) in ACTIVISM_FORMS


def _normalize_filed_date(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    value = raw_value.strip()
    if len(value) >= 10 and value[4] == "-" and value[7] == "-":
        return value[:10]
    match = re.search(r"(\d{8})", value)
    if match:
        digits = match.group(1)
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:]}"
    return None


def _extract_accession(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    cleaned = raw_value.strip()
    match = re.search(r"(\d{10}-\d{2}-\d{6})", cleaned)
    if match:
        return match.group(1)
    digits = re.sub(r"\D", "", cleaned)
    if len(digits) == 18:
        return f"{digits[:10]}-{digits[10:12]}-{digits[12:]}"
    return cleaned or None


def _extract_efts_hits(payload: dict[str, object]) -> list[dict[str, object]]:
    hits = payload.get("hits")
    if isinstance(hits, dict):
        nested_hits = hits.get("hits")
        if isinstance(nested_hits, list):
            return [item for item in nested_hits if isinstance(item, dict)]
    if isinstance(hits, list):
        return [item for item in hits if isinstance(item, dict)]
    filings = payload.get("filings")
    if isinstance(filings, list):
        return [item for item in filings if isinstance(item, dict)]
    return []


def _efts_hit_to_filing(hit: dict[str, object], cik: str) -> dict[str, str] | None:
    source = hit.get("_source") if isinstance(hit.get("_source"), dict) else hit
    form = str(source.get("formType") or source.get("form") or "").strip()
    filed = _normalize_filed_date(
        str(source.get("filedAt") or source.get("filed") or source.get("filed_date") or "")
    )
    accession = _extract_accession(
        str(
            source.get("adsh")
            or source.get("accessionNo")
            or source.get("accessionNumber")
            or source.get("accession")
            or ""
        )
    )
    url = str(
        source.get("primaryDocUrl")
        or source.get("linkToHtml")
        or source.get("linkToTxt")
        or source.get("linkToFilingDetails")
        or source.get("url")
        or ""
    ).strip()
    if not form or not filed:
        return None
    filing: dict[str, str] = {
        "accession": accession or "",
        "cik": str(source.get("cik") or cik),
        "filed": filed,
        "form": form,
    }
    if url:
        filing["url"] = url
    return filing


async def _search_efts_filings(
    client: httpx.AsyncClient,
    *,
    cik: str,
    since: str,
    headers: dict[str, str],
    manager_name: str | None,
    form_types: list[str],
) -> list[dict[str, str]]:
    response = await _request_with_retry(
        client,
        EFTS_URL,
        headers,
        source="edgar",
        params={
            "q": "",
            "dateRange": "custom",
            "startdt": since,
            "forms": ",".join(form_types),
            "entityName": manager_name or cik,
        },
    )
    filings: list[dict[str, str]] = []
    for hit in _extract_efts_hits(response.json()):
        filing = _efts_hit_to_filing(hit, cik)
        if filing is not None:
            filings.append(filing)
    return filings


def _unique_filings(filings: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str, str]] = set()
    unique: list[dict[str, str]] = []
    for filing in filings:
        key = (
            str(filing.get("accession") or ""),
            str(filing.get("url") or ""),
            _normalize_form_type(str(filing.get("form") or "")),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(filing)
    return unique


async def list_new_filings(
    cik: str,
    since: str,
    form_types: list[str] | None = None,
    manager_name: str | None = None,
) -> list[dict[str, str]]:
    """Return EDGAR filings newer than ``since`` (YYYY-MM-DD)."""
    requested_forms = [_normalize_form_type(form) for form in (form_types or DEFAULT_FORM_TYPES)]
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    filings: list[dict[str, str]] = []

    async with httpx.AsyncClient() as client:
        if any(_is_13f_form(form) for form in requested_forms):
            url = f"{BASE_URL}/submissions/CIK{cik.zfill(10)}.json"
            response = await _request_with_retry(client, url, headers, source="edgar")
            recent = response.json().get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            dates = recent.get("filingDate", [])
            accessions = recent.get("accessionNumber", [])
            for form, filed, accession in zip(forms, dates, accessions, strict=False):
                normalized = _normalize_form_type(str(form))
                if normalized in requested_forms and str(filed) > since:
                    filings.append(
                        {
                            "accession": str(accession),
                            "cik": cik,
                            "filed": str(filed),
                        }
                    )

        activism_forms = [form for form in requested_forms if _is_activism_form(form)]
        if activism_forms:
            filings.extend(
                await _search_efts_filings(
                    client,
                    cik=cik,
                    since=since,
                    headers=headers,
                    manager_name=manager_name,
                    form_types=activism_forms,
                )
            )

    filings = _unique_filings(filings)
    if not filings:
        raise UserWarning("No matching EDGAR filings found")
    return filings


async def download(filing: dict[str, str]) -> str:
    """Download a filing's primary document."""
    url = filing.get("url")
    if url:
        if url.startswith("/"):
            url = f"https://www.sec.gov{url}"
    else:
        accession = filing["accession"].replace("-", "")
        cik = filing["cik"].zfill(10)
        url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/primary_doc.xml"

    headers = {"User-Agent": USER_AGENT}
    async with httpx.AsyncClient() as client:
        response = await _request_with_retry(client, url, headers, source="edgar")
        return response.text


def _extract_label_value(raw_text: str, label: str) -> str | None:
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    normalized_label = re.sub(r"[^a-z0-9]+", "", label.lower())
    for idx, line in enumerate(lines):
        normalized_line = re.sub(r"[^a-z0-9]+", "", line.lower())
        if normalized_line == normalized_label and idx + 1 < len(lines):
            return lines[idx + 1].strip(" :")
        if normalized_line.startswith(normalized_label):
            suffix = re.sub(rf"^{re.escape(label)}\s*:?", "", line, flags=re.IGNORECASE).strip()
            if suffix:
                return suffix
    inline = re.search(rf"{re.escape(label)}\s*:?\s*([^\n\r]+)", raw_text, flags=re.IGNORECASE)
    if inline:
        return inline.group(1).strip()
    return None


def _extract_item_section(raw_text: str, item_number: int) -> str:
    pattern = re.compile(
        rf"ITEM\s+{item_number}\.?\s*(.*?)(?=ITEM\s+{item_number + 1}\.?|SIGNATURE|$)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(raw_text)
    return match.group(1).strip() if match else ""


def _extract_first_match(raw_text: str, patterns: list[str]) -> str | None:
    for pattern in patterns:
        match = re.search(pattern, raw_text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
    return None


def _parse_int(value: str | None) -> int | None:
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits else None


def _parse_float(value: str | None) -> float | None:
    if not value:
        return None
    cleaned = value.replace("%", "").replace(",", " ").strip()
    match = re.search(r"\d+(?:\.\d+)?", cleaned)
    return float(match.group(0)) if match else None


def _extract_group_members(item_two_text: str) -> list[str]:
    match = re.search(
        r"(?:Reporting Persons?|Group Members?)\s*:?\s*(.+)",
        item_two_text,
        flags=re.IGNORECASE,
    )
    if not match:
        return []
    raw_members = re.split(r"[;,]|\band\b", match.group(1))
    return [member.strip() for member in raw_members if member.strip()]


def parse_13d(raw_text: str) -> dict[str, object]:
    """Parse a Schedule 13D filing using line-based heuristics."""
    item_two = _extract_item_section(raw_text, 2)
    item_four = _extract_item_section(raw_text, 4)
    filed_date = _normalize_filed_date(
        _extract_first_match(
            raw_text,
            [
                r"FILED AS OF DATE\s*:?\s*(\d{8})",
                r"Date of Event Which Requires Filing of this Statement\s*:?\s*([0-9/\-]{8,10})",
            ],
        )
    )
    return {
        "subject_company": _extract_label_value(raw_text, "Name of Issuer") or "",
        "cusip": re.sub(r"\s+", "", _extract_label_value(raw_text, "CUSIP Number") or "")[:9],
        "ownership_pct": _parse_float(
            _extract_first_match(
                raw_text,
                [
                    r"Percent of Class(?: Represented by Amount in Row \(\d+\))?\s*:?\s*([0-9][0-9., ]*%?)",
                    r"Percent of Class\s*:?\s*([0-9][0-9., ]*%?)",
                ],
            )
        ),
        "shares": _parse_int(
            _extract_first_match(
                raw_text,
                [
                    r"Amount Beneficially Owned(?: by Each Reporting Person)?(?: in Row \(\d+\))?\s*:?\s*([\d,]+)",
                    r"Amount Beneficially Owned\s*:?\s*([\d,]+)",
                ],
            )
        ),
        "group_members": _extract_group_members(item_two),
        "purpose_snippet": re.sub(r"\s+", " ", item_four).strip()[:500],
        "filed_date": filed_date,
    }


def parse_13g(raw_text: str) -> dict[str, object]:
    """Parse a Schedule 13G filing using line-based heuristics."""
    filed_date = _normalize_filed_date(
        _extract_first_match(
            raw_text,
            [
                r"FILED AS OF DATE\s*:?\s*(\d{8})",
                r"Date of Event Which Requires Filing of this Statement\s*:?\s*([0-9/\-]{8,10})",
            ],
        )
    )
    return {
        "subject_company": _extract_label_value(raw_text, "Name of Issuer") or "",
        "cusip": re.sub(r"\s+", "", _extract_label_value(raw_text, "CUSIP Number") or "")[:9],
        "ownership_pct": _parse_float(
            _extract_first_match(
                raw_text,
                [
                    r"Percent of Class(?: Represented by Amount in Row \(\d+\))?\s*:?\s*([0-9][0-9., ]*%?)",
                    r"Percent of Class\s*:?\s*([0-9][0-9., ]*%?)",
                ],
            )
        ),
        "shares": _parse_int(
            _extract_first_match(
                raw_text,
                [
                    r"Amount Beneficially Owned(?: by Each Reporting Person)?(?: in Row \(\d+\))?\s*:?\s*([\d,]+)",
                    r"Amount Beneficially Owned\s*:?\s*([\d,]+)",
                ],
            )
        ),
        "filed_date": filed_date,
    }


async def parse(
    raw: str,
    form_type: str | None = None,
) -> list[dict[str, int | str]] | dict[str, object]:
    """Parse a filing payload into holdings rows or activism metadata."""
    if form_type and _is_activism_form(form_type):
        if "13D" in _normalize_form_type(form_type):
            return parse_13d(raw)
        return parse_13g(raw)

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
