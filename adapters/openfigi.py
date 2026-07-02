"""OpenFIGI-backed CUSIP identifier resolution."""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from adapters.base import get_placeholder, is_sqlite

logger = logging.getLogger(__name__)

OPENFIGI_MAPPING_URL = "https://api.openfigi.com/v3/mapping"
MAX_OPENFIGI_JOBS = 100
CACHE_LOOKUP_CHUNK_SIZE = 500
DEFAULT_KEYLESS_DELAY_SECONDS = 2.5
DEFAULT_KEYED_DELAY_SECONDS = 0.25


@dataclass(frozen=True)
class IdentifierResolution:
    cusip: str
    ticker: str | None = None
    figi: str | None = None
    composite_figi: str | None = None
    share_class_figi: str | None = None
    isin: str | None = None
    lei: str | None = None
    name: str | None = None
    source: str = "openfigi"


class OpenFigiClient:
    """Small OpenFIGI mapping client with conservative batching and pacing."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        enable_keyless: bool | None = None,
        url: str = OPENFIGI_MAPPING_URL,
        opener: Any | None = None,
        sleep: Any | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else os.getenv("OPENFIGI_API_KEY")
        if enable_keyless is None:
            enable_keyless = os.getenv("OPENFIGI_ENABLE_KEYLESS", "").lower() in {
                "1",
                "true",
                "yes",
            }
        self.enable_keyless = enable_keyless
        self.url = url
        self._opener = opener or urllib.request.urlopen
        self._sleep = sleep or time.sleep
        self._last_request_at = 0.0

    @property
    def enabled(self) -> bool:
        return bool(self.api_key or self.enable_keyless)

    def map_cusips(self, cusips: list[str]) -> dict[str, IdentifierResolution]:
        normalized = [_normalize_cusip(cusip) for cusip in cusips]
        pending = [cusip for cusip in dict.fromkeys(normalized) if cusip]
        if not pending or not self.enabled:
            return {}

        resolved: dict[str, IdentifierResolution] = {}
        for start in range(0, len(pending), MAX_OPENFIGI_JOBS):
            batch = pending[start : start + MAX_OPENFIGI_JOBS]
            resolved.update(self._map_batch(batch))
        return resolved

    def _map_batch(self, cusips: list[str]) -> dict[str, IdentifierResolution]:
        self._pace()
        payload = json.dumps(
            [{"idType": "ID_CUSIP", "idValue": cusip} for cusip in cusips]
        ).encode()
        request = urllib.request.Request(
            self.url,
            data=payload,
            headers=self._headers(),
            method="POST",
        )
        try:
            with self._opener(request, timeout=20) as response:
                body = response.read()
        except (OSError, urllib.error.URLError, urllib.error.HTTPError):
            logger.warning("OpenFIGI mapping request failed", exc_info=True)
            return {}
        try:
            decoded = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            logger.warning("OpenFIGI mapping response was not valid JSON", exc_info=True)
            return {}
        if not isinstance(decoded, list):
            return {}
        return _parse_mapping_response(cusips, decoded)

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-OPENFIGI-APIKEY"] = self.api_key
        return headers

    def _pace(self) -> None:
        delay = DEFAULT_KEYED_DELAY_SECONDS if self.api_key else DEFAULT_KEYLESS_DELAY_SECONDS
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < delay:
            self._sleep(delay - elapsed)
        self._last_request_at = time.monotonic()


def _normalize_cusip(value: Any) -> str:
    text = str(value or "").strip().upper()
    normalized = "".join(ch for ch in text if ch.isalnum())
    return normalized if len(normalized) == 9 else ""


def _parse_mapping_response(
    cusips: list[str], payload: list[Any]
) -> dict[str, IdentifierResolution]:
    resolved: dict[str, IdentifierResolution] = {}
    for cusip, item in zip(cusips, payload, strict=False):
        if not isinstance(item, dict):
            continue
        rows = item.get("data")
        if not isinstance(rows, list) or not rows:
            continue
        row = rows[0]
        if not isinstance(row, dict):
            continue
        resolved[cusip] = IdentifierResolution(
            cusip=cusip,
            ticker=_optional_text(row.get("ticker")),
            figi=_optional_text(row.get("figi")),
            composite_figi=_optional_text(row.get("compositeFIGI")),
            share_class_figi=_optional_text(row.get("shareClassFIGI")),
            isin=(
                _optional_text(row.get("securityID"))
                if row.get("securityIDType") == "ISIN"
                else None
            ),
            lei=_optional_text(row.get("lei")),
            name=_optional_text(row.get("name")),
        )
    return resolved


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def ensure_identifier_resolution_schema(conn: Any) -> None:
    if is_sqlite(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS identifier_resolution_cache (
                cusip TEXT PRIMARY KEY,
                ticker TEXT,
                figi TEXT,
                composite_figi TEXT,
                share_class_figi TEXT,
                isin TEXT,
                lei TEXT,
                name TEXT,
                source TEXT NOT NULL DEFAULT 'openfigi',
                resolved_at TEXT DEFAULT CURRENT_TIMESTAMP
            )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS identifier_resolution_metrics (
                metric_id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                filing_id INTEGER,
                total_cusips INTEGER NOT NULL,
                unmapped_cusips INTEGER NOT NULL,
                unmapped_cusip_rate REAL NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )""")
        for column in (
            "resolved_ticker TEXT",
            "resolved_figi TEXT",
            "resolved_lei TEXT",
            "resolution_source TEXT",
        ):
            _sqlite_add_column_if_missing(conn, "holdings", column)
        return

    conn.execute("""CREATE TABLE IF NOT EXISTS identifier_resolution_cache (
            cusip text PRIMARY KEY,
            ticker text,
            figi text,
            composite_figi text,
            share_class_figi text,
            isin text,
            lei text,
            name text,
            source text NOT NULL DEFAULT 'openfigi',
            resolved_at timestamptz DEFAULT now()
        )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS identifier_resolution_metrics (
            metric_id bigserial PRIMARY KEY,
            source text NOT NULL,
            filing_id bigint,
            total_cusips integer NOT NULL,
            unmapped_cusips integer NOT NULL,
            unmapped_cusip_rate real NOT NULL,
            created_at timestamptz DEFAULT now()
        )""")
    conn.execute("ALTER TABLE holdings ADD COLUMN IF NOT EXISTS resolved_ticker text")
    conn.execute("ALTER TABLE holdings ADD COLUMN IF NOT EXISTS resolved_figi text")
    conn.execute("ALTER TABLE holdings ADD COLUMN IF NOT EXISTS resolved_lei text")
    conn.execute("ALTER TABLE holdings ADD COLUMN IF NOT EXISTS resolution_source text")


def _sqlite_add_column_if_missing(conn: Any, table: str, column_sql: str) -> None:
    column_name = column_sql.split()[0]
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if column_name not in {row[1] for row in rows}:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_sql}")


def resolve_holding_identifiers(
    conn: Any,
    holdings: list[dict[str, Any]],
    *,
    filing_id: int | None = None,
    source: str = "edgar",
    client: OpenFigiClient | None = None,
) -> float:
    """Annotate holding rows with cached/OpenFIGI identifiers and record unmapped rate."""
    ensure_identifier_resolution_schema(conn)
    cusips = [_normalize_cusip(row.get("cusip")) for row in holdings]
    unique_cusips = [cusip for cusip in dict.fromkeys(cusips) if cusip]
    cached = _load_cached(conn, unique_cusips)
    missing = [cusip for cusip in unique_cusips if cusip not in cached]
    live = (client or OpenFigiClient()).map_cusips(missing)
    if live:
        _upsert_cache(conn, live.values())
        cached.update(live)

    unmapped_cusips = {cusip for cusip in unique_cusips if cusip not in cached}
    for row, cusip in zip(holdings, cusips, strict=False):
        if not cusip:
            continue
        resolution = cached.get(cusip)
        if resolution is None:
            continue
        row["resolved_ticker"] = resolution.ticker
        row["resolved_figi"] = (
            resolution.figi or resolution.composite_figi or resolution.share_class_figi
        )
        row["isin"] = row.get("isin") or resolution.isin
        row["resolved_lei"] = resolution.lei
        row["resolution_source"] = resolution.source

    total = len(unique_cusips)
    unmapped = len(unmapped_cusips)
    rate = (unmapped / total) if total else 0.0
    _record_unmapped_rate(
        conn, source=source, filing_id=filing_id, total=total, unmapped=unmapped, rate=rate
    )
    return rate


def _load_cached(conn: Any, cusips: list[str]) -> dict[str, IdentifierResolution]:
    if not cusips:
        return {}
    marker = get_placeholder(conn)
    rows = []
    for start in range(0, len(cusips), CACHE_LOOKUP_CHUNK_SIZE):
        batch = cusips[start : start + CACHE_LOOKUP_CHUNK_SIZE]
        rows.extend(
            conn.execute(
                "SELECT cusip, ticker, figi, composite_figi, share_class_figi, isin, lei, name, source "
                "FROM identifier_resolution_cache "
                f"WHERE cusip IN ({', '.join(marker for _ in batch)})",
                tuple(batch),
            ).fetchall()
        )
    return {
        str(row[0]): IdentifierResolution(
            cusip=str(row[0]),
            ticker=row[1],
            figi=row[2],
            composite_figi=row[3],
            share_class_figi=row[4],
            isin=row[5],
            lei=row[6],
            name=row[7],
            source=row[8] or "openfigi",
        )
        for row in rows
    }


def _upsert_cache(conn: Any, resolutions: Any) -> None:
    marker = get_placeholder(conn)
    for resolution in resolutions:
        values = (
            resolution.cusip,
            resolution.ticker,
            resolution.figi,
            resolution.composite_figi,
            resolution.share_class_figi,
            resolution.isin,
            resolution.lei,
            resolution.name,
            resolution.source,
        )
        if is_sqlite(conn):
            conn.execute(
                "INSERT INTO identifier_resolution_cache("
                "cusip, ticker, figi, composite_figi, share_class_figi, isin, lei, name, source"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(cusip) DO UPDATE SET "
                "ticker=excluded.ticker, figi=excluded.figi, "
                "composite_figi=excluded.composite_figi, "
                "share_class_figi=excluded.share_class_figi, isin=excluded.isin, "
                "lei=excluded.lei, name=excluded.name, source=excluded.source, "
                "resolved_at=CURRENT_TIMESTAMP",
                values,
            )
            continue
        conn.execute(
            "INSERT INTO identifier_resolution_cache("
            "cusip, ticker, figi, composite_figi, share_class_figi, isin, lei, name, source"
            f") VALUES ({', '.join(marker for _ in values)}) "
            "ON CONFLICT(cusip) DO UPDATE SET "
            "ticker=EXCLUDED.ticker, figi=EXCLUDED.figi, "
            "composite_figi=EXCLUDED.composite_figi, "
            "share_class_figi=EXCLUDED.share_class_figi, isin=EXCLUDED.isin, "
            "lei=EXCLUDED.lei, name=EXCLUDED.name, source=EXCLUDED.source, "
            "resolved_at=now()",
            values,
        )


def _record_unmapped_rate(
    conn: Any,
    *,
    source: str,
    filing_id: int | None,
    total: int,
    unmapped: int,
    rate: float,
) -> None:
    marker = get_placeholder(conn)
    values = (source, filing_id, total, unmapped, rate)
    conn.execute(
        "INSERT INTO identifier_resolution_metrics("
        "source, filing_id, total_cusips, unmapped_cusips, unmapped_cusip_rate"
        f") VALUES ({', '.join(marker for _ in values)})",
        values,
    )
