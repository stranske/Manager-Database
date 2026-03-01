"""Prefect flow orchestrating filing ingestion across jurisdictions."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, Awaitable, Callable

import boto3
from prefect import flow, task

from adapters.base import connect_db, get_adapter
from embeddings import store_document
from etl.logging_setup import configure_logging, log_outcome

RAW_DIR = Path(os.getenv("RAW_DIR", "./data/raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)

S3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
    region_name="us-east-1",
)
BUCKET = os.getenv("MINIO_BUCKET", "filings")
DB_PATH = os.getenv("DB_PATH", "dev.db")

_ADAPTER_MAP = {"us": "edgar", "uk": "uk", "ca": "canada"}
_IDENTIFIER_ENV = {
    "us": "CIK_LIST",
    "uk": "UK_COMPANY_NUMBERS",
    "ca": "CA_CIK_LIST",
}
_IDENTIFIER_DEFAULT = {
    "us": "0001791786,0001434997",
    "uk": "",
    "ca": "",
}

configure_logging("ingest_flow")
logger = logging.getLogger(__name__)


def _is_sqlite(conn: Any) -> bool:
    return isinstance(conn, sqlite3.Connection)


def _placeholder(conn: Any) -> str:
    return "?" if _is_sqlite(conn) else "%s"


def _ensure_filing_tables(conn: Any) -> None:
    if _is_sqlite(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS filings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                manager_id INTEGER,
                source TEXT NOT NULL,
                external_id TEXT NOT NULL,
                filed_date TEXT,
                type TEXT,
                parsed_payload TEXT
            )""")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS filings_source_external_idx "
            "ON filings(source, external_id)"
        )
        conn.execute("""CREATE TABLE IF NOT EXISTS holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filing_id INTEGER,
                manager_id INTEGER,
                cik TEXT,
                accession TEXT,
                filed DATE,
                nameOfIssuer TEXT,
                cusip TEXT,
                value INTEGER,
                sshPrnamt INTEGER
            )""")
        return
    conn.execute("""CREATE TABLE IF NOT EXISTS filings (
            id bigserial PRIMARY KEY,
            manager_id bigint,
            source text NOT NULL,
            external_id text NOT NULL,
            filed_date date,
            type text,
            parsed_payload jsonb
        )""")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS filings_source_external_idx "
        "ON filings(source, external_id)"
    )
    conn.execute("""CREATE TABLE IF NOT EXISTS holdings (
            id bigserial PRIMARY KEY,
            filing_id bigint,
            manager_id bigint,
            cik text,
            accession text,
            filed date,
            \"nameOfIssuer\" text,
            cusip text,
            value bigint,
            \"sshPrnamt\" bigint
        )""")


def _lookup_manager_id(conn: Any, jurisdiction: str, identifier: str) -> int | None:
    marker = _placeholder(conn)
    try:
        if jurisdiction in {"us", "ca"}:
            row = conn.execute(
                f"SELECT id FROM managers WHERE cik = {marker} LIMIT 1",
                (identifier,),
            ).fetchone()
        elif jurisdiction == "uk":
            if _is_sqlite(conn):
                row = conn.execute(
                    "SELECT id FROM managers "
                    "WHERE json_extract(registry_ids, '$.uk_company_number') = ? LIMIT 1",
                    (identifier,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT id FROM managers "
                    "WHERE registry_ids->>'uk_company_number' = %s LIMIT 1",
                    (identifier,),
                ).fetchone()
        else:
            row = None
    except Exception:
        logger.warning(
            "Failed to look up manager",
            extra={"jurisdiction": jurisdiction, "identifier": identifier},
            exc_info=True,
        )
        return None
    if not row or row[0] is None:
        return None
    return int(row[0])


def _filing_external_id(filing: dict[str, Any], jurisdiction: str) -> str:
    if jurisdiction == "uk":
        return str(filing.get("transaction_id", ""))
    return str(filing.get("accession", ""))


def _filing_date(filing: dict[str, Any], jurisdiction: str) -> str | None:
    if jurisdiction == "uk":
        return filing.get("date")
    return filing.get("filed")


def _filing_type(
    parsed_rows: list[dict[str, Any]], filing: dict[str, Any], jurisdiction: str
) -> str:
    if jurisdiction == "uk":
        return str(parsed_rows[0].get("filing_type") or "unknown") if parsed_rows else "unknown"
    return str(filing.get("form") or "13F-HR")


def _looks_like_holdings_rows(parsed_rows: list[dict[str, Any]]) -> bool:
    if not parsed_rows:
        return False
    required = {"nameOfIssuer", "cusip", "value", "sshPrnamt"}
    return all(required.issubset(row.keys()) for row in parsed_rows)


def _insert_filing(
    conn: Any,
    *,
    manager_id: int | None,
    source: str,
    external_id: str,
    filed_date: str | None,
    filing_type: str,
    parsed_rows: list[dict[str, Any]],
) -> int:
    payload = json.dumps(parsed_rows)
    marker = _placeholder(conn)
    if _is_sqlite(conn):
        sql = (
            "INSERT OR REPLACE INTO filings(manager_id, source, external_id, filed_date, type, parsed_payload) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        cursor = conn.execute(
            sql,
            (manager_id, source, external_id, filed_date, filing_type, payload),
        )
        return int(cursor.lastrowid or 0)
    sql = (
        "INSERT INTO filings(manager_id, source, external_id, filed_date, type, parsed_payload) "
        "VALUES (%s, %s, %s, %s, %s, %s::jsonb) "
        "ON CONFLICT (source, external_id) DO UPDATE SET "
        "manager_id = EXCLUDED.manager_id, "
        "filed_date = EXCLUDED.filed_date, "
        "type = EXCLUDED.type, "
        "parsed_payload = EXCLUDED.parsed_payload "
        "RETURNING id"
    )
    row = conn.execute(
        sql,
        (manager_id, source, external_id, filed_date, filing_type, payload),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _insert_holdings_rows(
    conn: Any,
    *,
    filing_id: int,
    manager_id: int | None,
    identifier: str,
    external_id: str,
    filed_date: str | None,
    parsed_rows: list[dict[str, Any]],
    jurisdiction: str,
) -> int:
    marker = _placeholder(conn)
    sql = (
        "INSERT INTO holdings(filing_id, manager_id, cik, accession, filed, nameOfIssuer, cusip, value, sshPrnamt) "
        f"VALUES ({','.join([marker] * 9)})"
    )
    inserted = 0
    cik_value = identifier if jurisdiction == "us" else None
    for row in parsed_rows:
        conn.execute(
            sql,
            (
                filing_id,
                manager_id,
                cik_value,
                external_id,
                filed_date,
                row.get("nameOfIssuer"),
                row.get("cusip"),
                int(row.get("value") or 0),
                int(row.get("sshPrnamt") or 0),
            ),
        )
        inserted += 1
    return inserted


@task
async def fetch_and_store(
    identifier: str,
    since: str,
    *,
    jurisdiction: str,
    adapter: Any | None = None,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    adapter = adapter or get_adapter(_ADAPTER_MAP.get(jurisdiction, "edgar"))
    filings = await adapter.list_new_filings(identifier, since)
    conn = connect_db(db_path or DB_PATH)
    _ensure_filing_tables(conn)

    results: list[dict[str, Any]] = []
    row_count = 0
    max_results = int(os.getenv("MAX_RESULTS_IN_MEMORY", "100000"))

    for filing in filings:
        raw = await adapter.download(filing)
        external_id = _filing_external_id(filing, jurisdiction)
        ext = "xml" if isinstance(raw, str) else "pdf"
        S3.put_object(
            Bucket=BUCKET,
            Key=f"raw/{external_id}.{ext}",
            Body=raw,
            ServerSideEncryption="AES256",
        )
        parsed_rows = await adapter.parse(raw)
        if isinstance(raw, str):
            store_document(raw)

        manager_id = _lookup_manager_id(conn, jurisdiction, identifier)
        filing_id = _insert_filing(
            conn,
            manager_id=manager_id,
            source=jurisdiction,
            external_id=external_id,
            filed_date=_filing_date(filing, jurisdiction),
            filing_type=_filing_type(parsed_rows, filing, jurisdiction),
            parsed_rows=parsed_rows,
        )

        should_keep_results = row_count < max_results
        if _looks_like_holdings_rows(parsed_rows):
            row_count += _insert_holdings_rows(
                conn,
                filing_id=filing_id,
                manager_id=manager_id,
                identifier=identifier,
                external_id=external_id,
                filed_date=_filing_date(filing, jurisdiction),
                parsed_rows=parsed_rows,
                jurisdiction=jurisdiction,
            )
        conn.commit()
        if should_keep_results:
            results.extend(parsed_rows)

    logger.info(
        "Stored filings",
        extra={"identifier": identifier, "jurisdiction": jurisdiction, "rows": row_count},
    )
    conn.close()
    return results


def _default_identifiers(jurisdiction: str) -> list[str]:
    env_key = _IDENTIFIER_ENV.get(jurisdiction, "CIK_LIST")
    default = _IDENTIFIER_DEFAULT.get(jurisdiction, "")
    raw = os.getenv(env_key, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


@flow
async def ingest_flow(
    *,
    jurisdiction: str,
    identifiers: list[str] | None = None,
    since: str | None = None,
    fetcher: Callable[[str, str], Awaitable[list[dict[str, Any]]]] | None = None,
) -> list[dict[str, Any]]:
    identifiers = identifiers if identifiers is not None else _default_identifiers(jurisdiction)
    since = since or "1970-01-01"
    if fetcher is None:

        async def _default_fetcher(identifier: str, since_date: str) -> list[dict[str, Any]]:
            return await fetch_and_store(
                identifier,
                since_date,
                jurisdiction=jurisdiction,
            )

        fetcher = _default_fetcher

    all_rows: list[dict[str, Any]] = []
    for identifier in identifiers:
        try:
            rows = await fetcher(identifier, since)
            all_rows.extend(rows)
            log_outcome(
                logger,
                "Ingest flow completed",
                has_data=bool(rows),
                extra={"identifier": identifier, "rows": len(rows), "jurisdiction": jurisdiction},
            )
        except UserWarning:
            logger.warning(
                "No filings found",
                extra={"identifier": identifier, "since": since, "jurisdiction": jurisdiction},
            )
        except Exception:
            logger.exception(
                "Ingest flow failed",
                extra={"identifier": identifier, "since": since, "jurisdiction": jurisdiction},
            )
    (RAW_DIR / "parsed.json").write_text(json.dumps(all_rows))
    log_outcome(
        logger,
        "Ingest flow finished",
        has_data=bool(all_rows),
        extra={"total_rows": len(all_rows), "jurisdiction": jurisdiction},
    )
    return all_rows
