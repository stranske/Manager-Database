"""Prefect flow orchestrating filing ingestion across jurisdictions."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast

import boto3
from prefect import flow, task

from adapters.base import (
    connect_db,
    get_adapter,
    get_placeholder,
    get_table_columns,
    is_sqlite,
)
from adapters.base import (
    manager_id_column as shared_manager_id_column,
)
from etl.logging_setup import configure_logging, log_outcome


def store_document(
    text: str,
    db_path: str | None = None,
    manager_id: int | None = None,
    kind: str = "note",
    filename: str | None = None,
) -> int:
    try:
        from embeddings import store_document as _store_document
    except Exception:
        _ = (text, db_path, manager_id, kind, filename)
        return 0
    return _store_document(
        text,
        db_path=db_path,
        manager_id=manager_id,
        kind=kind,
        filename=filename,
    )


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

_ADAPTER_MAP = {"us": "edgar", "uk": "uk", "ca": "canada", "sg": "mas", "au": "asic"}
_IDENTIFIER_ENV = {
    "us": "CIK_LIST",
    "uk": "UK_COMPANY_NUMBERS",
    "ca": "CA_CIK_LIST",
    "sg": "SG_ENTITY_IDS",
    "au": "AU_ASIC_IDS",
}
_IDENTIFIER_DEFAULT = {
    "us": "0001791786,0001434997",
    "uk": "",
    "ca": "",
    "sg": "",
    "au": "",
}

configure_logging("ingest_flow")
logger = logging.getLogger(__name__)

Fetcher = Callable[[str, str], Awaitable[list[dict[str, Any]]]]

_table_columns = get_table_columns


def _manager_id_column(conn: Any) -> str | None:
    return shared_manager_id_column(conn)


def _ensure_filing_tables(conn: Any) -> None:
    if is_sqlite(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS filings (
                id INTEGER PRIMARY KEY,
                manager_id INTEGER,
                source TEXT NOT NULL,
                external_id TEXT NOT NULL,
                filed_date TEXT,
                type TEXT,
                parsed_payload TEXT
            )""")
        filing_columns = _table_columns(conn, "filings")
        if {"source", "external_id"}.issubset(filing_columns):
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS filings_source_external_idx "
                "ON filings(source, external_id)"
            )
        elif "raw_key" in filing_columns:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS filings_raw_key_idx " "ON filings(raw_key)"
            )
        conn.execute("""CREATE TABLE IF NOT EXISTS holdings (
                id INTEGER PRIMARY KEY,
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
    filing_columns = _table_columns(conn, "filings")
    if {"source", "external_id"}.issubset(filing_columns):
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS filings_source_external_idx "
            "ON filings(source, external_id)"
        )
    elif "raw_key" in filing_columns:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS filings_raw_key_idx ON filings(raw_key)")
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
    id_column = _manager_id_column(conn)
    if not id_column:
        return None
    marker = get_placeholder(conn)
    try:
        if jurisdiction in {"us", "ca"}:
            row = conn.execute(
                f"SELECT {id_column} FROM managers WHERE cik = {marker} LIMIT 1",
                (identifier,),
            ).fetchone()
        elif jurisdiction in {"uk", "sg", "au"}:
            registry_key = {
                "uk": "uk_company_number",
                "sg": "sg_entity_id",
                "au": "au_asic_id",
            }[jurisdiction]
            if is_sqlite(conn):
                row = conn.execute(
                    f"SELECT {id_column} FROM managers "
                    f"WHERE json_extract(registry_ids, '$.{registry_key}') = ? LIMIT 1",
                    (identifier,),
                ).fetchone()
            else:
                row = conn.execute(
                    f"SELECT {id_column} FROM managers "
                    f"WHERE registry_ids->>'{registry_key}' = %s LIMIT 1",
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
    if jurisdiction in {"ca", "sg", "au"}:
        return str(filing.get("id") or filing.get("accession") or "")
    return str(filing.get("accession", ""))


def _filing_date(filing: dict[str, Any], jurisdiction: str) -> str | None:
    if jurisdiction in {"uk", "sg", "au"}:
        return filing.get("date")
    return filing.get("filed")


def _filing_type(
    parsed_rows: list[dict[str, Any]], filing: dict[str, Any], jurisdiction: str
) -> str:
    if jurisdiction in {"uk", "ca", "sg", "au"}:
        if not parsed_rows:
            return "unknown"
        first_row = parsed_rows[0]
        for key in ("filing_type", "type", "form_type", "form"):
            value = first_row.get(key)
            if value:
                return str(value)
        return "unknown"
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
    filing_columns = _table_columns(conn, "filings")
    id_column = "filing_id" if "filing_id" in filing_columns else "id"
    raw_key = f"{source}:{external_id}"
    has_external_id = "external_id" in filing_columns

    if is_sqlite(conn):
        if has_external_id:
            sql = (
                "INSERT INTO filings(manager_id, source, external_id, filed_date, type, parsed_payload) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(source, external_id) DO UPDATE SET "
                "manager_id = excluded.manager_id, "
                "filed_date = excluded.filed_date, "
                "type = excluded.type, "
                "parsed_payload = excluded.parsed_payload "
                f"RETURNING {id_column}"
            )
            row = conn.execute(
                sql,
                (manager_id, source, external_id, filed_date, filing_type, payload),
            ).fetchone()
            return int(row[0]) if row and row[0] is not None else 0
        sql = (
            "INSERT INTO filings(manager_id, source, type, filed_date, raw_key, parsed_payload) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(raw_key) DO UPDATE SET "
            "manager_id = excluded.manager_id, "
            "filed_date = excluded.filed_date, "
            "type = excluded.type, "
            "parsed_payload = excluded.parsed_payload "
            f"RETURNING {id_column}"
        )
        row = conn.execute(
            sql,
            (manager_id, source, filing_type, filed_date, raw_key, payload),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    if has_external_id:
        sql = (
            "INSERT INTO filings(manager_id, source, external_id, filed_date, type, parsed_payload) "
            "VALUES (%s, %s, %s, %s, %s, %s::jsonb) "
            "ON CONFLICT (source, external_id) DO UPDATE SET "
            "manager_id = EXCLUDED.manager_id, "
            "filed_date = EXCLUDED.filed_date, "
            "type = EXCLUDED.type, "
            "parsed_payload = EXCLUDED.parsed_payload "
            f"RETURNING {id_column}"
        )
        row = conn.execute(
            sql,
            (manager_id, source, external_id, filed_date, filing_type, payload),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    sql = (
        "INSERT INTO filings(manager_id, type, filed_date, source, raw_key, parsed_payload) "
        "VALUES (%s, %s, %s, %s, %s, %s::jsonb) "
        "ON CONFLICT (raw_key) DO UPDATE SET "
        "manager_id = EXCLUDED.manager_id, "
        "filed_date = EXCLUDED.filed_date, "
        "type = EXCLUDED.type, "
        "parsed_payload = EXCLUDED.parsed_payload "
        f"RETURNING {id_column}"
    )
    row = conn.execute(
        sql,
        (manager_id, filing_type, filed_date, source, raw_key, payload),
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
    holdings_columns = _table_columns(conn, "holdings")
    canonical_holdings = {"name_of_issuer", "shares", "value_usd"}.issubset(holdings_columns)
    marker = get_placeholder(conn)
    if canonical_holdings:
        sql = (
            "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
            f"VALUES ({','.join([marker] * 5)})"
        )
    else:
        sql = (
            "INSERT INTO holdings(filing_id, manager_id, cik, accession, filed, nameOfIssuer, cusip, value, sshPrnamt) "
            f"VALUES ({','.join([marker] * 9)})"
        )
    inserted = 0
    cik_value = identifier if jurisdiction == "us" else None
    for row in parsed_rows:
        if canonical_holdings:
            conn.execute(
                sql,
                (
                    filing_id,
                    row.get("cusip"),
                    row.get("nameOfIssuer"),
                    int(row.get("sshPrnamt") or 0),
                    int(row.get("value") or 0),
                ),
            )
        else:
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


def _delete_holdings_rows(conn: Any, *, filing_id: int) -> None:
    holdings_columns = _table_columns(conn, "holdings")
    if "filing_id" not in holdings_columns:
        return
    marker = get_placeholder(conn)
    conn.execute(f"DELETE FROM holdings WHERE filing_id = {marker}", (filing_id,))


def _run_in_transaction(conn: Any, work: Any) -> Any:
    transaction = getattr(conn, "transaction", None)
    if callable(transaction):
        with transaction():
            return work()
    return work()


def _rollback_quietly(conn: Any) -> None:
    rollback = getattr(conn, "rollback", None)
    if callable(rollback):
        try:
            rollback()
        except Exception:
            logger.warning("Failed to roll back ingest transaction", exc_info=True)


def _enable_transactional_writes(conn: Any) -> bool | None:
    if isinstance(conn, sqlite3.Connection) or not hasattr(conn, "autocommit"):
        return None
    original = bool(conn.autocommit)
    if original:
        conn.autocommit = False
    return original


def _restore_autocommit_quietly(conn: Any, original: bool | None) -> None:
    if original is None:
        return
    try:
        conn.autocommit = original
    except Exception:
        logger.warning("Failed to restore ingest database autocommit", exc_info=True)


def _close_quietly(conn: Any) -> None:
    close = getattr(conn, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            logger.warning("Failed to close ingest database connection", exc_info=True)


def _replace_holdings_rows(
    conn: Any,
    *,
    filing_id: int,
    manager_id: int,
    identifier: str,
    external_id: str,
    filed_date: str | None,
    parsed_rows: list[dict[str, Any]],
    jurisdiction: str,
) -> int:
    def _work() -> int:
        _delete_holdings_rows(conn, filing_id=filing_id)
        return _insert_holdings_rows(
            conn,
            filing_id=filing_id,
            manager_id=manager_id,
            identifier=identifier,
            external_id=external_id,
            filed_date=filed_date,
            parsed_rows=parsed_rows,
            jurisdiction=jurisdiction,
        )

    return int(_run_in_transaction(conn, _work))


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
    original_autocommit: bool | None = None
    try:
        original_autocommit = _enable_transactional_writes(conn)
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
            if manager_id is None:
                logger.warning(
                    "Manager not found; skipping filing",
                    extra={
                        "jurisdiction": jurisdiction,
                        "identifier": identifier,
                        "external_id": external_id,
                    },
                )
                continue
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
            # UK filings are metadata-driven (e.g., CS01/AR01) and should be
            # stored as parsed payload on the filings row, not expanded holdings.
            if jurisdiction != "uk" and _looks_like_holdings_rows(parsed_rows):
                row_count += _replace_holdings_rows(
                    conn,
                    filing_id=filing_id,
                    manager_id=manager_id,
                    identifier=identifier,
                    external_id=external_id,
                    filed_date=_filing_date(filing, jurisdiction),
                    parsed_rows=parsed_rows,
                    jurisdiction=jurisdiction,
                )
            if should_keep_results:
                results.extend(parsed_rows)

        conn.commit()
        logger.info(
            "Stored filings",
            extra={"identifier": identifier, "jurisdiction": jurisdiction, "rows": row_count},
        )
        return results
    except Exception:
        _rollback_quietly(conn)
        raise
    finally:
        _restore_autocommit_quietly(conn, original_autocommit)
        _close_quietly(conn)


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
    fetcher: object | None = None,
) -> list[dict[str, Any]]:
    identifiers = identifiers if identifiers is not None else _default_identifiers(jurisdiction)
    since = since or "1970-01-01"
    resolved_fetcher: Fetcher
    if fetcher is None:

        async def _default_fetcher(identifier: str, since_date: str) -> list[dict[str, Any]]:
            return await fetch_and_store(
                identifier,
                since_date,
                jurisdiction=jurisdiction,
            )

        resolved_fetcher = _default_fetcher
    elif callable(fetcher):
        resolved_fetcher = cast(Fetcher, fetcher)
    else:
        raise TypeError("fetcher must be callable")

    all_rows: list[dict[str, Any]] = []
    for identifier in identifiers:
        try:
            rows = await resolved_fetcher(identifier, since)
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
