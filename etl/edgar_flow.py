"""US EDGAR flow with backward-compatible behavior."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
from typing import Any, cast

from prefect import flow, task

import etl.ingest_flow as ingest_module
from adapters.base import connect_db, get_adapter
from alerts.integration import build_new_filing_event, fire_alerts_for_event
from etl.logging_setup import configure_logging, log_outcome

try:
    from embeddings import store_document
except ModuleNotFoundError:

    def store_document(
        text: str,
        db_path: str | None = None,
        manager_id: int | None = None,
        kind: str = "note",
        filename: str | None = None,
    ) -> int:
        _ = (text, db_path, manager_id, kind, filename)
        return 0


RAW_DIR = ingest_module.RAW_DIR
S3 = ingest_module.S3
BUCKET = ingest_module.BUCKET
DB_PATH = os.getenv("DB_PATH", "dev.db")
ADAPTER = get_adapter("edgar")

configure_logging("edgar_flow")
logger = logging.getLogger(__name__)


class _EdgarLogProxy:
    def __init__(self, base: logging.Logger) -> None:
        self._base = base

    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        self._base.log(level, msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._base.warning(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._base.info(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        mapped = "EDGAR flow failed" if msg == "Ingest flow failed" else msg
        self._base.exception(mapped, *args, **kwargs)


def _columns(conn: Any, table: str) -> set[str]:
    try:
        if isinstance(conn, sqlite3.Connection):
            rows = conn.execute(f"PRAGMA table_xinfo({table})").fetchall()
            return {str(row[1]) for row in rows}
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table,),
        ).fetchall()
        return {str(row[0]) for row in rows}
    except Exception:
        return set()
    return {str(row[1]) for row in rows}


def _placeholder(conn: Any) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def _manager_id_for_cik(conn: Any, cik: str) -> int | None:
    manager_cols = _columns(conn, "managers")
    if not manager_cols:
        return None
    id_col = (
        "manager_id" if "manager_id" in manager_cols else ("id" if "id" in manager_cols else None)
    )
    if not id_col or "cik" not in manager_cols:
        return None
    marker = _placeholder(conn)
    row = conn.execute(
        f"SELECT {id_col} FROM managers WHERE cik = {marker} LIMIT 1", (cik,)
    ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _ensure_legacy_tables(conn: Any) -> None:
    if isinstance(conn, sqlite3.Connection):
        conn.execute("""CREATE TABLE IF NOT EXISTS filings (
                filing_id INTEGER PRIMARY KEY,
                manager_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                period_end TEXT,
                filed_date TEXT,
                source TEXT NOT NULL,
                url TEXT,
                raw_key TEXT UNIQUE,
                parsed_payload TEXT,
                schema_version INTEGER
            )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS holdings (
                holding_id INTEGER PRIMARY KEY,
                filing_id INTEGER NOT NULL,
                cusip TEXT,
                isin TEXT,
                name_of_issuer TEXT,
                shares INTEGER,
                value_usd NUMERIC,
                delta_type TEXT,
                FOREIGN KEY(filing_id) REFERENCES filings(filing_id)
            )""")
        return
    conn.execute("""CREATE TABLE IF NOT EXISTS filings (
            filing_id bigserial PRIMARY KEY,
            manager_id bigint NOT NULL REFERENCES managers(manager_id),
            type text NOT NULL,
            period_end date,
            filed_date date,
            source text NOT NULL,
            url text,
            raw_key text,
            parsed_payload jsonb,
            schema_version int DEFAULT 1,
            created_at timestamptz DEFAULT now()
        )""")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_filings_raw_key_unique "
        "ON filings (raw_key) WHERE raw_key IS NOT NULL"
    )
    conn.execute("""CREATE TABLE IF NOT EXISTS holdings (
            holding_id bigserial PRIMARY KEY,
            filing_id bigint NOT NULL REFERENCES filings(filing_id),
            cusip text,
            isin text,
            name_of_issuer text,
            shares bigint,
            value_usd numeric(18,2),
            delta_type text,
            created_at timestamptz DEFAULT now()
        )""")


def _upsert_filing_legacy(
    conn: Any, manager_id: int | None, filing_type: str, filed_date: str | None, raw_key: str
) -> int:
    if manager_id is None:
        return 0
    payload = json.dumps({"raw_key": raw_key})
    if isinstance(conn, sqlite3.Connection):
        columns = _columns(conn, "filings")
        values: dict[str, Any] = {
            "manager_id": manager_id,
            "type": filing_type,
            "filed_date": filed_date,
            "source": "edgar",
            "raw_key": raw_key,
            "parsed_payload": payload,
        }
        insert_columns = [column for column in values if column in columns]
        update_columns = [column for column in insert_columns if column != "raw_key"]
        row = conn.execute(
            f"INSERT INTO filings({', '.join(insert_columns)}) "
            f"VALUES ({', '.join('?' for _ in insert_columns)}) "
            "ON CONFLICT(raw_key) DO UPDATE SET "
            f"{', '.join(f'{column} = excluded.{column}' for column in update_columns)} "
            "RETURNING filing_id",
            [values[column] for column in insert_columns],
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    row = conn.execute(
        "INSERT INTO filings(manager_id, type, filed_date, source, raw_key, parsed_payload) "
        "VALUES (%s, %s, %s, %s, %s, %s::jsonb) "
        "ON CONFLICT (raw_key) WHERE raw_key IS NOT NULL DO UPDATE SET "
        "manager_id = EXCLUDED.manager_id, "
        "type = EXCLUDED.type, "
        "filed_date = EXCLUDED.filed_date, "
        "parsed_payload = EXCLUDED.parsed_payload "
        "RETURNING filing_id",
        (manager_id, filing_type, filed_date, "edgar", raw_key, payload),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _insert_holding_legacy(
    conn: Any,
    filing_id: int,
    row: dict[str, Any],
    *,
    manager_id: int | None,
    cik: str,
    accession: str,
    filed_date: str | None,
) -> None:
    columns = _columns(conn, "holdings")
    marker = _placeholder(conn)
    values: dict[str, Any] = {
        "filing_id": filing_id,
        "cusip": row.get("cusip"),
        "name_of_issuer": row.get("nameOfIssuer"),
        "shares": int(row.get("sshPrnamt") or 0),
        "value_usd": int(row.get("value") or 0),
    }
    if isinstance(conn, sqlite3.Connection):
        values.update(
            {
                "manager_id": manager_id,
                "cik": cik,
                "accession": accession,
                "filed": filed_date,
                "nameOfIssuer": row.get("nameOfIssuer"),
                "value": int(row.get("value") or 0),
                "sshPrnamt": int(row.get("sshPrnamt") or 0),
            }
        )
    insert_columns = [column for column in values if column in columns]
    if not insert_columns:
        return
    conn.execute(
        f"INSERT INTO holdings({', '.join(insert_columns)}) "
        f"VALUES ({', '.join(marker for _ in insert_columns)})",
        [values[column] for column in insert_columns],
    )


@task
async def fetch_and_store(cik: str, since: str):
    filings = await ADAPTER.list_new_filings(cik, since)
    conn = connect_db(DB_PATH)
    _ensure_legacy_tables(conn)

    manager_cols = _columns(conn, "managers")
    manager_id = _manager_id_for_cik(conn, cik)
    if manager_cols and manager_id is None:
        logger.warning("Manager not found; skipping filings", extra={"cik": cik})
        conn.close()
        return []

    all_rows: list[dict[str, Any]] = []
    for filing in filings:
        raw = await ADAPTER.download(filing)
        raw_bytes = raw.encode("utf-8") if isinstance(raw, str) else raw
        raw_hash = hashlib.sha256(raw_bytes).hexdigest()[:16]
        accession = str(filing.get("accession") or "unknown")
        raw_key = f"raw/edgar/{raw_hash}_{accession}.xml"

        S3.put_object(Bucket=BUCKET, Key=raw_key, Body=raw, ServerSideEncryption="AES256")
        if isinstance(raw, str):
            try:
                store_document(
                    raw,
                    db_path=DB_PATH,
                    manager_id=manager_id,
                    kind="filing_text",
                    filename=f"{accession}.xml",
                )
            except TypeError:
                store_document(raw)

        parsed_rows = await ADAPTER.parse(raw)
        filing_id = _upsert_filing_legacy(
            conn,
            manager_id=manager_id,
            filing_type=str(filing.get("form") or "13F-HR"),
            filed_date=filing.get("filed"),
            raw_key=raw_key,
        )
        for row in parsed_rows:
            _insert_holding_legacy(
                conn,
                filing_id,
                row,
                manager_id=manager_id,
                cik=cik,
                accession=accession,
                filed_date=filing.get("filed"),
            )
        conn.commit()
        await fire_alerts_for_event(
            conn,
            build_new_filing_event(
                filing_id=filing_id if filing_id > 0 else None,
                manager_id=manager_id,
                filing_type=str(filing.get("form") or "13F-HR"),
                filed_date=filing.get("filed"),
                payload={"accession": accession, "source": "edgar"},
            ),
        )
        all_rows.extend(parsed_rows)
    conn.close()
    return all_rows


@flow
async def edgar_flow(cik_list: list[str] | None = None, since: str | None = None):
    ingest_module.RAW_DIR = RAW_DIR
    ingest_module.S3 = S3
    ingest_module.BUCKET = BUCKET
    ingest_module.DB_PATH = DB_PATH
    ingest_module.logger = cast(Any, _EdgarLogProxy(logger))
    ingest_callable = getattr(ingest_module.ingest_flow, "fn", ingest_module.ingest_flow)
    all_rows = await ingest_callable(
        jurisdiction="us",
        identifiers=cik_list,
        since=since,
        fetcher=fetch_and_store,
    )
    log_outcome(
        logger,
        "EDGAR flow finished",
        has_data=bool(all_rows),
        extra={"total_rows": len(all_rows)},
    )
    return all_rows


if __name__ == "__main__":
    import asyncio

    asyncio.run(edgar_flow())
