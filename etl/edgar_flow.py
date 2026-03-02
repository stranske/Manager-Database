"""US EDGAR flow with backward-compatible behavior."""

from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
from typing import Any, cast

from prefect import flow, task

import etl.ingest_flow as ingest_module
from adapters.base import connect_db, get_adapter
from etl.logging_setup import configure_logging, log_outcome

try:
    from embeddings import store_document
except ModuleNotFoundError:

    def store_document(text: str, db_path: str | None = None) -> None:
        _ = (text, db_path)
        return


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
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {str(row[1]) for row in rows}


def _manager_id_for_cik(conn: Any, cik: str) -> int | None:
    manager_cols = _columns(conn, "managers")
    if not manager_cols:
        return None
    id_col = (
        "manager_id" if "manager_id" in manager_cols else ("id" if "id" in manager_cols else None)
    )
    if not id_col or "cik" not in manager_cols:
        return None
    row = conn.execute(f"SELECT {id_col} FROM managers WHERE cik = ? LIMIT 1", (cik,)).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _ensure_legacy_tables(conn: Any) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS filings (
            filing_id INTEGER PRIMARY KEY AUTOINCREMENT,
            manager_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            filed_date TEXT,
            source TEXT,
            url TEXT,
            raw_key TEXT UNIQUE,
            schema_version INTEGER
        )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS holdings (
            holding_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id INTEGER NOT NULL,
            cusip TEXT,
            name_of_issuer TEXT,
            shares INTEGER,
            value_usd INTEGER,
            FOREIGN KEY(filing_id) REFERENCES filings(filing_id)
        )""")


def _upsert_filing_legacy(
    conn: Any, manager_id: int, filing_type: str, filed_date: str | None, raw_key: str
) -> int:
    conn.execute(
        "INSERT OR IGNORE INTO filings(manager_id, type, filed_date, source, raw_key) "
        "VALUES (?, ?, ?, ?, ?)",
        (manager_id, filing_type, filed_date, "edgar", raw_key),
    )
    row = conn.execute("SELECT filing_id FROM filings WHERE raw_key = ?", (raw_key,)).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _insert_holding_legacy(conn: Any, filing_id: int, row: dict[str, Any]) -> None:
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            filing_id,
            row.get("cusip"),
            row.get("nameOfIssuer"),
            int(row.get("sshPrnamt") or 0),
            int(row.get("value") or 0),
        ),
    )


@task
async def fetch_and_store(cik: str, since: str):
    filings = await ADAPTER.list_new_filings(cik, since)
    conn = connect_db(DB_PATH)
    _ensure_legacy_tables(conn)

    manager_id = _manager_id_for_cik(conn, cik)
    if manager_id is None:
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
            _insert_holding_legacy(conn, filing_id, row)
        conn.commit()
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
