"""Prefect flow for Schedule 13D/13G activism filing ingestion."""

from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
from typing import Any

from prefect import flow, task
from prefect.schedules import Cron

from adapters import edgar
from adapters.base import connect_db
from etl.edgar_flow import BUCKET, S3
from etl.logging_setup import configure_logging, log_outcome

configure_logging("activism_flow")
logger = logging.getLogger(__name__)

ACTIVISM_FORMS = ["SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"]
ACTIVISM_FLOW_NIGHTLY_CRON = os.getenv("ACTIVISM_FLOW_CRON", "0 4 * * *")
ACTIVISM_FLOW_TIMEZONE = os.getenv("ACTIVISM_FLOW_TIMEZONE", "UTC")
DB_PATH = os.getenv("DB_PATH", "dev.db")


def _is_sqlite(conn: Any) -> bool:
    return isinstance(conn, sqlite3.Connection)


def _placeholder(conn: Any) -> str:
    return "?" if _is_sqlite(conn) else "%s"


def _ensure_activism_filings_table(conn: Any) -> None:
    if _is_sqlite(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS activism_filings (
                filing_id INTEGER PRIMARY KEY AUTOINCREMENT,
                manager_id INTEGER NOT NULL,
                filing_type TEXT NOT NULL,
                subject_company TEXT NOT NULL,
                subject_cusip TEXT,
                ownership_pct REAL,
                shares INTEGER,
                group_members TEXT,
                purpose_snippet TEXT,
                filed_date TEXT NOT NULL,
                url TEXT NOT NULL,
                raw_key TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (manager_id, filing_type, subject_cusip, filed_date)
            )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activism_manager ON activism_filings(manager_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activism_cusip ON activism_filings(subject_cusip)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activism_date ON activism_filings(filed_date DESC)"
        )
        return

    try:
        conn.execute("SELECT 1 FROM activism_filings LIMIT 1")
    except Exception as exc:
        message = str(exc)
        exc_name = exc.__class__.__name__
        pgcode = getattr(exc, "pgcode", None)
        missing_table = (
            "does not exist" in message or pgcode == "42P01" or "UndefinedTable" in exc_name
        )
        if missing_table:
            raise RuntimeError(
                "activism_filings table is missing on Postgres; apply schema migrations first"
            ) from exc
        raise


def _load_manager_row(conn: Any, manager_id: int) -> tuple[str, str] | None:
    ph = _placeholder(conn)
    row = conn.execute(
        f"SELECT name, cik FROM managers WHERE manager_id = {ph} LIMIT 1",
        (manager_id,),
    ).fetchone()
    if not row or not row[1]:
        return None
    return str(row[0] or ""), str(row[1])


def _all_manager_ids(conn: Any) -> list[int]:
    rows = conn.execute(
        "SELECT manager_id FROM managers WHERE cik IS NOT NULL ORDER BY manager_id"
    ).fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def _serialize_group_members(value: object, *, sqlite_mode: bool) -> object:
    if sqlite_mode:
        if isinstance(value, list):
            return "|".join(str(item) for item in value)
        return ""
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def _upsert_activism_filing(
    conn: Any,
    *,
    manager_id: int,
    filing: dict[str, str],
    parsed: dict[str, object],
    raw_key: str,
) -> None:
    ph = _placeholder(conn)
    values = (
        manager_id,
        filing["form"],
        str(parsed.get("subject_company") or ""),
        str(parsed.get("cusip") or "") or None,
        parsed.get("ownership_pct"),
        parsed.get("shares"),
        _serialize_group_members(parsed.get("group_members"), sqlite_mode=_is_sqlite(conn)),
        str(parsed.get("purpose_snippet") or "") or None,
        str(parsed.get("filed_date") or filing.get("filed") or ""),
        str(filing.get("url") or "https://www.sec.gov"),
        raw_key,
    )
    if _is_sqlite(conn):
        conn.execute(
            "INSERT OR IGNORE INTO activism_filings("
            "manager_id, filing_type, subject_company, subject_cusip, ownership_pct, shares, "
            "group_members, purpose_snippet, filed_date, url, raw_key"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values,
        )
        return
    conn.execute(
        "INSERT INTO activism_filings("
        "manager_id, filing_type, subject_company, subject_cusip, ownership_pct, shares, "
        "group_members, purpose_snippet, filed_date, url, raw_key"
        f") VALUES ({', '.join([ph] * 11)}) "
        "ON CONFLICT(manager_id, filing_type, subject_cusip, filed_date) DO UPDATE SET "
        "subject_company = excluded.subject_company, ownership_pct = excluded.ownership_pct, "
        "shares = excluded.shares, group_members = excluded.group_members, "
        "purpose_snippet = excluded.purpose_snippet, url = excluded.url, raw_key = excluded.raw_key",
        values,
    )


@task
async def fetch_activism_filings(manager_id: int, since: str) -> list[dict[str, object]]:
    conn = connect_db(DB_PATH)
    _ensure_activism_filings_table(conn)

    manager_row = _load_manager_row(conn, manager_id)
    if manager_row is None:
        logger.warning(
            "Skipping activism fetch for manager without CIK", extra={"manager_id": manager_id}
        )
        conn.close()
        return []

    manager_name, cik = manager_row
    try:
        filings = await edgar.list_new_filings(
            cik,
            since,
            form_types=ACTIVISM_FORMS,
            manager_name=manager_name,
        )
    except UserWarning:
        conn.close()
        return []

    inserted: list[dict[str, object]] = []
    for filing in filings:
        form_type = str(filing.get("form") or "")
        if form_type not in ACTIVISM_FORMS:
            continue
        raw_text = await edgar.download(filing)
        raw_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()[:16]
        accession = str(filing.get("accession") or raw_hash)
        raw_key = f"raw/activism/{raw_hash}_{accession}.txt"
        S3.put_object(Bucket=BUCKET, Key=raw_key, Body=raw_text, ServerSideEncryption="AES256")
        parsed = await edgar.parse(raw_text, form_type=form_type)
        if not isinstance(parsed, dict):
            continue
        _upsert_activism_filing(
            conn,
            manager_id=manager_id,
            filing=filing,
            parsed=parsed,
            raw_key=raw_key,
        )
        inserted.append({**parsed, "filing_type": form_type, "raw_key": raw_key})

    conn.commit()
    conn.close()
    return inserted


@task
async def fetch_all_managers(since: str) -> list[dict[str, object]]:
    conn = connect_db(DB_PATH)
    _ensure_activism_filings_table(conn)
    manager_ids = _all_manager_ids(conn)
    conn.close()

    rows: list[dict[str, object]] = []
    for manager_id in manager_ids:
        rows.extend(await fetch_activism_filings.fn(manager_id, since))
    return rows


@flow(name="activism-ingestion")
async def activism_flow(since: str = "2024-01-01") -> list[dict[str, object]]:
    rows = await fetch_all_managers.fn(since)
    log_outcome(
        logger,
        "Activism flow finished",
        has_data=bool(rows),
        extra={"rows": len(rows)},
    )
    return rows


activism_flow_deployment = activism_flow.to_deployment(
    name="activism-ingestion",
    schedule=Cron(ACTIVISM_FLOW_NIGHTLY_CRON, timezone=ACTIVISM_FLOW_TIMEZONE),
)
activism_deployment = activism_flow_deployment
