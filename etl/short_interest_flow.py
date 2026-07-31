"""Ingest and expose short-interest annotations without re-scoring conviction (#1470)."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import date
from typing import Any

from adapters.base import is_sqlite
from adapters.short_interest import ShortInterestFetchError, fetch_short_interest
from etl.insider_flow import resolve_held_issuers

logger = logging.getLogger(__name__)

DDL_SQLITE = """
CREATE TABLE IF NOT EXISTS short_interest (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    cusip TEXT,
    short_interest REAL,
    float_shares REAL,
    short_interest_pct REAL,
    report_date TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'finra',
    ingested_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ticker, report_date, source)
)
"""

DDL_PG = """
CREATE TABLE IF NOT EXISTS short_interest (
    metric_id bigserial PRIMARY KEY,
    ticker text NOT NULL,
    cusip text,
    short_interest numeric,
    float_shares numeric,
    short_interest_pct numeric,
    report_date date NOT NULL,
    source text NOT NULL DEFAULT 'finra',
    ingested_at timestamptz DEFAULT now(),
    UNIQUE (ticker, report_date, source)
)
"""


def ensure_short_interest_table(conn: Any) -> None:
    """Create the short-interest table and lookup indexes when absent."""
    conn.execute(DDL_SQLITE if is_sqlite(conn) else DDL_PG)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_short_interest_ticker_date ON short_interest (ticker, report_date DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_short_interest_cusip_date ON short_interest (cusip, report_date DESC)"
    )
    if hasattr(conn, "commit"):
        conn.commit()


def upsert_short_interest(conn: Any, rows: list[Mapping[str, Any]]) -> int:
    """Upsert finite short-interest records by ticker/reporting date/source."""
    if not rows:
        return 0
    ensure_short_interest_table(conn)
    sql = (
        "INSERT INTO short_interest (ticker, cusip, short_interest, float_shares, short_interest_pct, report_date, source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(ticker, report_date, source) DO UPDATE SET "
        "cusip=excluded.cusip, short_interest=excluded.short_interest, float_shares=excluded.float_shares, short_interest_pct=excluded.short_interest_pct"
        if is_sqlite(conn)
        else "INSERT INTO short_interest (ticker, cusip, short_interest, float_shares, short_interest_pct, report_date, source) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT(ticker, report_date, source) DO UPDATE SET cusip=EXCLUDED.cusip, short_interest=EXCLUDED.short_interest, float_shares=EXCLUDED.float_shares, short_interest_pct=EXCLUDED.short_interest_pct"
    )
    changed = 0
    for row in rows:
        report_date = str(row.get("report_date") or date.today().isoformat())
        cur = conn.execute(
            sql,
            (
                row["ticker"],
                row.get("cusip"),
                row.get("short_interest"),
                row.get("float_shares"),
                row.get("short_interest_pct"),
                report_date,
                row.get("source") or "finra",
            ),
        )
        changed += int(getattr(cur, "rowcount", 0) or 0)
    if hasattr(conn, "commit"):
        conn.commit()
    return changed


def ingest_short_interest_for_issuers(
    conn: Any,
    issuers: list[Mapping[str, Any]],
    *,
    fetcher: Callable[[str], list[Mapping[str, Any]]] | None = None,
    raise_on_fetch_error: bool = False,
) -> dict[str, Any]:
    """Fetch per held issuer; a missing/rate-limited feed never drops conviction rows."""
    ensure_short_interest_table(conn)
    inserted = 0
    errors: list[str] = []
    for issuer in issuers:
        ticker = str(issuer.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        try:
            rows = fetch_short_interest(ticker, fetcher=fetcher)
        except Exception as exc:
            message = f"{ticker}: {exc}"
            errors.append(message)
            logger.warning("short-interest fetch failed for %s: %s", ticker, exc)
            if raise_on_fetch_error:
                raise ShortInterestFetchError(message) from exc
            continue
        for row in rows:
            if not row.get("cusip") and issuer.get("cusip"):
                row["cusip"] = issuer["cusip"]
        inserted += upsert_short_interest(conn, rows)
    return {"inserted": inserted, "errors": errors, "issuer_count": len(issuers)}


def ingest_short_interest_for_manager(
    conn: Any,
    manager_id: int,
    *,
    fetcher: Callable[[str], list[Mapping[str, Any]]] | None = None,
    raise_on_fetch_error: bool = False,
) -> dict[str, Any]:
    return ingest_short_interest_for_issuers(
        conn,
        resolve_held_issuers(conn, manager_id=manager_id),
        fetcher=fetcher,
        raise_on_fetch_error=raise_on_fetch_error,
    )


def short_interest_annotation(
    conn: Any, ticker: str | None, *, cusip: str | None = None
) -> dict[str, Any] | None:
    """Return the latest short-interest context for a holding, or ``None`` when absent.

    This is a read path: schema initialization belongs to the migration or the
    ingest flow, so a read-only database role can serve annotations.
    """
    if not ticker and not cusip:
        return None
    ph = "?" if is_sqlite(conn) else "%s"
    sql = (
        "SELECT short_interest, float_shares, short_interest_pct, report_date, source "
        "FROM short_interest WHERE {clause} ORDER BY report_date DESC, metric_id DESC LIMIT 1"
    )
    # CUSIP is the issuer identity; a ticker can be reassigned to another issuer.
    lookups: list[tuple[str, Any]] = []
    if cusip:
        lookups.append((f"cusip = {ph}", cusip))
    if ticker:
        lookups.append((f"UPPER(ticker) = UPPER({ph})", ticker))
    row = None
    for clause, param in lookups:
        row = conn.execute(sql.format(clause=clause), (param,)).fetchone()
        if row:
            break
    if not row:
        return None
    return {
        "short_interest": row[0],
        "float_shares": row[1],
        "short_interest_pct": row[2],
        "short_interest_report_date": row[3],
        "short_interest_source": row[4],
    }
