"""ETL flow: ingest Form-4 insider transactions and annotate conviction rows."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import date, timedelta
from typing import Any

from adapters.insider import (
    InsiderFetchError,
    fetch_form4_transactions,
    net_direction_for_rows,
)

logger = logging.getLogger(__name__)

DDL_SQLITE = """
CREATE TABLE IF NOT EXISTS insider_transactions (
    txn_id INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer_cik TEXT NOT NULL,
    ticker TEXT,
    insider_name TEXT,
    txn_code TEXT,
    shares REAL,
    txn_date TEXT,
    acquired_disposed TEXT,
    cusip TEXT,
    ingested_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (issuer_cik, ticker, insider_name, txn_code, shares, txn_date, acquired_disposed)
)
"""

DDL_PG = """
CREATE TABLE IF NOT EXISTS insider_transactions (
    txn_id bigserial PRIMARY KEY,
    issuer_cik text NOT NULL,
    ticker text,
    insider_name text,
    txn_code text,
    shares numeric,
    txn_date date,
    acquired_disposed text,
    cusip text,
    ingested_at timestamptz DEFAULT now(),
    UNIQUE (issuer_cik, ticker, insider_name, txn_code, shares, txn_date, acquired_disposed)
)
"""


def _is_sqlite(conn: Any) -> bool:
    return conn.__class__.__module__.startswith("sqlite3")


def ensure_insider_transactions_table(conn: Any) -> None:
    """Create insider_transactions when missing (SQLite bootstrap / tests)."""
    ddl = DDL_SQLITE if _is_sqlite(conn) else DDL_PG
    conn.execute(ddl)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_insider_issuer_date "
        "ON insider_transactions (issuer_cik, txn_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_insider_ticker_date "
        "ON insider_transactions (ticker, txn_date)"
    )
    if hasattr(conn, "commit"):
        conn.commit()


def upsert_insider_transactions(conn: Any, rows: list[Mapping[str, Any]]) -> int:
    """Insert normalized insider rows; ignore duplicates on the natural key."""
    if not rows:
        return 0
    ensure_insider_transactions_table(conn)
    sql = (
        "INSERT OR IGNORE INTO insider_transactions "
        "(issuer_cik, ticker, insider_name, txn_code, shares, txn_date, acquired_disposed, cusip) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        if _is_sqlite(conn)
        else (
            "INSERT INTO insider_transactions "
            "(issuer_cik, ticker, insider_name, txn_code, shares, txn_date, acquired_disposed, cusip) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (issuer_cik, ticker, insider_name, txn_code, shares, txn_date, acquired_disposed) "
            "DO NOTHING"
        )
    )
    inserted = 0
    for row in rows:
        params = (
            row.get("issuer_cik"),
            row.get("ticker"),
            row.get("insider_name"),
            row.get("txn_code"),
            row.get("shares"),
            row.get("txn_date"),
            row.get("acquired_disposed"),
            row.get("cusip"),
        )
        cur = conn.execute(sql, params)
        try:
            inserted += int(getattr(cur, "rowcount", 0) or 0)
        except Exception:
            inserted += 0
    if hasattr(conn, "commit"):
        conn.commit()
    return inserted


def _holdings_current_clause(conn: Any) -> str:
    """Prefer non-superseded holdings when the bitemporal column exists."""
    try:
        cols = (
            {
                str(r[1] if not isinstance(r, Mapping) else r.get("name") or next(iter(r.values())))
                for r in conn.execute("PRAGMA table_info(holdings)").fetchall()
            }
            if _is_sqlite(conn)
            else set()
        )
    except Exception:
        cols = set()
    if not cols and not _is_sqlite(conn):
        try:
            rows = conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'holdings'"
            ).fetchall()
            cols = {str(r[0]) for r in rows}
        except Exception:
            cols = set()
    if "superseded_at" in cols:
        return " AND h.superseded_at IS NULL"
    return ""


def resolve_held_issuers(conn: Any, manager_id: int | None = None) -> list[dict[str, Any]]:
    """
    Resolve distinct issuers from current holdings via resolved_ticker / cusip.
    """
    where = _holdings_current_clause(conn)
    params: tuple[Any, ...] = ()
    join = ""
    if manager_id is not None:
        join = " JOIN filings f ON f.filing_id = h.filing_id "
        where = f" AND f.manager_id = {'?' if _is_sqlite(conn) else '%s'}" + where
        params = (manager_id,)
    sql = (
        "SELECT DISTINCT "
        "COALESCE(NULLIF(h.resolved_ticker, ''), NULL) AS ticker, "
        "h.cusip AS cusip "
        f"FROM holdings h{join} "
        f"WHERE 1=1{where}"
    )
    rows = conn.execute(sql, params).fetchall()
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        ticker = row[0] if not isinstance(row, Mapping) else row.get("ticker")
        cusip = row[1] if not isinstance(row, Mapping) else row.get("cusip")
        key = str(ticker or cusip or "").strip().upper()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append({"ticker": ticker, "cusip": cusip, "issuer": ticker or cusip})
    return out


def ingest_insider_for_issuers(
    conn: Any,
    issuers: list[Mapping[str, Any]],
    *,
    lookback_days: int = 90,
    fetcher: Callable[..., list[Mapping[str, Any]]] | None = None,
    raise_on_fetch_error: bool = False,
) -> dict[str, Any]:
    """
    Pull Form-4 for each issuer and upsert.

    By default, per-issuer fetch failures are logged and skipped so conviction
    consumers are never blocked (finite/robust). Set raise_on_fetch_error=True
    only for deliberate-break tests.
    """
    ensure_insider_transactions_table(conn)
    inserted_total = 0
    errors: list[str] = []
    for issuer_row in issuers:
        issuer = str(issuer_row.get("issuer") or issuer_row.get("ticker") or "").strip()
        if not issuer:
            continue
        ticker = issuer_row.get("ticker")
        cusip = issuer_row.get("cusip")
        try:
            rows = fetch_form4_transactions(
                issuer,
                lookback_days=lookback_days,
                ticker=str(ticker) if ticker else None,
                fetcher=fetcher,
            )
            for row in rows:
                if cusip and not row.get("cusip"):
                    row["cusip"] = cusip
            inserted_total += upsert_insider_transactions(conn, rows)
        except Exception as exc:
            msg = f"{issuer}: {exc}"
            errors.append(msg)
            logger.warning("insider fetch failed for %s: %s", issuer, exc)
            if raise_on_fetch_error:
                raise InsiderFetchError(msg) from exc
    return {"inserted": inserted_total, "errors": errors, "issuer_count": len(issuers)}


def ingest_insider_for_manager(
    conn: Any,
    manager_id: int,
    *,
    lookback_days: int = 90,
    fetcher: Callable[..., list[Mapping[str, Any]]] | None = None,
    raise_on_fetch_error: bool = False,
) -> dict[str, Any]:
    """Resolve a manager's held issuers and ingest Form-4 rows for them."""
    issuers = resolve_held_issuers(conn, manager_id=manager_id)
    return ingest_insider_for_issuers(
        conn,
        issuers,
        lookback_days=lookback_days,
        fetcher=fetcher,
        raise_on_fetch_error=raise_on_fetch_error,
    )


def insider_net_direction_for_ticker(
    conn: Any,
    ticker: str | None,
    *,
    lookback_days: int = 90,
    cusip: str | None = None,
) -> str:
    """Compute net-direction annotation for one issuer key from stored rows."""
    ensure_insider_transactions_table(conn)
    if not ticker and not cusip:
        return "unknown"
    ph = "?" if _is_sqlite(conn) else "%s"
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    clauses: list[str] = []
    params: list[Any] = []
    if ticker:
        clauses.append(f"UPPER(COALESCE(ticker, '')) = UPPER({ph})")
        params.append(ticker)
    if cusip:
        clauses.append(f"cusip = {ph}")
        params.append(cusip)
    where = " OR ".join(clauses)
    params.append(cutoff)
    rows = conn.execute(
        "SELECT issuer_cik, ticker, insider_name, txn_code, shares, txn_date, acquired_disposed "
        f"FROM insider_transactions WHERE ({where}) AND (txn_date IS NULL OR txn_date >= {ph})",
        tuple(params),
    ).fetchall()
    mapped = []
    for row in rows:
        if isinstance(row, Mapping):
            mapped.append(row)
        else:
            mapped.append(
                {
                    "issuer_cik": row[0],
                    "ticker": row[1],
                    "insider_name": row[2],
                    "txn_code": row[3],
                    "shares": row[4],
                    "txn_date": row[5],
                    "acquired_disposed": row[6],
                }
            )
    return net_direction_for_rows(mapped)


def annotate_conviction_rows(
    conn: Any,
    conviction_rows: list[Mapping[str, Any]],
    *,
    lookback_days: int = 90,
) -> list[dict[str, Any]]:
    """Attach insider_net_direction to conviction-like dict rows without mutating scoring."""
    out: list[dict[str, Any]] = []
    for row in conviction_rows:
        item = dict(row)
        ticker = item.get("ticker") or item.get("resolved_ticker")
        cusip = item.get("cusip")
        item["insider_net_direction"] = insider_net_direction_for_ticker(
            conn,
            str(ticker) if ticker else None,
            lookback_days=lookback_days,
            cusip=str(cusip) if cusip else None,
        )
        out.append(item)
    return out
