"""ETL flow: ingest Form-4 insider transactions and annotate conviction rows."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import date, timedelta
from typing import Any

from adapters.base import connect_db, is_sqlite
from adapters.insider import (
    InsiderFetchError,
    fetch_form4_transactions,
    net_direction_for_rows,
)
from etl.point_in_time import holdings_visibility_sql

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


def ensure_insider_transactions_table(conn: Any) -> None:
    """Create insider_transactions when missing (SQLite bootstrap / tests)."""
    ddl = DDL_SQLITE if is_sqlite(conn) else DDL_PG
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
        if is_sqlite(conn)
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


def resolve_held_issuers(
    conn: Any,
    manager_id: int | None = None,
    *,
    as_of: date | None = None,
) -> list[dict[str, Any]]:
    """
    Resolve distinct issuers from current holdings via resolved_ticker / cusip.
    """
    visibility, vis_params = holdings_visibility_sql(conn, as_of)
    params: list[Any] = list(vis_params)
    join = ""
    manager_filter = ""
    if manager_id is not None:
        join = " JOIN filings f ON f.filing_id = h.filing_id "
        ph = "?" if is_sqlite(conn) else "%s"
        manager_filter = f" AND f.manager_id = {ph}"
        params.append(manager_id)
    sql = (
        "SELECT DISTINCT "
        "COALESCE(NULLIF(h.resolved_ticker, ''), NULL) AS ticker, "
        "h.cusip AS cusip "
        f"FROM holdings h{join} "
        f"WHERE {visibility}{manager_filter}"
    )
    rows = conn.execute(sql, tuple(params)).fetchall()
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
    issuers: Sequence[Mapping[str, Any]],
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
        except Exception as exc:
            msg = f"{issuer}: {exc}"
            errors.append(msg)
            logger.warning("insider fetch failed for %s: %s", issuer, exc)
            if raise_on_fetch_error:
                raise InsiderFetchError(msg) from exc
            continue
        for row in rows:
            if cusip and not row.get("cusip"):
                row["cusip"] = cusip
        inserted_total += upsert_insider_transactions(conn, rows)
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
    ph = "?" if is_sqlite(conn) else "%s"
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


SUMMARY_KEYS = (
    "issuers_resolved",
    "filings_scanned",
    "rows_inserted",
    "unmatched_issuers",
    "skipped_transaction_codes",
    "fetch_errors",
)


def build_run_summary(
    issuers: Sequence[Mapping[str, Any]],
    ingest_result: Mapping[str, Any],
) -> dict[str, int | None]:
    """Assemble the local Form-4 CLI run summary (#1917)."""
    return {
        "issuers_resolved": len(issuers),
        "filings_scanned": None,
        "rows_inserted": int(ingest_result.get("inserted") or 0),
        "unmatched_issuers": None,
        "skipped_transaction_codes": None,
        "fetch_errors": len(ingest_result.get("errors") or []),
    }


def run_form4_ingest(
    conn: Any,
    *,
    as_of: date,
    lookback_days: int = 90,
    fetcher: Callable[..., list[Mapping[str, Any]]] | None = None,
) -> dict[str, int | None]:
    """Resolve held issuers as-of ``as_of`` and ingest Form-4 rows."""
    issuers = resolve_held_issuers(conn, as_of=as_of)
    ingest_result = ingest_insider_for_issuers(
        conn,
        issuers,
        lookback_days=lookback_days,
        fetcher=fetcher,
    )
    return build_run_summary(issuers, ingest_result)


def _format_summary_value(value: int | None) -> str:
    return "null" if value is None else str(value)


def print_run_summary(summary: Mapping[str, int | None], *, as_json: bool = False) -> None:
    if as_json:
        print(json.dumps(dict(summary), indent=2, sort_keys=True))
        return
    for key in SUMMARY_KEYS:
        print(f"{key}: {_format_summary_value(summary.get(key))}")


def main(argv: list[str] | None = None) -> int:
    """Local entry point: ``python -m etl.insider_flow --as-of YYYY-MM-DD``."""
    parser = argparse.ArgumentParser(description="Ingest Form-4 insider transactions")
    parser.add_argument(
        "--as-of",
        required=True,
        type=date.fromisoformat,
        help="Holdings as-of date (YYYY-MM-DD)",
    )
    parser.add_argument("--lookback-days", type=int, default=90)
    parser.add_argument("--json", action="store_true", help="Emit summary as JSON")
    args = parser.parse_args(argv)
    conn = connect_db()
    try:
        summary = run_form4_ingest(
            conn,
            as_of=args.as_of,
            lookback_days=args.lookback_days,
        )
    finally:
        conn.close()
    print_run_summary(summary, as_json=args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
