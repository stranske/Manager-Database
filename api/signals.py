"""FastAPI endpoints and shared queries for conviction, crowded, and contrarian signals."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel

from adapters.base import connect_db

router = APIRouter()


class CrowdedTradeResponse(BaseModel):
    cusip: str
    name_of_issuer: str | None
    manager_count: int
    manager_names: list[str]
    total_value_usd: float | None
    avg_conviction_pct: float | None
    report_date: date


class ContrarianSignalResponse(BaseModel):
    manager_name: str | None
    cusip: str
    name_of_issuer: str | None
    direction: str
    consensus_direction: str
    delta_value: float | None
    consensus_count: int | None
    report_date: date


class ConvictionScoreResponse(BaseModel):
    cusip: str
    name_of_issuer: str | None
    value_usd: float | None
    conviction_pct: float | None
    portfolio_weight: float | None


def _is_sqlite(conn: Any) -> bool:
    return isinstance(conn, sqlite3.Connection)


def _placeholder(conn: Any) -> str:
    return "?" if _is_sqlite(conn) else "%s"


def _table_exists(conn: Any, table_name: str) -> bool:
    if _is_sqlite(conn):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None
    row = conn.execute("SELECT to_regclass(%s)", (table_name,)).fetchone()
    return bool(row and row[0])


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value))


def _resolve_latest_report_date(conn: Any, table_name: str) -> date | None:
    if not _table_exists(conn, table_name):
        return None
    row = conn.execute(f"SELECT MAX(report_date) FROM {table_name}").fetchone()
    if not row or row[0] is None:
        return None
    return _to_date(row[0])


def _resolve_latest_manager_filing_id(conn: Any, manager_id: int) -> int | None:
    if not _table_exists(conn, "filings"):
        return None
    ph = _placeholder(conn)
    row = conn.execute(
        "SELECT filing_id "
        "FROM filings "
        f"WHERE manager_id = {ph} "
        "ORDER BY COALESCE(filed_date, period_end) DESC, filing_id DESC "
        "LIMIT 1",
        (manager_id,),
    ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _parse_manager_ids(value: Any) -> list[int]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        parsed = []
        for item in value:
            try:
                parsed.append(int(item))
            except (TypeError, ValueError):
                continue
        return parsed

    raw = str(value).strip()
    if not raw:
        return []
    if raw.startswith("{") and raw.endswith("}"):
        raw = raw[1:-1]
        parts = [token.strip() for token in raw.split(",") if token.strip()]
        return [int(token) for token in parts if token.isdigit()]
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if isinstance(loaded, list):
        parsed = []
        for item in loaded:
            if not isinstance(item, (int, float, str)):
                continue
            try:
                parsed.append(int(item))
            except (TypeError, ValueError):
                continue
        return parsed
    return []


def _load_manager_latest_holdings_cusips(
    conn: Any, manager_id: int, report_date: date
) -> set[str] | None:
    if not (_table_exists(conn, "filings") and _table_exists(conn, "holdings")):
        return None

    ph = _placeholder(conn)
    latest_filing = conn.execute(
        "SELECT filing_id "
        "FROM filings "
        f"WHERE manager_id = {ph} AND COALESCE(period_end, filed_date) <= {ph} "
        "ORDER BY COALESCE(period_end, filed_date) DESC, filed_date DESC, filing_id DESC "
        "LIMIT 1",
        (manager_id, report_date),
    ).fetchone()
    if not latest_filing or latest_filing[0] is None:
        return set()

    rows = conn.execute(
        f"SELECT cusip FROM holdings WHERE filing_id = {ph}",
        (latest_filing[0],),
    ).fetchall()
    return {str(row[0]) for row in rows if row[0]}


def _manager_id_column(conn: Any) -> str | None:
    if not _table_exists(conn, "managers"):
        return None
    manager_id_column = "manager_id"
    if _is_sqlite(conn):
        rows = conn.execute("PRAGMA table_info(managers)").fetchall()
        columns = {str(row[1]) for row in rows}
        if "manager_id" not in columns and "id" in columns:
            manager_id_column = "id"
        elif "manager_id" not in columns:
            return None
    return manager_id_column


def _manager_name_lookup(conn: Any) -> dict[int, str]:
    manager_id_column = _manager_id_column(conn)
    if manager_id_column is None:
        return {}
    rows = conn.execute(f"SELECT {manager_id_column}, name FROM managers").fetchall()
    lookup: dict[int, str] = {}
    for row in rows:
        if row[0] is None or not row[1]:
            continue
        try:
            lookup[int(row[0])] = str(row[1])
        except (TypeError, ValueError):
            continue
    return lookup


# Keep these queries in the API layer so Streamlit can reuse the same semantics.
def query_crowded_trades(
    conn: Any,
    *,
    report_date: date | None = None,
    manager_id: int | None = None,
    min_managers: int = 3,
    limit: int = 50,
) -> list[CrowdedTradeResponse]:
    if not _table_exists(conn, "crowded_trades"):
        return []

    resolved_date = report_date or _resolve_latest_report_date(conn, "crowded_trades")
    if resolved_date is None:
        return []

    ph = _placeholder(conn)
    params: list[Any] = [resolved_date, max(1, min_managers)]
    rows = conn.execute(
        "SELECT ct.cusip, ct.name_of_issuer, ct.manager_count, ct.manager_ids, "
        "ct.total_value_usd, ct.avg_conviction_pct, ct.report_date "
        "FROM crowded_trades ct "
        f"WHERE ct.report_date = {ph} AND ct.manager_count >= {ph} "
        "ORDER BY ct.manager_count DESC, ct.total_value_usd DESC, ct.cusip ASC ",
        tuple(params),
    ).fetchall()
    manager_names = _manager_name_lookup(conn)
    held_cusips = (
        _load_manager_latest_holdings_cusips(conn, manager_id, resolved_date)
        if manager_id is not None
        else None
    )
    crowded_rows: list[CrowdedTradeResponse] = []
    for row in rows:
        cusip = str(row[0])
        manager_ids = _parse_manager_ids(row[3])
        if manager_id is not None:
            if held_cusips is not None:
                if cusip not in held_cusips:
                    continue
            elif manager_id not in manager_ids:
                continue

        crowded_rows.append(
            CrowdedTradeResponse(
                cusip=cusip,
                name_of_issuer=str(row[1]) if row[1] is not None else None,
                manager_count=int(row[2]),
                manager_names=[
                    manager_names[manager_ref]
                    for manager_ref in manager_ids
                    if manager_ref in manager_names
                ],
                total_value_usd=_to_float(row[4]),
                avg_conviction_pct=_to_float(row[5]),
                report_date=_to_date(row[6]),
            )
        )
        if len(crowded_rows) >= limit:
            break
    return crowded_rows


def query_contrarian_signals(
    conn: Any,
    *,
    report_date: date | None = None,
    manager_id: int | None = None,
    limit: int = 50,
) -> list[ContrarianSignalResponse]:
    if not _table_exists(conn, "contrarian_signals"):
        return []

    resolved_date = report_date or _resolve_latest_report_date(conn, "contrarian_signals")
    if resolved_date is None:
        return []

    ph = _placeholder(conn)
    manager_id_column = _manager_id_column(conn)
    manager_join = (
        f"LEFT JOIN managers m ON m.{manager_id_column} = cs.manager_id"
        if manager_id_column is not None
        else ""
    )
    filters = [f"cs.report_date = {ph}"]
    params: list[Any] = [resolved_date]
    if manager_id is not None:
        filters.append(f"cs.manager_id = {ph}")
        params.append(manager_id)
    params.append(limit)
    rows = conn.execute(
        "SELECT m.name, cs.cusip, cs.name_of_issuer, cs.direction, cs.consensus_direction, "
        "cs.manager_delta_value, cs.consensus_count, cs.report_date "
        "FROM contrarian_signals cs "
        f"{manager_join} "
        f"WHERE {' AND '.join(filters)} "
        "ORDER BY cs.consensus_count DESC, ABS(COALESCE(cs.manager_delta_value, 0)) DESC, cs.cusip ASC "
        f"LIMIT {ph}",
        tuple(params),
    ).fetchall()
    return [
        ContrarianSignalResponse(
            manager_name=str(row[0]) if row[0] is not None else None,
            cusip=str(row[1]),
            name_of_issuer=str(row[2]) if row[2] is not None else None,
            direction=str(row[3]),
            consensus_direction=str(row[4]),
            delta_value=_to_float(row[5]),
            consensus_count=int(row[6]) if row[6] is not None else None,
            report_date=_to_date(row[7]),
        )
        for row in rows
    ]


def query_conviction_scores(
    conn: Any,
    manager_id: int,
    *,
    filing_id: int | None = None,
    min_conviction_pct: float = 0.0,
    limit: int = 100,
) -> list[ConvictionScoreResponse]:
    if not _table_exists(conn, "conviction_scores"):
        return []

    resolved_filing_id = filing_id or _resolve_latest_manager_filing_id(conn, manager_id)
    if resolved_filing_id is None:
        return []

    ph = _placeholder(conn)
    rows = conn.execute(
        "SELECT cusip, name_of_issuer, value_usd, conviction_pct, portfolio_weight "
        "FROM conviction_scores "
        f"WHERE manager_id = {ph} AND filing_id = {ph} AND COALESCE(conviction_pct, 0) >= {ph} "
        "ORDER BY conviction_pct DESC, value_usd DESC, cusip ASC "
        f"LIMIT {ph}",
        (manager_id, resolved_filing_id, min_conviction_pct, limit),
    ).fetchall()
    return [
        ConvictionScoreResponse(
            cusip=str(row[0]),
            name_of_issuer=str(row[1]) if row[1] is not None else None,
            value_usd=_to_float(row[2]),
            conviction_pct=_to_float(row[3]),
            portfolio_weight=_to_float(row[4]),
        )
        for row in rows
    ]


@router.get(
    "/api/signals/crowded",
    response_model=list[CrowdedTradeResponse],
    summary="List crowded trades",
)
async def get_crowded_trades(
    report_date: date | None = None,
    manager_id: int | None = None,
    min_managers: int = Query(3, ge=1),
    limit: int = Query(50, ge=1, le=500),
) -> list[CrowdedTradeResponse]:
    conn = connect_db()
    try:
        return query_crowded_trades(
            conn,
            report_date=report_date,
            manager_id=manager_id,
            min_managers=min_managers,
            limit=limit,
        )
    finally:
        conn.close()


@router.get(
    "/api/signals/contrarian",
    response_model=list[ContrarianSignalResponse],
    summary="List contrarian signals",
)
async def get_contrarian_signals(
    report_date: date | None = None,
    manager_id: int | None = None,
    limit: int = Query(50, ge=1, le=500),
) -> list[ContrarianSignalResponse]:
    conn = connect_db()
    try:
        return query_contrarian_signals(
            conn,
            report_date=report_date,
            manager_id=manager_id,
            limit=limit,
        )
    finally:
        conn.close()


@router.get(
    "/api/signals/conviction/{manager_id}",
    response_model=list[ConvictionScoreResponse],
    summary="List conviction scores for a manager",
)
async def get_conviction_scores(
    manager_id: int,
    filing_id: int | None = None,
    min_conviction_pct: float = Query(0.0, ge=0.0),
    limit: int = Query(100, ge=1, le=500),
) -> list[ConvictionScoreResponse]:
    conn = connect_db()
    try:
        return query_conviction_scores(
            conn,
            manager_id,
            filing_id=filing_id,
            min_conviction_pct=min_conviction_pct,
            limit=limit,
        )
    finally:
        conn.close()
