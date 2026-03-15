"""FastAPI endpoints and shared queries for activism filings and events."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from adapters.base import connect_db

router = APIRouter()


class ActivismFilingResponse(BaseModel):
    filing_id: int
    manager_name: str | None
    filing_type: str
    subject_company: str
    subject_cusip: str | None
    ownership_pct: float | None
    shares: int | None
    filed_date: date
    url: str | None


class ActivismEventResponse(BaseModel):
    event_id: int
    manager_name: str | None
    event_type: str
    subject_company: str
    ownership_pct: float | None
    previous_pct: float | None
    delta_pct: float | None
    threshold_crossed: float | None
    detected_at: datetime


class ActivismTimelineEntry(BaseModel):
    date: date
    type: str
    description: str
    ownership_pct: float | None
    event_types: list[str] = Field(default_factory=list)


class ActiveCampaignResponse(BaseModel):
    manager_name: str | None
    subject_company: str
    cusip: str | None
    current_ownership_pct: float | None
    latest_filing_date: date
    event_count: int
    latest_event_type: str | None


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


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value))


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return datetime.fromisoformat(text)


# Keep the SQL-building logic in one place so API and Streamlit views stay aligned.
def query_activism_filings(
    conn: Any,
    *,
    manager_id: int | None = None,
    cusip: str | None = None,
    filing_type: str | None = None,
    since: date | None = None,
    limit: int = 100,
) -> list[ActivismFilingResponse]:
    if not _table_exists(conn, "activism_filings"):
        return []

    ph = _placeholder(conn)
    filters: list[str] = []
    params: list[Any] = []

    if manager_id is not None:
        filters.append(f"af.manager_id = {ph}")
        params.append(manager_id)
    if cusip:
        filters.append(f"upper(COALESCE(af.subject_cusip, '')) = upper({ph})")
        params.append(cusip)
    if filing_type:
        filters.append(f"af.filing_type = {ph}")
        params.append(filing_type)
    if since is not None:
        filters.append(f"af.filed_date >= {ph}")
        params.append(since)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    params.append(limit)
    rows = conn.execute(
        "SELECT af.filing_id, m.name, af.filing_type, af.subject_company, af.subject_cusip, "
        "af.ownership_pct, af.shares, af.filed_date, af.url "
        "FROM activism_filings af "
        "LEFT JOIN managers m ON m.manager_id = af.manager_id "
        f"{where_clause} "
        "ORDER BY af.filed_date DESC, af.filing_id DESC "
        f"LIMIT {ph}",
        tuple(params),
    ).fetchall()
    return [
        ActivismFilingResponse(
            filing_id=int(row[0]),
            manager_name=str(row[1]) if row[1] is not None else None,
            filing_type=str(row[2]),
            subject_company=str(row[3]),
            subject_cusip=str(row[4]) if row[4] is not None else None,
            ownership_pct=_to_float(row[5]),
            shares=_to_int(row[6]),
            filed_date=_to_date(row[7]),
            url=str(row[8]) if row[8] is not None else None,
        )
        for row in rows
    ]


def query_activism_events(
    conn: Any,
    *,
    manager_id: int | None = None,
    event_type: str | None = None,
    cusip: str | None = None,
    since: date | None = None,
    limit: int = 100,
) -> list[ActivismEventResponse]:
    if not _table_exists(conn, "activism_events"):
        return []

    ph = _placeholder(conn)
    filters: list[str] = []
    params: list[Any] = []

    if manager_id is not None:
        filters.append(f"ae.manager_id = {ph}")
        params.append(manager_id)
    if event_type:
        filters.append(f"ae.event_type = {ph}")
        params.append(event_type)
    if cusip:
        filters.append(f"upper(COALESCE(ae.subject_cusip, '')) = upper({ph})")
        params.append(cusip)
    if since is not None:
        if _is_sqlite(conn):
            filters.append(f"date(ae.detected_at) >= date({ph})")
        else:
            filters.append(f"ae.detected_at::date >= {ph}")
        params.append(since)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    params.append(limit)
    rows = conn.execute(
        "SELECT ae.event_id, m.name, ae.event_type, ae.subject_company, ae.ownership_pct, "
        "ae.previous_pct, ae.delta_pct, ae.threshold_crossed, ae.detected_at "
        "FROM activism_events ae "
        "LEFT JOIN managers m ON m.manager_id = ae.manager_id "
        f"{where_clause} "
        "ORDER BY ae.detected_at DESC, ae.event_id DESC "
        f"LIMIT {ph}",
        tuple(params),
    ).fetchall()
    return [
        ActivismEventResponse(
            event_id=int(row[0]),
            manager_name=str(row[1]) if row[1] is not None else None,
            event_type=str(row[2]),
            subject_company=str(row[3]),
            ownership_pct=_to_float(row[4]),
            previous_pct=_to_float(row[5]),
            delta_pct=_to_float(row[6]),
            threshold_crossed=_to_float(row[7]),
            detected_at=_to_datetime(row[8]),
        )
        for row in rows
    ]


def query_activism_timeline(conn: Any, manager_id: int) -> list[ActivismTimelineEntry]:
    if not _table_exists(conn, "activism_filings"):
        return []

    ph = _placeholder(conn)
    event_cte = (
        ", event_entries AS ("
        "    SELECT "
        + ("date(ae.detected_at)" if _is_sqlite(conn) else "ae.detected_at::date")
        + " AS entry_date, 'event' AS entry_type, "
        "           ae.event_type || ' on ' || ae.subject_company || "
        "           CASE WHEN ae.threshold_crossed IS NOT NULL THEN ' (threshold ' || ae.threshold_crossed || '%)' ELSE '' END AS description, "
        "           ae.ownership_pct AS ownership_pct, ae.event_type AS event_type, ae.event_id AS sort_id "
        "    FROM activism_events ae "
        f"    WHERE ae.manager_id = {ph} "
        ") "
    )
    union_source = "SELECT * FROM filing_entries"
    params: tuple[Any, ...]
    if _table_exists(conn, "activism_events"):
        union_source = "SELECT * FROM filing_entries UNION ALL SELECT * FROM event_entries"
        params = (manager_id, manager_id)
    else:
        event_cte = ""
        params = (manager_id,)

    rows = conn.execute(
        "WITH filing_entries AS ("
        "    SELECT af.filed_date AS entry_date, 'filing' AS entry_type, "
        "           'Filed ' || af.filing_type || ' for ' || af.subject_company || "
        "           CASE WHEN af.ownership_pct IS NOT NULL THEN ' at ' || af.ownership_pct || '% ownership' ELSE '' END AS description, "
        "           af.ownership_pct AS ownership_pct, '' AS event_type, af.filing_id AS sort_id "
        "    FROM activism_filings af "
        f"    WHERE af.manager_id = {ph} "
        ") " + event_cte + "SELECT entry_date, entry_type, description, ownership_pct, event_type "
        f"FROM ({union_source}) timeline "
        "ORDER BY entry_date ASC, entry_type ASC, sort_id ASC",
        params,
    ).fetchall()
    return [
        ActivismTimelineEntry(
            date=_to_date(row[0]),
            type=str(row[1]),
            description=str(row[2]),
            ownership_pct=_to_float(row[3]),
            event_types=[str(row[4])] if row[4] else [],
        )
        for row in rows
    ]


def query_active_campaigns(
    conn: Any,
    *,
    min_ownership_pct: float = 5.0,
    limit: int = 100,
) -> list[ActiveCampaignResponse]:
    if not _table_exists(conn, "activism_filings"):
        return []

    ph = _placeholder(conn)
    if _table_exists(conn, "activism_events"):
        event_count_sql = (
            "COALESCE((SELECT COUNT(*) FROM activism_events ae "
            "WHERE ae.manager_id = ranked.manager_id "
            "AND COALESCE(ae.subject_cusip, '') = COALESCE(ranked.subject_cusip, '') "
            "AND ae.subject_company = ranked.subject_company), 0)"
        )
        latest_event_sql = (
            "(SELECT ae.event_type FROM activism_events ae "
            "WHERE ae.manager_id = ranked.manager_id "
            "AND COALESCE(ae.subject_cusip, '') = COALESCE(ranked.subject_cusip, '') "
            "AND ae.subject_company = ranked.subject_company "
            "ORDER BY ae.detected_at DESC, ae.event_id DESC LIMIT 1)"
        )
    else:
        event_count_sql = "0"
        latest_event_sql = "NULL"
    rows = conn.execute(
        "WITH ranked AS ("
        "    SELECT af.filing_id, af.manager_id, af.subject_company, af.subject_cusip, "
        "           af.ownership_pct, af.filed_date, m.name AS manager_name, "
        "           ROW_NUMBER() OVER ("
        "               PARTITION BY af.manager_id, COALESCE(af.subject_cusip, af.subject_company) "
        "               ORDER BY af.filed_date DESC, af.filing_id DESC"
        "           ) AS row_number "
        "    FROM activism_filings af "
        "    LEFT JOIN managers m ON m.manager_id = af.manager_id"
        ") "
        "SELECT ranked.manager_name, ranked.subject_company, ranked.subject_cusip, "
        "       ranked.ownership_pct, ranked.filed_date, "
        f"       {event_count_sql} AS event_count, "
        f"       {latest_event_sql} AS latest_event_type "
        "FROM ranked "
        "WHERE ranked.row_number = 1 AND COALESCE(ranked.ownership_pct, 0) >= "
        f"{ph} "
        "ORDER BY ranked.filed_date DESC, ranked.ownership_pct DESC "
        f"LIMIT {ph}",
        (min_ownership_pct, limit),
    ).fetchall()
    return [
        ActiveCampaignResponse(
            manager_name=str(row[0]) if row[0] is not None else None,
            subject_company=str(row[1]),
            cusip=str(row[2]) if row[2] is not None else None,
            current_ownership_pct=_to_float(row[3]),
            latest_filing_date=_to_date(row[4]),
            event_count=int(row[5] or 0),
            latest_event_type=str(row[6]) if row[6] is not None else None,
        )
        for row in rows
    ]


@router.get(
    "/api/activism/filings",
    response_model=list[ActivismFilingResponse],
    summary="List activism filings",
)
async def list_activism_filings(
    manager_id: int | None = None,
    cusip: str | None = None,
    filing_type: str | None = None,
    since: date | None = None,
    limit: int = Query(100, ge=1, le=500),
) -> list[ActivismFilingResponse]:
    conn = connect_db()
    try:
        return query_activism_filings(
            conn,
            manager_id=manager_id,
            cusip=cusip,
            filing_type=filing_type,
            since=since,
            limit=limit,
        )
    finally:
        conn.close()


@router.get(
    "/api/activism/events",
    response_model=list[ActivismEventResponse],
    summary="List activism events",
)
async def list_activism_events(
    manager_id: int | None = None,
    event_type: str | None = None,
    cusip: str | None = None,
    since: date | None = None,
    limit: int = Query(100, ge=1, le=500),
) -> list[ActivismEventResponse]:
    conn = connect_db()
    try:
        return query_activism_events(
            conn,
            manager_id=manager_id,
            event_type=event_type,
            cusip=cusip,
            since=since,
            limit=limit,
        )
    finally:
        conn.close()


@router.get(
    "/api/activism/timeline/{manager_id}",
    response_model=list[ActivismTimelineEntry],
    summary="Return a manager activism timeline",
)
async def activism_timeline(manager_id: int) -> list[ActivismTimelineEntry]:
    conn = connect_db()
    try:
        return query_activism_timeline(conn, manager_id)
    finally:
        conn.close()


@router.get(
    "/api/activism/active-campaigns",
    response_model=list[ActiveCampaignResponse],
    summary="List active activism campaigns",
)
async def active_campaigns(
    min_ownership_pct: float = Query(5.0, ge=0.0),
    limit: int = Query(100, ge=1, le=500),
) -> list[ActiveCampaignResponse]:
    conn = connect_db()
    try:
        return query_active_campaigns(conn, min_ownership_pct=min_ownership_pct, limit=limit)
    finally:
        conn.close()
