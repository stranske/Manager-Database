"""FastAPI endpoints and shared queries for conviction, crowded, and contrarian signals."""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from typing import Any

try:
    from fastapi import APIRouter, Query
    from pydantic import BaseModel
except ModuleNotFoundError:
    os.environ.setdefault("UI_OFFLINE", "1")
    from api._compat import offline_api_imports

    APIRouter, BaseModel, _Field, Query = offline_api_imports()

from adapters.base import (
    connect_db,
    get_placeholder,
    table_exists,
)
from adapters.base import (
    manager_id_column as shared_manager_id_column,
)

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
    insider_net_direction: str | None = None
    short_interest_pct: float | None = None
    short_interest_report_date: date | None = None
    short_interest_source: str | None = None


class AttributionPositionResponse(BaseModel):
    filing_id: int | None
    disclosure_date: date
    as_of_date: date
    security_key: str
    ticker: str | None
    cusip: str | None
    name_of_issuer: str | None
    position_return: float | None
    value_usd: float | None
    status: str
    skip_reason: str | None = None


class ManagerAttributionResponse(BaseModel):
    manager_id: int
    as_of_date: date | None
    positions: int
    positions_skipped: int
    realized_return: float | None
    hit_rate: float | None
    rows: list[AttributionPositionResponse]


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
    if not table_exists(conn, table_name):
        return None
    row = conn.execute(f"SELECT MAX(report_date) FROM {table_name}").fetchone()
    if not row or row[0] is None:
        return None
    return _to_date(row[0])


def _resolve_latest_manager_filing_id(conn: Any, manager_id: int) -> int | None:
    if not table_exists(conn, "filings"):
        return None
    ph = get_placeholder(conn)
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


def _manager_id_column(conn: Any) -> str | None:
    return shared_manager_id_column(conn, require_table=True)


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
    if not table_exists(conn, "crowded_trades"):
        return []

    resolved_date = report_date or _resolve_latest_report_date(conn, "crowded_trades")
    if resolved_date is None:
        return []

    ph = get_placeholder(conn)
    params: list[Any] = [resolved_date, max(1, min_managers)]
    manager_filter = ""
    if manager_id is not None:
        params.extend([manager_id, resolved_date])
        manager_filter = (
            "AND EXISTS ("
            "    WITH ranked_filings AS ("
            "        SELECT filing_id, ROW_NUMBER() OVER ("
            "            PARTITION BY manager_id "
            "            ORDER BY COALESCE(filed_date, period_end) DESC, filing_id DESC"
            "        ) AS rn "
            "        FROM filings "
            f"        WHERE manager_id = {ph} AND COALESCE(filed_date, period_end) <= {ph}"
            "    ) "
            "    SELECT 1 FROM ranked_filings rf "
            "    JOIN holdings h ON h.filing_id = rf.filing_id "
            "    WHERE rf.rn = 1 AND h.cusip = ct.cusip"
            ") "
        )
    params.append(limit)
    rows = conn.execute(
        "SELECT ct.cusip, ct.name_of_issuer, ct.manager_count, ct.manager_ids, "
        "ct.total_value_usd, ct.avg_conviction_pct, ct.report_date "
        "FROM crowded_trades ct "
        f"WHERE ct.report_date = {ph} AND ct.manager_count >= {ph} "
        f"{manager_filter}"
        "ORDER BY ct.manager_count DESC, ct.total_value_usd DESC, ct.cusip ASC "
        f"LIMIT {ph}",
        tuple(params),
    ).fetchall()
    manager_names = _manager_name_lookup(conn)
    return [
        CrowdedTradeResponse(
            cusip=str(row[0]),
            name_of_issuer=str(row[1]) if row[1] is not None else None,
            manager_count=int(row[2]),
            manager_names=[
                manager_names[manager_ref]
                for manager_ref in _parse_manager_ids(row[3])
                if manager_ref in manager_names
            ],
            total_value_usd=_to_float(row[4]),
            avg_conviction_pct=_to_float(row[5]),
            report_date=_to_date(row[6]),
        )
        for row in rows
    ]


def query_contrarian_signals(
    conn: Any,
    *,
    report_date: date | None = None,
    manager_id: int | None = None,
    limit: int = 50,
) -> list[ContrarianSignalResponse]:
    if not table_exists(conn, "contrarian_signals"):
        return []

    resolved_date = report_date or _resolve_latest_report_date(conn, "contrarian_signals")
    if resolved_date is None:
        return []

    ph = get_placeholder(conn)
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
    if not table_exists(conn, "conviction_scores"):
        return []

    resolved_filing_id = filing_id or _resolve_latest_manager_filing_id(conn, manager_id)
    if resolved_filing_id is None:
        return []

    ph = get_placeholder(conn)
    rows = conn.execute(
        "SELECT cusip, name_of_issuer, value_usd, conviction_pct, portfolio_weight "
        "FROM conviction_scores "
        f"WHERE manager_id = {ph} AND filing_id = {ph} AND COALESCE(conviction_pct, 0) >= {ph} "
        "ORDER BY conviction_pct DESC, value_usd DESC, cusip ASC "
        f"LIMIT {ph}",
        (manager_id, resolved_filing_id, min_conviction_pct, limit),
    ).fetchall()

    # Optional external annotations — never change conviction scoring (#1461, #1470).
    annotate_insider = None
    annotate_short_interest = None
    if table_exists(conn, "insider_transactions"):
        from etl.insider_flow import insider_net_direction_for_ticker

        annotate_insider = insider_net_direction_for_ticker
    if table_exists(conn, "short_interest"):
        from etl.short_interest_flow import short_interest_annotation

        annotate_short_interest = short_interest_annotation

    ticker_by_cusip: dict[str, str | None] = {}
    if annotate_insider is not None or annotate_short_interest is not None:
        try:
            hold_rows = conn.execute(
                f"SELECT cusip, resolved_ticker FROM holdings WHERE filing_id = {ph}",
                (resolved_filing_id,),
            ).fetchall()
            for hold in hold_rows:
                cusip = str(hold[0])
                ticker_by_cusip[cusip] = str(hold[1]) if hold[1] is not None else None
        except Exception:
            ticker_by_cusip = {}

    out: list[ConvictionScoreResponse] = []
    for row in rows:
        cusip = str(row[0])
        direction = None
        if annotate_insider is not None:
            direction = annotate_insider(
                conn,
                ticker_by_cusip.get(cusip),
                cusip=cusip,
            )
        short_interest = (
            annotate_short_interest(conn, ticker_by_cusip.get(cusip), cusip=cusip)
            if annotate_short_interest is not None
            else None
        )
        out.append(
            ConvictionScoreResponse(
                cusip=cusip,
                name_of_issuer=str(row[1]) if row[1] is not None else None,
                value_usd=_to_float(row[2]),
                conviction_pct=_to_float(row[3]),
                portfolio_weight=_to_float(row[4]),
                insider_net_direction=direction,
                short_interest_pct=(
                    _to_float(short_interest.get("short_interest_pct")) if short_interest else None
                ),
                short_interest_report_date=(
                    _to_date(short_interest["short_interest_report_date"])
                    if short_interest and short_interest.get("short_interest_report_date")
                    else None
                ),
                short_interest_source=(
                    str(short_interest["short_interest_source"])
                    if short_interest and short_interest.get("short_interest_source")
                    else None
                ),
            )
        )
    return out


@router.get(
    "/api/signals/crowded",
    response_model=list[CrowdedTradeResponse],
    summary="List crowded trades",
)
async def get_crowded_trades(
    report_date: date | None = None,
    min_managers: int = Query(3, ge=1),
    limit: int = Query(50, ge=1, le=500),
) -> list[CrowdedTradeResponse]:
    conn = connect_db()
    try:
        return query_crowded_trades(
            conn,
            report_date=report_date,
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


@router.get(
    "/api/signals/attribution/{manager_id}",
    response_model=ManagerAttributionResponse,
    summary="List position attribution for a manager",
)
async def get_manager_attribution(
    manager_id: int,
    as_of_date: date | None = None,
    limit: int = Query(200, ge=1, le=1000),
) -> ManagerAttributionResponse:
    """Read-only derived returns since disclosure. Raw prices are not redistributed."""
    from etl.attribution_flow import (
        PositionAttribution,
        query_manager_attribution,
        summarize_manager_attribution,
    )

    conn = connect_db()
    try:
        rows = query_manager_attribution(
            conn,
            manager_id,
            as_of_date=as_of_date,
            limit=limit,
        )
    finally:
        conn.close()

    positions = [
        PositionAttribution(
            manager_id=int(row["manager_id"]),
            filing_id=int(row["filing_id"]) if row.get("filing_id") is not None else None,
            disclosure_date=_coerce_date_value(row["disclosure_date"]),
            as_of_date=_coerce_date_value(row["as_of_date"]),
            security_key=str(row["security_key"]),
            ticker=str(row["ticker"]) if row.get("ticker") else None,
            cusip=str(row["cusip"]) if row.get("cusip") else None,
            name_of_issuer=str(row["name_of_issuer"]) if row.get("name_of_issuer") else None,
            value_usd=_to_float(row.get("value_usd")),
            position_return=_to_float(row.get("position_return")),
            status=str(row.get("status") or "filled"),
            skip_reason=str(row["skip_reason"]) if row.get("skip_reason") else None,
        )
        for row in rows
    ]
    summary = summarize_manager_attribution(positions)
    return ManagerAttributionResponse(
        manager_id=manager_id,
        as_of_date=as_of_date or (positions[0].as_of_date if positions else None),
        positions=int(summary["positions"]),
        positions_skipped=int(summary["positions_skipped"]),
        realized_return=summary["realized_return"],
        hit_rate=summary["hit_rate"],
        rows=[
            AttributionPositionResponse(
                filing_id=p.filing_id,
                disclosure_date=p.disclosure_date,
                as_of_date=p.as_of_date,
                security_key=p.security_key,
                ticker=p.ticker,
                cusip=p.cusip,
                name_of_issuer=p.name_of_issuer,
                position_return=p.position_return,
                value_usd=p.value_usd,
                status=p.status,
                skip_reason=p.skip_reason,
            )
            for p in positions
        ],
    )


def _coerce_date_value(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])
