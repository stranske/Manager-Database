"""Point-in-time holdings helpers (bitemporal as-of + current view).

Implements Manager-Database #1463 / design #1400.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from adapters.base import get_placeholder, get_table_columns


def _normalize_as_of(as_of: date | datetime) -> datetime:
    if isinstance(as_of, datetime):
        if as_of.tzinfo is None:
            return as_of.replace(tzinfo=UTC)
        return as_of.astimezone(UTC)
    return datetime(as_of.year, as_of.month, as_of.day, tzinfo=UTC)


def _as_of_literal(as_of: datetime) -> str:
    return as_of.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def current_holdings_sql(table_alias: str = "h") -> str:
    """SQL predicate selecting non-superseded holdings rows."""
    return f"{table_alias}.superseded_at IS NULL"


def current_holdings(
    conn: Any,
    *,
    manager_id: int | None = None,
    filing_id: int | None = None,
) -> list[dict[str, Any]]:
    """Return current (non-superseded) holdings, optionally filtered."""
    columns = get_table_columns(conn, "holdings")
    if "holding_id" not in columns and "id" not in columns:
        return []
    marker = get_placeholder(conn)
    where: list[str] = []
    params: list[Any] = []
    if "superseded_at" in columns:
        where.append("h.superseded_at IS NULL")
    join_filings = manager_id is not None
    if filing_id is not None:
        where.append(f"h.filing_id = {marker}")
        params.append(filing_id)
    if manager_id is not None:
        where.append(f"f.manager_id = {marker}")
        params.append(manager_id)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    if join_filings:
        sql = (
            "SELECT h.* FROM holdings h "
            "JOIN filings f ON f.filing_id = h.filing_id"
            f"{where_sql} ORDER BY h.holding_id"
        )
    else:
        sql = f"SELECT h.* FROM holdings h{where_sql} ORDER BY h.holding_id"
    result = conn.execute(sql, tuple(params))
    return _rows_as_dicts(result)


def holdings_as_of(
    conn: Any,
    manager_id: int,
    as_of_date: date | datetime,
) -> list[dict[str, Any]]:
    """Return authoritative holdings known by ``as_of_date`` for a manager.

    A row is visible when ``knowledge_time <= as_of`` and
    (``superseded_at`` is null or ``superseded_at > as_of``). Among filings with
    ``filed_date <= as_of``, the latest filing per ``period_end`` (else
    ``filed_date``) is selected, then that filing's visible holdings are returned.
    """
    holdings_columns = get_table_columns(conn, "holdings")
    filing_columns = get_table_columns(conn, "filings")
    if "filing_id" not in holdings_columns or "filing_id" not in filing_columns:
        return []

    as_of = _normalize_as_of(as_of_date)
    as_of_text = _as_of_literal(as_of)
    as_of_day = as_of.date().isoformat()
    marker = get_placeholder(conn)
    has_knowledge = "knowledge_time" in holdings_columns
    has_superseded = "superseded_at" in holdings_columns
    period_expr = (
        "COALESCE(f.period_end, f.filed_date)" if "period_end" in filing_columns else "f.filed_date"
    )

    # Load candidate filings for the manager filed by as_of, then pick latest
    # per period in Python so SQLite/Postgres stay aligned without dialect SQL.
    filing_sql = (
        f"SELECT f.filing_id, f.filed_date, {period_expr} AS event_time "
        f"FROM filings f "
        f"WHERE f.manager_id = {marker} "
        f"AND f.filed_date IS NOT NULL "
        f"AND f.filed_date <= {marker}"
    )
    filing_rows = conn.execute(filing_sql, (manager_id, as_of_day)).fetchall()
    if not filing_rows:
        return []

    latest_by_period: dict[str, tuple[Any, ...]] = {}
    for row in filing_rows:
        if hasattr(row, "keys"):
            filing_id = row["filing_id"]
            filed_date = row["filed_date"]
            event_time = row["event_time"]
        else:
            filing_id, filed_date, event_time = row[0], row[1], row[2]
        period_key = str(event_time)
        filed_key = str(filed_date)
        prev = latest_by_period.get(period_key)
        if prev is None:
            latest_by_period[period_key] = (filing_id, filed_key, event_time)
            continue
        prev_filed = prev[1]
        prev_fid = int(prev[0])
        if filed_key > prev_filed or (filed_key == prev_filed and int(filing_id) > prev_fid):
            latest_by_period[period_key] = (filing_id, filed_key, event_time)

    selected_ids = [int(item[0]) for item in latest_by_period.values()]
    if not selected_ids:
        return []

    placeholders = ", ".join(marker for _ in selected_ids)
    where = [f"h.filing_id IN ({placeholders})"]
    params: list[Any] = list(selected_ids)
    if has_knowledge:
        where.append(f"h.knowledge_time <= {marker}")
        params.append(as_of_text)
    if has_superseded:
        where.append(f"(h.superseded_at IS NULL OR h.superseded_at > {marker})")
        params.append(as_of_text)

    sql = (
        "SELECT h.* FROM holdings h "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY h.filing_id, h.holding_id"
    )
    result = conn.execute(sql, tuple(params))
    return _rows_as_dicts(result)


def _rows_as_dicts(result: Any) -> list[dict[str, Any]]:
    rows = result.fetchall() if hasattr(result, "fetchall") else list(result)
    if not rows:
        return []
    if hasattr(rows[0], "keys"):
        return [dict(row) for row in rows]
    description = getattr(result, "description", None)
    if description:
        keys = [col[0] for col in description]
        return [dict(zip(keys, row, strict=False)) for row in rows]
    return [{f"col_{idx}": value for idx, value in enumerate(row)} for row in rows]
