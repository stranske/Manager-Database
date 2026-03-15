"""Alert rules and alert history API endpoints."""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Body, HTTPException, Path, Query
from pydantic import BaseModel

from adapters.base import connect_db
from alerts.db import (
    deserialize_json_array,
    deserialize_json_object,
    ensure_alert_tables,
    fetch_alert_by_id,
    fetch_rule_by_id,
    is_sqlite,
    parse_timestamp,
    placeholder,
    rule_from_row,
    serialize_channels,
    serialize_json,
)
from alerts.models import AlertRule, AlertRuleCreate, AlertRuleUpdate, normalize_event_type

router = APIRouter()
logger = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    import psycopg as psycopg
except ImportError:  # pragma: no cover - psycopg not installed for SQLite-only tests
    psycopg = None  # type: ignore[assignment]

DB_ERROR_TYPES: tuple[type[BaseException], ...] = (sqlite3.Error,)
if psycopg is not None:
    DB_ERROR_TYPES = DB_ERROR_TYPES + (psycopg.Error,)


class AlertHistoryResponse(BaseModel):
    """Response payload for alert history entries."""

    alert_id: int
    rule_name: str
    event_type: str
    payload_json: dict[str, Any]
    fired_at: datetime
    delivered_channels: list[str]
    acknowledged: bool


def _normalize_actor(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="Query parameter 'by' must not be empty")
    return normalized


def _raise_db_unavailable(exc: BaseException) -> None:
    logger.exception("Database error in alerts API.", exc_info=exc)
    raise HTTPException(status_code=503, detail="Database unavailable") from exc


def _to_alert_response(row: tuple[Any, ...]) -> AlertHistoryResponse:
    return AlertHistoryResponse(
        alert_id=int(row[0]),
        rule_name=str(row[1]),
        event_type=str(row[2]),
        payload_json=deserialize_json_object(row[3]),
        fired_at=parse_timestamp(row[4]),
        delivered_channels=deserialize_json_array(row[5]),
        acknowledged=bool(row[6]),
    )


@router.post("/api/alerts/rules", response_model=AlertRule, status_code=201)
async def create_rule(rule: AlertRuleCreate) -> AlertRule:
    """Create a new alert rule."""
    conn = None
    try:
        conn = connect_db()
        ensure_alert_tables(conn)
        if is_sqlite(conn):
            cursor = conn.execute(
                """INSERT INTO alert_rules(
                    name, description, event_type, condition_json, channels, enabled, manager_id,
                    created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    rule.name,
                    rule.description,
                    rule.event_type,
                    serialize_json(rule.condition_json),
                    serialize_channels(conn, rule.channels),
                    1 if rule.enabled else 0,
                    rule.manager_id,
                    rule.created_by,
                ),
            )
            conn.commit()
            rule_id = int(cursor.lastrowid) if cursor.lastrowid is not None else 0
        else:
            cursor = conn.execute(
                """INSERT INTO alert_rules(
                    name, description, event_type, condition_json, channels, enabled, manager_id,
                    created_by
                ) VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s)
                RETURNING rule_id""",
                (
                    rule.name,
                    rule.description,
                    rule.event_type,
                    serialize_json(rule.condition_json),
                    serialize_channels(conn, rule.channels),
                    rule.enabled,
                    rule.manager_id,
                    rule.created_by,
                ),
            )
            inserted = cursor.fetchone()
            rule_id = int(inserted[0]) if inserted and inserted[0] is not None else 0
        row = fetch_rule_by_id(conn, rule_id)
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    if row is None:
        raise HTTPException(status_code=500, detail="Failed to create alert rule")
    return rule_from_row(row)


@router.get("/api/alerts/rules", response_model=list[AlertRule])
async def list_rules(
    event_type: str | None = None,
    enabled: bool | None = None,
) -> list[AlertRule]:
    """List all alert rules, optionally filtered."""
    if event_type is not None:
        try:
            event_type = normalize_event_type(event_type)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    conn = None
    try:
        conn = connect_db()
        ensure_alert_tables(conn)
        where_clauses: list[str] = []
        params: list[Any] = []
        ph = placeholder(conn)
        if event_type is not None:
            where_clauses.append(f"event_type = {ph}")
            params.append(event_type)
        if enabled is not None:
            where_clauses.append(f"enabled = {ph}")
            params.append(1 if is_sqlite(conn) and enabled else 0 if is_sqlite(conn) else enabled)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        cursor = conn.execute(
            f"""SELECT rule_id, name, description, event_type, condition_json, channels, enabled,
                       manager_id, created_by, created_at, updated_at
                  FROM alert_rules
                  {where_sql}
                  ORDER BY rule_id DESC""",
            params,
        )
        rows = cursor.fetchall()
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    return [rule_from_row(row) for row in rows]


@router.get("/api/alerts/rules/{rule_id}", response_model=AlertRule)
async def get_rule(
    rule_id: int = Path(..., ge=1, description="Alert rule identifier"),
) -> AlertRule:
    """Get a single alert rule by ID."""
    conn = None
    try:
        conn = connect_db()
        ensure_alert_tables(conn)
        row = fetch_rule_by_id(conn, rule_id)
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    return rule_from_row(row)


@router.put("/api/alerts/rules/{rule_id}", response_model=AlertRule)
async def update_rule(
    update: Annotated[AlertRuleUpdate, Body(...)],
    rule_id: int = Path(..., ge=1, description="Alert rule identifier"),
) -> AlertRule:
    """Update an existing alert rule."""
    if (
        update.name is None
        and update.description is None
        and update.condition_json is None
        and update.channels is None
        and update.enabled is None
        and update.created_by is None
    ):
        raise HTTPException(status_code=400, detail="No update fields were provided")

    conn = None
    try:
        conn = connect_db()
        ensure_alert_tables(conn)
        existing = fetch_rule_by_id(conn, rule_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="Alert rule not found")

        ph = placeholder(conn)
        set_clauses: list[str] = []
        params: list[Any] = []
        if update.name is not None:
            set_clauses.append(f"name = {ph}")
            params.append(update.name)
        if update.description is not None:
            set_clauses.append(f"description = {ph}")
            params.append(update.description)
        if update.condition_json is not None:
            if is_sqlite(conn):
                set_clauses.append(f"condition_json = {ph}")
            else:
                set_clauses.append(f"condition_json = {ph}::jsonb")
            params.append(serialize_json(update.condition_json))
        if update.channels is not None:
            set_clauses.append(f"channels = {ph}")
            params.append(serialize_channels(conn, update.channels))
        if update.enabled is not None:
            set_clauses.append(f"enabled = {ph}")
            params.append(
                1
                if is_sqlite(conn) and update.enabled
                else 0 if is_sqlite(conn) else update.enabled
            )
        if update.created_by is not None:
            set_clauses.append(f"created_by = {ph}")
            params.append(update.created_by)
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")

        params.append(rule_id)
        conn.execute(
            f"UPDATE alert_rules SET {', '.join(set_clauses)} WHERE rule_id = {ph}",
            params,
        )
        if is_sqlite(conn):
            conn.commit()
        row = fetch_rule_by_id(conn, rule_id)
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    return rule_from_row(row)


@router.delete("/api/alerts/rules/{rule_id}")
async def delete_rule(rule_id: int = Path(..., ge=1, description="Alert rule identifier")):
    """Delete an alert rule (soft delete: set enabled=false)."""
    conn = None
    try:
        conn = connect_db()
        ensure_alert_tables(conn)
        ph = placeholder(conn)
        cursor = conn.execute(
            f"UPDATE alert_rules SET enabled = {ph}, updated_at = CURRENT_TIMESTAMP WHERE rule_id = {ph}",
            (0 if is_sqlite(conn) else False, rule_id),
        )
        if is_sqlite(conn):
            conn.commit()
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Alert rule not found")
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    return {"rule_id": rule_id, "enabled": False}


@router.get("/api/alerts/history", response_model=list[AlertHistoryResponse])
async def list_alerts(
    since: datetime | None = None,
    acknowledged: bool | None = None,
    event_type: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
) -> list[AlertHistoryResponse]:
    """List alert history, newest first."""
    if event_type is not None:
        try:
            event_type = normalize_event_type(event_type)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    conn = None
    try:
        conn = connect_db()
        ensure_alert_tables(conn)
        ph = placeholder(conn)
        where_clauses: list[str] = []
        params: list[Any] = []
        if since is not None:
            where_clauses.append(f"fired_at >= {ph}")
            params.append(since.isoformat(sep=" "))
        if acknowledged is not None:
            where_clauses.append(f"acknowledged = {ph}")
            params.append(
                1 if is_sqlite(conn) and acknowledged else 0 if is_sqlite(conn) else acknowledged
            )
        if event_type is not None:
            where_clauses.append(f"event_type = {ph}")
            params.append(event_type)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        params.append(limit)
        cursor = conn.execute(
            f"""SELECT alert_id, rule_name, event_type, payload_json, fired_at, delivered_channels,
                       acknowledged
                  FROM alert_history
                  {where_sql}
                  ORDER BY fired_at DESC, alert_id DESC
                  LIMIT {ph}""",
            params,
        )
        rows = cursor.fetchall()
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    return [_to_alert_response(row) for row in rows]


@router.get("/api/alerts/unacknowledged/count")
async def unacknowledged_count() -> dict[str, int]:
    """Return {"count": N} for badge display."""
    conn = None
    try:
        conn = connect_db()
        ensure_alert_tables(conn)
        ph = placeholder(conn)
        cursor = conn.execute(
            f"SELECT COUNT(*) FROM alert_history WHERE acknowledged = {ph}",
            (0 if is_sqlite(conn) else False,),
        )
        row = cursor.fetchone()
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    count = int(row[0]) if row and row[0] is not None else 0
    return {"count": count}


@router.post("/api/alerts/history/{alert_id}/acknowledge", response_model=AlertHistoryResponse)
async def acknowledge_alert(
    alert_id: int = Path(..., ge=1, description="Alert history identifier"),
    by: str = Query("user", min_length=1, max_length=120),
) -> AlertHistoryResponse:
    """Mark an alert as acknowledged."""
    by = _normalize_actor(by)
    conn = None
    try:
        conn = connect_db()
        ensure_alert_tables(conn)
        ph = placeholder(conn)
        now = datetime.now(UTC).isoformat(sep=" ")
        conn.execute(
            f"""UPDATE alert_history
                SET acknowledged = {ph}, acknowledged_by = {ph}, acknowledged_at = {ph}
                WHERE alert_id = {ph}""",
            (1 if is_sqlite(conn) else True, by, now, alert_id),
        )
        if is_sqlite(conn):
            conn.commit()
        row = fetch_alert_by_id(conn, alert_id)
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return _to_alert_response(row)


@router.post("/api/alerts/history/acknowledge-all")
async def acknowledge_all(by: str = Query("user", min_length=1, max_length=120)) -> dict[str, int]:
    """Acknowledge all unacknowledged alerts. Returns {"acknowledged": N}."""
    by = _normalize_actor(by)
    conn = None
    try:
        conn = connect_db()
        ensure_alert_tables(conn)
        ph = placeholder(conn)
        now = datetime.now(UTC).isoformat(sep=" ")
        cursor = conn.execute(
            f"""UPDATE alert_history
                SET acknowledged = {ph}, acknowledged_by = {ph}, acknowledged_at = {ph}
                WHERE acknowledged = {ph}""",
            (1 if is_sqlite(conn) else True, by, now, 0 if is_sqlite(conn) else False),
        )
        if is_sqlite(conn):
            conn.commit()
        count = max(int(cursor.rowcount), 0)
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    return {"acknowledged": count}
