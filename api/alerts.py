"""Alert rules and alert history API endpoints."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Body, HTTPException, Path, Query
from pydantic import BaseModel, Field, field_validator

from adapters.base import connect_db

router = APIRouter()
logger = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    import psycopg as psycopg
except ImportError:  # pragma: no cover - psycopg not installed for SQLite-only tests
    psycopg = None  # type: ignore[assignment]

DB_ERROR_TYPES: tuple[type[BaseException], ...] = (sqlite3.Error,)
if psycopg is not None:
    DB_ERROR_TYPES = DB_ERROR_TYPES + (psycopg.Error,)

ALLOWED_EVENT_TYPES = {"large_delta", "new_filing", "manager_update"}
ALLOWED_CHANNELS = {"email", "slack", "webhook", "in_app"}


class AlertRuleCreate(BaseModel):
    """Payload for creating alert rules."""

    name: str = Field(..., description="Rule name")
    event_type: str = Field(..., description="Alert event type")
    condition_json: dict[str, Any] = Field(default_factory=dict, description="Rule condition JSON")
    channels: list[str] = Field(..., description="Delivery channels")
    enabled: bool = Field(True, description="Whether the rule is enabled")
    manager_id: int | None = Field(None, description="Optional manager filter")

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Rule name is required.")
        return normalized

    @field_validator("event_type")
    @classmethod
    def _validate_event_type(cls, value: str) -> str:
        normalized = value.strip()
        if normalized not in ALLOWED_EVENT_TYPES:
            raise ValueError(f"Unsupported event_type: {normalized}")
        return normalized

    @field_validator("channels")
    @classmethod
    def _validate_channels(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("At least one delivery channel is required.")
        normalized = [channel.strip() for channel in value if channel and channel.strip()]
        if len(normalized) != len(value):
            raise ValueError("Channels cannot be empty.")
        invalid = sorted(set(normalized) - ALLOWED_CHANNELS)
        if invalid:
            raise ValueError(f"Unsupported channels: {', '.join(invalid)}")
        return normalized


class AlertRuleUpdate(BaseModel):
    """Payload for updating alert rules."""

    name: str | None = Field(None, description="Rule name")
    condition_json: dict[str, Any] | None = Field(None, description="Rule condition JSON")
    channels: list[str] | None = Field(None, description="Delivery channels")
    enabled: bool | None = Field(None, description="Whether the rule is enabled")

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return value
        normalized = value.strip()
        if not normalized:
            raise ValueError("Rule name is required.")
        return normalized

    @field_validator("channels")
    @classmethod
    def _validate_channels(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return value
        if not value:
            raise ValueError("At least one delivery channel is required.")
        normalized = [channel.strip() for channel in value if channel and channel.strip()]
        if len(normalized) != len(value):
            raise ValueError("Channels cannot be empty.")
        invalid = sorted(set(normalized) - ALLOWED_CHANNELS)
        if invalid:
            raise ValueError(f"Unsupported channels: {', '.join(invalid)}")
        return normalized


class AlertRuleResponse(BaseModel):
    """Response payload for alert rules."""

    rule_id: int
    name: str
    event_type: str
    condition_json: dict[str, Any]
    channels: list[str]
    enabled: bool
    manager_id: int | None
    created_at: datetime


class AlertHistoryResponse(BaseModel):
    """Response payload for alert history."""

    alert_id: int
    rule_name: str
    event_type: str
    payload_json: dict[str, Any]
    fired_at: datetime
    delivered_channels: list[str]
    acknowledged: bool


def _is_sqlite(conn: Any) -> bool:
    return isinstance(conn, sqlite3.Connection)


def _placeholder(conn: Any) -> str:
    return "?" if _is_sqlite(conn) else "%s"


def _serialize_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _deserialize_json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if raw in (None, ""):
        return {}
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    return {}


def _deserialize_json_array(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(item) for item in raw]
    if raw in (None, ""):
        return []
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return []


def _parse_timestamp(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw
    if raw is None:
        return datetime.utcnow()
    if isinstance(raw, str):
        normalized = raw.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            pass
    raise ValueError(f"Invalid timestamp value: {raw!r}")


def _normalize_actor(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="Query parameter 'by' must not be empty")
    return normalized


def _ensure_alert_tables(conn: Any) -> None:
    if _is_sqlite(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS alert_rules (
                rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                condition_json TEXT NOT NULL,
                channels TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                manager_id INTEGER,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS alert_history (
                alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id INTEGER,
                rule_name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                fired_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                delivered_channels TEXT NOT NULL,
                acknowledged INTEGER NOT NULL DEFAULT 0,
                acknowledged_by TEXT,
                acknowledged_at TIMESTAMP
            )""")
        conn.commit()
        return
    conn.execute("""CREATE TABLE IF NOT EXISTS alert_rules (
            rule_id bigserial PRIMARY KEY,
            name text NOT NULL,
            event_type text NOT NULL,
            condition_json jsonb NOT NULL,
            channels jsonb NOT NULL,
            enabled boolean NOT NULL DEFAULT true,
            manager_id bigint,
            created_at timestamptz NOT NULL DEFAULT now()
        )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS alert_history (
            alert_id bigserial PRIMARY KEY,
            rule_id bigint,
            rule_name text NOT NULL,
            event_type text NOT NULL,
            payload_json jsonb NOT NULL,
            fired_at timestamptz NOT NULL DEFAULT now(),
            delivered_channels jsonb NOT NULL,
            acknowledged boolean NOT NULL DEFAULT false,
            acknowledged_by text,
            acknowledged_at timestamptz
        )""")


def _raise_db_unavailable(exc: BaseException) -> None:
    logger.exception("Database error in alerts API.", exc_info=exc)
    raise HTTPException(status_code=503, detail="Database unavailable") from exc


def _to_rule_response(row: tuple[Any, ...]) -> AlertRuleResponse:
    return AlertRuleResponse(
        rule_id=int(row[0]),
        name=str(row[1]),
        event_type=str(row[2]),
        condition_json=_deserialize_json_object(row[3]),
        channels=_deserialize_json_array(row[4]),
        enabled=bool(row[5]),
        manager_id=int(row[6]) if row[6] is not None else None,
        created_at=_parse_timestamp(row[7]),
    )


def _to_alert_response(row: tuple[Any, ...]) -> AlertHistoryResponse:
    return AlertHistoryResponse(
        alert_id=int(row[0]),
        rule_name=str(row[1]),
        event_type=str(row[2]),
        payload_json=_deserialize_json_object(row[3]),
        fired_at=_parse_timestamp(row[4]),
        delivered_channels=_deserialize_json_array(row[5]),
        acknowledged=bool(row[6]),
    )


def _fetch_rule_by_id(conn: Any, rule_id: int) -> tuple[Any, ...] | None:
    placeholder = _placeholder(conn)
    cursor = conn.execute(
        f"""SELECT rule_id, name, event_type, condition_json, channels, enabled, manager_id, created_at
            FROM alert_rules
            WHERE rule_id = {placeholder}""",
        (rule_id,),
    )
    return cursor.fetchone()


def _fetch_alert_by_id(conn: Any, alert_id: int) -> tuple[Any, ...] | None:
    placeholder = _placeholder(conn)
    cursor = conn.execute(
        f"""SELECT alert_id, rule_name, event_type, payload_json, fired_at, delivered_channels, acknowledged
            FROM alert_history
            WHERE alert_id = {placeholder}""",
        (alert_id,),
    )
    return cursor.fetchone()


@router.post("/api/alerts/rules", response_model=AlertRuleResponse, status_code=201)
async def create_rule(rule: AlertRuleCreate) -> AlertRuleResponse:
    """Create a new alert rule."""
    conn = None
    try:
        conn = connect_db()
        _ensure_alert_tables(conn)
        if _is_sqlite(conn):
            cursor = conn.execute(
                """INSERT INTO alert_rules(name, event_type, condition_json, channels, enabled, manager_id)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    rule.name,
                    rule.event_type,
                    _serialize_json(rule.condition_json),
                    _serialize_json(rule.channels),
                    1 if rule.enabled else 0,
                    rule.manager_id,
                ),
            )
            conn.commit()
            rule_id = int(cursor.lastrowid) if cursor.lastrowid is not None else 0
        else:
            cursor = conn.execute(
                """INSERT INTO alert_rules(name, event_type, condition_json, channels, enabled, manager_id)
                   VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s)
                   RETURNING rule_id""",
                (
                    rule.name,
                    rule.event_type,
                    _serialize_json(rule.condition_json),
                    _serialize_json(rule.channels),
                    rule.enabled,
                    rule.manager_id,
                ),
            )
            inserted = cursor.fetchone()
            rule_id = int(inserted[0]) if inserted and inserted[0] is not None else 0
        row = _fetch_rule_by_id(conn, rule_id)
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    if row is None:
        raise HTTPException(status_code=500, detail="Failed to create alert rule")
    return _to_rule_response(row)


@router.get("/api/alerts/rules", response_model=list[AlertRuleResponse])
async def list_rules(
    event_type: str | None = None,
    enabled: bool | None = None,
) -> list[AlertRuleResponse]:
    """List all alert rules, optionally filtered."""
    if event_type and event_type not in ALLOWED_EVENT_TYPES:
        raise HTTPException(status_code=400, detail=f"Unsupported event_type: {event_type}")
    conn = None
    try:
        conn = connect_db()
        _ensure_alert_tables(conn)
        where_clauses: list[str] = []
        params: list[Any] = []
        placeholder = _placeholder(conn)
        if event_type is not None:
            where_clauses.append(f"event_type = {placeholder}")
            params.append(event_type)
        if enabled is not None:
            where_clauses.append(f"enabled = {placeholder}")
            params.append(1 if _is_sqlite(conn) and enabled else 0 if _is_sqlite(conn) else enabled)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        cursor = conn.execute(
            f"""SELECT rule_id, name, event_type, condition_json, channels, enabled, manager_id, created_at
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
    return [_to_rule_response(row) for row in rows]


@router.get("/api/alerts/rules/{rule_id}", response_model=AlertRuleResponse)
async def get_rule(
    rule_id: int = Path(..., ge=1, description="Alert rule identifier"),
) -> AlertRuleResponse:
    """Get a single alert rule by ID."""
    conn = None
    try:
        conn = connect_db()
        _ensure_alert_tables(conn)
        row = _fetch_rule_by_id(conn, rule_id)
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    return _to_rule_response(row)


@router.put("/api/alerts/rules/{rule_id}", response_model=AlertRuleResponse)
async def update_rule(
    update: Annotated[AlertRuleUpdate, Body(...)],
    rule_id: int = Path(..., ge=1, description="Alert rule identifier"),
) -> AlertRuleResponse:
    """Update an existing alert rule."""
    if (
        update.name is None
        and update.condition_json is None
        and update.channels is None
        and update.enabled is None
    ):
        raise HTTPException(status_code=400, detail="No update fields were provided")

    conn = None
    try:
        conn = connect_db()
        _ensure_alert_tables(conn)
        existing = _fetch_rule_by_id(conn, rule_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="Alert rule not found")

        placeholder = _placeholder(conn)
        set_clauses: list[str] = []
        params: list[Any] = []
        if update.name is not None:
            set_clauses.append(f"name = {placeholder}")
            params.append(update.name)
        if update.condition_json is not None:
            if _is_sqlite(conn):
                set_clauses.append(f"condition_json = {placeholder}")
                params.append(_serialize_json(update.condition_json))
            else:
                set_clauses.append(f"condition_json = {placeholder}::jsonb")
                params.append(_serialize_json(update.condition_json))
        if update.channels is not None:
            if _is_sqlite(conn):
                set_clauses.append(f"channels = {placeholder}")
                params.append(_serialize_json(update.channels))
            else:
                set_clauses.append(f"channels = {placeholder}::jsonb")
                params.append(_serialize_json(update.channels))
        if update.enabled is not None:
            set_clauses.append(f"enabled = {placeholder}")
            params.append(
                1
                if _is_sqlite(conn) and update.enabled
                else 0 if _is_sqlite(conn) else update.enabled
            )

        params.append(rule_id)
        conn.execute(
            f"UPDATE alert_rules SET {', '.join(set_clauses)} WHERE rule_id = {placeholder}",
            params,
        )
        if _is_sqlite(conn):
            conn.commit()
        row = _fetch_rule_by_id(conn, rule_id)
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    return _to_rule_response(row)


@router.delete("/api/alerts/rules/{rule_id}")
async def delete_rule(rule_id: int = Path(..., ge=1, description="Alert rule identifier")):
    """Delete an alert rule (soft delete: set enabled=false)."""
    conn = None
    try:
        conn = connect_db()
        _ensure_alert_tables(conn)
        placeholder = _placeholder(conn)
        cursor = conn.execute(
            f"UPDATE alert_rules SET enabled = {placeholder} WHERE rule_id = {placeholder}",
            (0 if _is_sqlite(conn) else False, rule_id),
        )
        if _is_sqlite(conn):
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
    if event_type and event_type not in ALLOWED_EVENT_TYPES:
        raise HTTPException(status_code=400, detail=f"Unsupported event_type: {event_type}")
    conn = None
    try:
        conn = connect_db()
        _ensure_alert_tables(conn)
        placeholder = _placeholder(conn)
        where_clauses: list[str] = []
        params: list[Any] = []
        if since is not None:
            where_clauses.append(f"fired_at >= {placeholder}")
            params.append(since.isoformat(sep=" "))
        if acknowledged is not None:
            where_clauses.append(f"acknowledged = {placeholder}")
            params.append(
                1 if _is_sqlite(conn) and acknowledged else 0 if _is_sqlite(conn) else acknowledged
            )
        if event_type is not None:
            where_clauses.append(f"event_type = {placeholder}")
            params.append(event_type)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        params.append(limit)
        cursor = conn.execute(
            f"""SELECT alert_id, rule_name, event_type, payload_json, fired_at, delivered_channels, acknowledged
                FROM alert_history
                {where_sql}
                ORDER BY fired_at DESC, alert_id DESC
                LIMIT {placeholder}""",
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
async def unacknowledged_count() -> dict:
    """Return {"count": N} for badge display."""
    conn = None
    try:
        conn = connect_db()
        _ensure_alert_tables(conn)
        placeholder = _placeholder(conn)
        cursor = conn.execute(
            f"SELECT COUNT(*) FROM alert_history WHERE acknowledged = {placeholder}",
            (0 if _is_sqlite(conn) else False,),
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
        _ensure_alert_tables(conn)
        placeholder = _placeholder(conn)
        now = datetime.utcnow().isoformat(sep=" ")
        conn.execute(
            f"""UPDATE alert_history
                SET acknowledged = {placeholder}, acknowledged_by = {placeholder}, acknowledged_at = {placeholder}
                WHERE alert_id = {placeholder}""",
            (1 if _is_sqlite(conn) else True, by, now, alert_id),
        )
        if _is_sqlite(conn):
            conn.commit()
        row = _fetch_alert_by_id(conn, alert_id)
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return _to_alert_response(row)


@router.post("/api/alerts/history/acknowledge-all")
async def acknowledge_all(by: str = Query("user", min_length=1, max_length=120)) -> dict:
    """Acknowledge all unacknowledged alerts. Returns {"acknowledged": N}."""
    by = _normalize_actor(by)
    conn = None
    try:
        conn = connect_db()
        _ensure_alert_tables(conn)
        placeholder = _placeholder(conn)
        now = datetime.utcnow().isoformat(sep=" ")
        cursor = conn.execute(
            f"""UPDATE alert_history
                SET acknowledged = {placeholder}, acknowledged_by = {placeholder}, acknowledged_at = {placeholder}
                WHERE acknowledged = {placeholder}""",
            (1 if _is_sqlite(conn) else True, by, now, 0 if _is_sqlite(conn) else False),
        )
        if _is_sqlite(conn):
            conn.commit()
        count = max(int(cursor.rowcount), 0)
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    return {"acknowledged": count}
