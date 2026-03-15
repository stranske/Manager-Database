"""Database helpers for alert rules and alert history."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

from alerts.models import AlertRule, FiredAlert

try:  # pragma: no cover - optional dependency
    import psycopg as psycopg
except ImportError:  # pragma: no cover - psycopg not installed for SQLite-only tests
    psycopg = None  # type: ignore[assignment]


def is_sqlite(conn: Any) -> bool:
    return isinstance(conn, sqlite3.Connection)


def placeholder(conn: Any) -> str:
    return "?" if is_sqlite(conn) else "%s"


def serialize_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def serialize_channels(conn: Any, channels: list[str]) -> Any:
    return serialize_json(channels) if is_sqlite(conn) else list(channels)


def deserialize_json_object(raw: Any) -> dict[str, Any]:
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


def deserialize_json_array(raw: Any) -> list[str]:
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


def parse_timestamp(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw
    if raw is None:
        return datetime.now(UTC)
    if isinstance(raw, str):
        normalized = raw.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            pass
    raise ValueError(f"Invalid timestamp value: {raw!r}")


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    return {str(row[1]) for row in rows}


def _sqlite_add_column_if_missing(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    definition: str,
) -> None:
    if column in _sqlite_columns(conn, table):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def ensure_alert_tables(conn: Any) -> None:
    if is_sqlite(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS alert_rules (
                rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                event_type TEXT NOT NULL CHECK (
                    event_type IN (
                        'new_filing', 'large_delta', 'news_spike', 'crowded_trade_change',
                        'contrarian_signal', 'missing_filing', 'etl_failure', 'activism_event'
                    )
                ),
                condition_json TEXT NOT NULL DEFAULT '{}',
                channels TEXT NOT NULL DEFAULT '[\"streamlit\"]',
                enabled INTEGER NOT NULL DEFAULT 1,
                manager_id INTEGER,
                created_by TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(manager_id) REFERENCES managers(manager_id)
            )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_rules_event ON alert_rules(event_type)")
        _sqlite_add_column_if_missing(conn, "alert_rules", "description", "TEXT")
        _sqlite_add_column_if_missing(conn, "alert_rules", "created_by", "TEXT")
        _sqlite_add_column_if_missing(conn, "alert_rules", "updated_at", "TIMESTAMP")
        conn.execute(
            "UPDATE alert_rules SET updated_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP)"
        )

        conn.execute("""CREATE TABLE IF NOT EXISTS alert_history (
                alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id INTEGER NOT NULL,
                rule_name TEXT NOT NULL,
                fired_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                delivered_channels TEXT NOT NULL DEFAULT '[]',
                delivery_errors TEXT,
                acknowledged INTEGER NOT NULL DEFAULT 0,
                acknowledged_by TEXT,
                acknowledged_at TIMESTAMP,
                FOREIGN KEY(rule_id) REFERENCES alert_rules(rule_id)
            )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alert_history_unack ON alert_history(fired_at DESC)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_history_rule ON alert_history(rule_id)")
        _sqlite_add_column_if_missing(conn, "alert_history", "delivery_errors", "TEXT")
        conn.commit()
        return

    conn.execute("""CREATE TABLE IF NOT EXISTS alert_rules (
            rule_id bigserial PRIMARY KEY,
            name text NOT NULL,
            description text,
            event_type text NOT NULL CHECK (
                event_type IN (
                    'new_filing', 'large_delta', 'news_spike', 'crowded_trade_change',
                    'contrarian_signal', 'missing_filing', 'etl_failure', 'activism_event'
                )
            ),
            condition_json jsonb NOT NULL DEFAULT '{}'::jsonb,
            channels text[] NOT NULL DEFAULT ARRAY['streamlit'],
            enabled boolean NOT NULL DEFAULT true,
            manager_id bigint REFERENCES managers(manager_id),
            created_by text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )""")
    conn.execute("ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS description text")
    conn.execute("ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS created_by text")
    conn.execute(
        "ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now()"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alert_rules_event ON alert_rules(event_type) WHERE enabled = true"
    )

    conn.execute("""CREATE TABLE IF NOT EXISTS alert_history (
            alert_id bigserial PRIMARY KEY,
            rule_id bigint NOT NULL REFERENCES alert_rules(rule_id),
            rule_name text NOT NULL,
            fired_at timestamptz NOT NULL DEFAULT now(),
            event_type text NOT NULL,
            payload_json jsonb NOT NULL,
            delivered_channels text[] NOT NULL DEFAULT ARRAY[]::text[],
            delivery_errors jsonb,
            acknowledged boolean NOT NULL DEFAULT false,
            acknowledged_by text,
            acknowledged_at timestamptz
        )""")
    conn.execute("ALTER TABLE alert_history ADD COLUMN IF NOT EXISTS delivery_errors jsonb")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alert_history_unack ON alert_history(fired_at DESC) WHERE acknowledged = false"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_history_rule ON alert_history(rule_id)")


def rule_from_row(row: tuple[Any, ...]) -> AlertRule:
    return AlertRule(
        rule_id=int(row[0]),
        name=str(row[1]),
        description=str(row[2]) if row[2] is not None else None,
        event_type=str(row[3]),
        condition_json=deserialize_json_object(row[4]),
        channels=deserialize_json_array(row[5]),
        enabled=bool(row[6]),
        manager_id=int(row[7]) if row[7] is not None else None,
        created_by=str(row[8]) if row[8] is not None else None,
        created_at=parse_timestamp(row[9]),
        updated_at=parse_timestamp(row[10]),
    )


def fetch_rule_by_id(conn: Any, rule_id: int) -> tuple[Any, ...] | None:
    ph = placeholder(conn)
    cursor = conn.execute(
        f"""SELECT rule_id, name, description, event_type, condition_json, channels, enabled,
                   manager_id, created_by, created_at, updated_at
              FROM alert_rules
              WHERE rule_id = {ph}""",
        (rule_id,),
    )
    return cursor.fetchone()


def fetch_alert_by_id(conn: Any, alert_id: int) -> tuple[Any, ...] | None:
    ph = placeholder(conn)
    cursor = conn.execute(
        f"""SELECT alert_id, rule_name, event_type, payload_json, fired_at, delivered_channels,
                   acknowledged
              FROM alert_history
              WHERE alert_id = {ph}""",
        (alert_id,),
    )
    return cursor.fetchone()


def insert_alert_history(conn: Any, fired_alerts: list[FiredAlert]) -> list[int]:
    if not fired_alerts:
        return []

    ensure_alert_tables(conn)
    alert_ids: list[int] = []
    ph = placeholder(conn)
    for fired in fired_alerts:
        params = (
            fired.rule.rule_id,
            fired.rule.name,
            fired.event.event_type,
            serialize_json(fired.event.payload),
            serialize_channels(conn, fired.channels),
        )
        if is_sqlite(conn):
            cursor = conn.execute(
                """INSERT INTO alert_history(
                    rule_id, rule_name, event_type, payload_json, delivered_channels
                ) VALUES (?, ?, ?, ?, ?)""",
                params,
            )
            if cursor.lastrowid is not None:
                alert_ids.append(int(cursor.lastrowid))
        else:
            cursor = conn.execute(
                f"""INSERT INTO alert_history(
                    rule_id, rule_name, event_type, payload_json, delivered_channels
                ) VALUES ({ph}, {ph}, {ph}, {ph}::jsonb, {ph})
                RETURNING alert_id""",
                params,
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                alert_ids.append(int(row[0]))
    if is_sqlite(conn):
        conn.commit()
    return alert_ids
