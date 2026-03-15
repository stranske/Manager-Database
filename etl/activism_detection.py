"""Helpers for deriving activism events from Schedule 13D/13G filings."""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from typing import Any

OWNERSHIP_THRESHOLDS = (5.0, 10.0, 15.0, 20.0, 25.0, 33.3, 50.0)
ALERT_EVENT_TYPE = "activism_event"
ACTIVISM_EVENT_TYPES = (
    "initial_stake",
    "threshold_crossing",
    "stake_increase",
    "stake_decrease",
    "group_formation",
    "amendment",
    "form_upgrade",
    "form_downgrade",
)


@dataclass(frozen=True)
class ActivismEvent:
    manager_id: int
    filing_id: int
    event_type: str
    subject_company: str
    subject_cusip: str | None
    ownership_pct: float | None
    previous_pct: float | None
    delta_pct: float | None
    threshold_crossed: float | None = None


@dataclass(frozen=True)
class AlertEvent:
    event_type: str
    manager_id: int
    payload: dict[str, Any]


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


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return round(float(value), 4)


def _to_int(value: Any) -> int:
    return int(value)


def _parse_thresholds() -> tuple[float, ...]:
    raw = os.getenv("ACTIVISM_THRESHOLDS")
    if raw is None or raw.strip() == "":
        return OWNERSHIP_THRESHOLDS

    values: list[float] = []
    for part in raw.split(","):
        stripped = part.strip()
        if not stripped:
            continue
        try:
            values.append(round(float(stripped), 4))
        except ValueError:
            continue
    if not values:
        return OWNERSHIP_THRESHOLDS
    return tuple(sorted({value for value in values if value > 0}))


def _normalize_form_type(form_type: str | None) -> str:
    normalized = str(form_type or "").upper().strip()
    if normalized.startswith("SC 13D"):
        return "SC 13D"
    if normalized.startswith("SC 13G"):
        return "SC 13G"
    return normalized


def _is_amendment(form_type: str | None) -> bool:
    return str(form_type or "").upper().strip().endswith("/A")


def _deserialize_group_members(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(item) for item in raw if str(item).strip()]
    if raw in (None, ""):
        return []
    if isinstance(raw, str):
        if raw.startswith("["):
            return _deserialize_json_array(raw)
        return [part.strip() for part in raw.split("|") if part.strip()]
    return []


def ensure_activism_events_table(conn: Any) -> None:
    if _is_sqlite(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS activism_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                manager_id INTEGER NOT NULL,
                filing_id INTEGER NOT NULL,
                event_type TEXT NOT NULL CHECK (
                    event_type IN (
                        'initial_stake',
                        'threshold_crossing',
                        'stake_increase',
                        'stake_decrease',
                        'group_formation',
                        'amendment',
                        'form_upgrade',
                        'form_downgrade'
                    )
                ),
                subject_company TEXT NOT NULL,
                subject_cusip TEXT,
                ownership_pct REAL,
                previous_pct REAL,
                delta_pct REAL,
                threshold_crossed REAL,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activism_events_manager ON activism_events(manager_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activism_events_type ON activism_events(event_type)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activism_events_date ON activism_events(detected_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activism_events_cusip ON activism_events(subject_cusip)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_activism_events_unique_base "
            "ON activism_events(manager_id, filing_id, event_type) "
            "WHERE threshold_crossed IS NULL"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_activism_events_unique_threshold "
            "ON activism_events(manager_id, filing_id, event_type, threshold_crossed) "
            "WHERE threshold_crossed IS NOT NULL"
        )
        return

    try:
        conn.execute("SELECT 1 FROM activism_events LIMIT 1")
    except Exception as exc:
        message = str(exc)
        exc_name = exc.__class__.__name__
        pgcode = getattr(exc, "pgcode", None)
        missing_table = (
            "does not exist" in message or pgcode == "42P01" or "UndefinedTable" in exc_name
        )
        if missing_table:
            raise RuntimeError(
                "activism_events table is missing on Postgres; apply schema migrations first"
            ) from exc
        raise


def ensure_alert_tables(conn: Any) -> None:
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


def _prior_filing_query(conn: Any, *, has_cusip: bool) -> tuple[str, tuple[Any, ...]]:
    ph = _placeholder(conn)
    if has_cusip:
        sql = (
            "SELECT filing_id, filing_type, ownership_pct, filed_date, subject_company, subject_cusip "
            "FROM activism_filings "
            f"WHERE manager_id = {ph} AND subject_cusip = {ph} "
            f"AND ((filed_date < {ph}) OR (filed_date = {ph} AND filing_id < {ph})) "
            "ORDER BY filed_date DESC, filing_id DESC LIMIT 1"
        )
        return sql, ()
    sql = (
        "SELECT filing_id, filing_type, ownership_pct, filed_date, subject_company, subject_cusip "
        "FROM activism_filings "
        f"WHERE manager_id = {ph} AND subject_company = {ph} "
        f"AND ((filed_date < {ph}) OR (filed_date = {ph} AND filing_id < {ph})) "
        "ORDER BY filed_date DESC, filing_id DESC LIMIT 1"
    )
    return sql, ()


def _fetch_prior_filing(conn: Any, filing: Mapping[str, Any]) -> dict[str, Any] | None:
    current_cusip = str(filing.get("subject_cusip") or "").strip()
    current_company = str(filing.get("subject_company") or "").strip()
    if not current_cusip and not current_company:
        return None

    sql, _ = _prior_filing_query(conn, has_cusip=bool(current_cusip))
    params = (
        _to_int(filing["manager_id"]),
        current_cusip if current_cusip else current_company,
        str(filing.get("filed_date") or ""),
        str(filing.get("filed_date") or ""),
        _to_int(filing["filing_id"]),
    )
    row = conn.execute(sql, params).fetchone()
    if not row:
        return None
    return {
        "filing_id": int(row[0]),
        "filing_type": str(row[1]),
        "ownership_pct": _to_float(row[2]),
        "filed_date": str(row[3] or ""),
        "subject_company": str(row[4] or ""),
        "subject_cusip": str(row[5] or "") or None,
    }


def _build_event(
    filing: Mapping[str, Any],
    *,
    event_type: str,
    previous_pct: float | None,
    threshold_crossed: float | None = None,
) -> ActivismEvent:
    ownership_pct = _to_float(filing.get("ownership_pct"))
    delta_pct = None
    if ownership_pct is not None and previous_pct is not None:
        delta_pct = round(ownership_pct - previous_pct, 4)
    return ActivismEvent(
        manager_id=_to_int(filing["manager_id"]),
        filing_id=_to_int(filing["filing_id"]),
        event_type=event_type,
        subject_company=str(filing.get("subject_company") or ""),
        subject_cusip=str(filing.get("subject_cusip") or "") or None,
        ownership_pct=ownership_pct,
        previous_pct=previous_pct,
        delta_pct=delta_pct,
        threshold_crossed=threshold_crossed,
    )


def detect_events(conn: Any, filing: Mapping[str, Any]) -> list[ActivismEvent]:
    """Detect activism events for a newly ingested filing."""
    prior = _fetch_prior_filing(conn, filing)
    current_pct = _to_float(filing.get("ownership_pct"))
    prior_pct = _to_float(prior.get("ownership_pct")) if prior else None
    compare_from = (
        prior_pct if prior_pct is not None else (0.0 if current_pct is not None else None)
    )
    events: list[ActivismEvent] = []

    if prior is None:
        events.append(_build_event(filing, event_type="initial_stake", previous_pct=None))

    if prior_pct is not None and current_pct is not None and current_pct != prior_pct:
        event_type = "stake_increase" if current_pct > prior_pct else "stake_decrease"
        events.append(_build_event(filing, event_type=event_type, previous_pct=prior_pct))

    if compare_from is not None and current_pct is not None and compare_from != current_pct:
        for threshold in _parse_thresholds():
            crossed_up = compare_from < threshold <= current_pct
            crossed_down = current_pct < threshold <= compare_from
            if crossed_up or crossed_down:
                events.append(
                    _build_event(
                        filing,
                        event_type="threshold_crossing",
                        previous_pct=compare_from,
                        threshold_crossed=threshold,
                    )
                )

    if prior is not None:
        previous_form = _normalize_form_type(str(prior.get("filing_type") or ""))
        current_form = _normalize_form_type(str(filing.get("filing_type") or ""))
        if previous_form == "SC 13G" and current_form == "SC 13D":
            events.append(_build_event(filing, event_type="form_upgrade", previous_pct=prior_pct))
        if previous_form == "SC 13D" and current_form == "SC 13G":
            events.append(_build_event(filing, event_type="form_downgrade", previous_pct=prior_pct))

    if _deserialize_group_members(filing.get("group_members")):
        events.append(_build_event(filing, event_type="group_formation", previous_pct=prior_pct))

    if _is_amendment(str(filing.get("filing_type") or "")):
        events.append(_build_event(filing, event_type="amendment", previous_pct=prior_pct))

    return events


def detect_events_batch(conn: Any, since: str) -> list[ActivismEvent]:
    """Detect events for all activism filings since a given date."""
    ensure_activism_events_table(conn)
    ph = _placeholder(conn)
    rows = conn.execute(
        "SELECT filing_id, manager_id, filing_type, subject_company, subject_cusip, "
        "ownership_pct, group_members, filed_date "
        f"FROM activism_filings WHERE filed_date >= {ph} "
        "ORDER BY filed_date ASC, filing_id ASC",
        (since,),
    ).fetchall()

    events: list[ActivismEvent] = []
    for row in rows:
        events.extend(
            detect_events(
                conn,
                {
                    "filing_id": int(row[0]),
                    "manager_id": int(row[1]),
                    "filing_type": str(row[2] or ""),
                    "subject_company": str(row[3] or ""),
                    "subject_cusip": str(row[4] or "") or None,
                    "ownership_pct": _to_float(row[5]),
                    "group_members": row[6],
                    "filed_date": str(row[7] or ""),
                },
            )
        )
    return events


def insert_activism_events(conn: Any, events: Iterable[ActivismEvent]) -> list[ActivismEvent]:
    """Persist detected activism events, skipping duplicates on reruns."""
    ensure_activism_events_table(conn)
    ph = _placeholder(conn)
    inserted: list[ActivismEvent] = []
    for event in events:
        params = (
            event.manager_id,
            event.filing_id,
            event.event_type,
            event.subject_company,
            event.subject_cusip,
            event.ownership_pct,
            event.previous_pct,
            event.delta_pct,
            event.threshold_crossed,
        )
        if _is_sqlite(conn):
            cursor = conn.execute(
                "INSERT OR IGNORE INTO activism_events("
                "manager_id, filing_id, event_type, subject_company, subject_cusip, "
                "ownership_pct, previous_pct, delta_pct, threshold_crossed"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params,
            )
        else:
            cursor = conn.execute(
                "INSERT INTO activism_events("
                "manager_id, filing_id, event_type, subject_company, subject_cusip, "
                "ownership_pct, previous_pct, delta_pct, threshold_crossed"
                f") VALUES ({', '.join([ph] * 9)}) "
                "ON CONFLICT DO NOTHING",
                params,
            )
        if getattr(cursor, "rowcount", 0):
            inserted.append(event)
    return inserted


def _condition_matches(condition_json: Mapping[str, Any], payload: Mapping[str, Any]) -> bool:
    if not condition_json:
        return True

    for key, expected in condition_json.items():
        if key == "min_ownership_pct":
            ownership_pct = _to_float(payload.get("ownership_pct"))
            if ownership_pct is None or ownership_pct < float(expected):
                return False
            continue
        if key == "min_delta_pct":
            delta_pct = _to_float(payload.get("delta_pct"))
            if delta_pct is None or abs(delta_pct) < float(expected):
                return False
            continue
        if key == "threshold_crossed":
            if _to_float(payload.get("threshold_crossed")) != _to_float(expected):
                return False
            continue
        if payload.get(key) != expected:
            return False
    return True


def fire_alerts_for_event(conn: Any, event: AlertEvent) -> int:
    """Create alert_history rows for enabled rules that match an activism event."""
    ensure_alert_tables(conn)
    ph = _placeholder(conn)
    enabled_value = 1 if _is_sqlite(conn) else True
    rows = conn.execute(
        "SELECT rule_id, name, condition_json, channels FROM alert_rules "
        f"WHERE enabled = {ph} AND event_type = {ph} "
        f"AND (manager_id IS NULL OR manager_id = {ph}) "
        "ORDER BY rule_id ASC",
        (enabled_value, event.event_type, event.manager_id),
    ).fetchall()
    inserted = 0
    for row in rows:
        condition_json = _deserialize_json_object(row[2])
        if not _condition_matches(condition_json, event.payload):
            continue

        delivered_channels = _deserialize_json_array(row[3])
        payload_json = _serialize_json(event.payload)
        channels_json = _serialize_json(delivered_channels)
        params = (int(row[0]), str(row[1]), event.event_type, payload_json, channels_json)
        if _is_sqlite(conn):
            conn.execute(
                "INSERT INTO alert_history("
                "rule_id, rule_name, event_type, payload_json, delivered_channels"
                ") VALUES (?, ?, ?, ?, ?)",
                params,
            )
        else:
            conn.execute(
                "INSERT INTO alert_history("
                "rule_id, rule_name, event_type, payload_json, delivered_channels"
                f") VALUES ({ph}, {ph}, {ph}, {ph}::jsonb, {ph}::jsonb)",
                params,
            )
        inserted += 1
    return inserted


def event_payload(event: ActivismEvent) -> dict[str, Any]:
    payload = asdict(event)
    payload["event_type"] = event.event_type
    return payload
