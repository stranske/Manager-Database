"""Materialize deterministic activism campaigns from filing-level records."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any

from adapters.base import get_placeholder, is_postgres, is_sqlite, table_exists


@dataclass(frozen=True)
class CampaignRunSummary:
    filings_scanned: int = 0
    campaigns_written: int = 0
    timeline_rows_written: int = 0
    skipped_filings: int = 0
    skip_reasons: dict[str, int] | None = None


def ensure_activism_campaign_tables(conn: Any) -> None:
    """Create runtime tables for SQLite development databases.

    Production schemas are owned by Alembic; these definitions keep the
    ingestion/materialization path usable in a fresh local SQLite database.
    """
    if is_sqlite(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS activism_campaigns (
                campaign_id INTEGER PRIMARY KEY,
                manager_id INTEGER NOT NULL,
                target_identifier TEXT NOT NULL,
                target_company TEXT NOT NULL,
                first_filed TEXT NOT NULL,
                last_filed TEXT NOT NULL,
                status TEXT NOT NULL,
                peak_ownership_pct REAL,
                latest_ownership_pct REAL,
                filing_count INTEGER NOT NULL DEFAULT 0,
                event_count INTEGER NOT NULL DEFAULT 0,
                latest_event_type TEXT,
                source_forms TEXT NOT NULL DEFAULT '[]',
                data_quality_flags TEXT NOT NULL DEFAULT '[]',
                computed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(manager_id, target_identifier)
            )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS activism_campaign_timeline (
                timeline_id INTEGER PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES activism_campaigns(campaign_id),
                filing_id INTEGER NOT NULL REFERENCES activism_filings(filing_id),
                event_id INTEGER REFERENCES activism_events(event_id),
                event_date TEXT NOT NULL,
                event_type TEXT NOT NULL,
                form_type TEXT NOT NULL,
                ownership_pct REAL,
                summary TEXT NOT NULL,
                source_url TEXT,
                UNIQUE(campaign_id, filing_id, event_id)
            )""")
    elif is_postgres(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS activism_campaigns (
                campaign_id BIGSERIAL PRIMARY KEY,
                manager_id BIGINT NOT NULL REFERENCES managers(manager_id),
                target_identifier TEXT NOT NULL,
                target_company TEXT NOT NULL,
                first_filed DATE NOT NULL,
                last_filed DATE NOT NULL,
                status TEXT NOT NULL,
                peak_ownership_pct NUMERIC(8,4),
                latest_ownership_pct NUMERIC(8,4),
                filing_count INTEGER NOT NULL DEFAULT 0,
                event_count INTEGER NOT NULL DEFAULT 0,
                latest_event_type TEXT,
                source_forms JSONB NOT NULL DEFAULT '[]'::jsonb,
                data_quality_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
                computed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(manager_id, target_identifier)
            )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS activism_campaign_timeline (
                timeline_id BIGSERIAL PRIMARY KEY,
                campaign_id BIGINT NOT NULL REFERENCES activism_campaigns(campaign_id),
                filing_id BIGINT NOT NULL REFERENCES activism_filings(filing_id),
                event_id BIGINT REFERENCES activism_events(event_id),
                event_date DATE NOT NULL,
                event_type TEXT NOT NULL,
                form_type TEXT NOT NULL,
                ownership_pct NUMERIC(8,4),
                summary TEXT NOT NULL,
                source_url TEXT,
                UNIQUE NULLS NOT DISTINCT(campaign_id, filing_id, event_id)
            )""")
    else:
        raise TypeError(f"Unsupported database connection type: {type(conn)!r}")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_activism_campaigns_manager ON activism_campaigns(manager_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_activism_campaigns_status ON activism_campaigns(status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_activism_campaign_timeline_campaign "
        "ON activism_campaign_timeline(campaign_id, event_date)"
    )


def _target_identifier(cusip: Any, company: Any) -> tuple[str | None, list[str]]:
    normalized_cusip = str(cusip or "").strip().upper()
    if normalized_cusip:
        return normalized_cusip, []
    normalized_company = " ".join(str(company or "").upper().split())
    if normalized_company:
        return f"name:{normalized_company}", ["missing_cusip"]
    return None, ["missing_target"]


def _status(form_type: str, latest_ownership: float | None) -> str:
    if latest_ownership is not None and latest_ownership <= 0:
        return "closed"
    if form_type.startswith("SC 13D"):
        return "active"
    if form_type.startswith("SC 13G"):
        return "monitoring"
    return "unknown"


def _json(value: list[str]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _timeline_summary(form_type: str, company: str, ownership: float | None) -> str:
    ownership_text = f" at {ownership:g}% ownership" if ownership is not None else ""
    return f"Filed {form_type} for {company}{ownership_text}."


def _upsert_campaign(
    conn: Any,
    *,
    manager_id: int,
    target_identifier: str,
    target_company: str,
    first_filed: str,
    last_filed: str,
    status: str,
    peak_ownership: float | None,
    latest_ownership: float | None,
    filing_count: int,
    event_count: int,
    latest_event_type: str | None,
    forms: list[str],
    flags: list[str],
) -> int:
    ph = get_placeholder(conn)
    values = (
        manager_id,
        target_identifier,
        target_company,
        first_filed,
        last_filed,
        status,
        peak_ownership,
        latest_ownership,
        filing_count,
        event_count,
        latest_event_type,
        _json(forms),
        _json(flags),
    )
    if is_sqlite(conn):
        conn.execute(
            "INSERT INTO activism_campaigns(manager_id, target_identifier, target_company, first_filed, "
            "last_filed, status, peak_ownership_pct, latest_ownership_pct, filing_count, event_count, "
            "latest_event_type, source_forms, data_quality_flags, computed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(manager_id, target_identifier) DO UPDATE SET "
            "target_company=excluded.target_company, first_filed=excluded.first_filed, "
            "last_filed=excluded.last_filed, status=excluded.status, peak_ownership_pct=excluded.peak_ownership_pct, "
            "latest_ownership_pct=excluded.latest_ownership_pct, filing_count=excluded.filing_count, "
            "event_count=excluded.event_count, latest_event_type=excluded.latest_event_type, "
            "source_forms=excluded.source_forms, data_quality_flags=excluded.data_quality_flags, "
            "computed_at=CURRENT_TIMESTAMP",
            values,
        )
        row = conn.execute(
            "SELECT campaign_id FROM activism_campaigns WHERE manager_id = ? AND target_identifier = ?",
            (manager_id, target_identifier),
        ).fetchone()
    else:
        conn.execute(
            "INSERT INTO activism_campaigns(manager_id, target_identifier, target_company, first_filed, "
            "last_filed, status, peak_ownership_pct, latest_ownership_pct, filing_count, event_count, "
            "latest_event_type, source_forms, data_quality_flags, computed_at) "
            f"VALUES ({', '.join([ph] * 13)}, CURRENT_TIMESTAMP) "
            "ON CONFLICT(manager_id, target_identifier) DO UPDATE SET "
            "target_company=excluded.target_company, first_filed=excluded.first_filed, "
            "last_filed=excluded.last_filed, status=excluded.status, peak_ownership_pct=excluded.peak_ownership_pct, "
            "latest_ownership_pct=excluded.latest_ownership_pct, filing_count=excluded.filing_count, "
            "event_count=excluded.event_count, latest_event_type=excluded.latest_event_type, "
            "source_forms=excluded.source_forms, data_quality_flags=excluded.data_quality_flags, "
            "computed_at=CURRENT_TIMESTAMP",
            values,
        )
        row = conn.execute(
            f"SELECT campaign_id FROM activism_campaigns WHERE manager_id = {ph} AND target_identifier = {ph}",
            (manager_id, target_identifier),
        ).fetchone()
    assert row is not None
    return int(row[0])


def materialize_activism_campaigns(conn: Any, since: date | None = None) -> CampaignRunSummary:
    """Build campaign summaries and deterministic filing timelines from source rows."""
    if not table_exists(conn, "activism_filings"):
        return CampaignRunSummary(skip_reasons={"missing_activism_filings_table": 1})
    ensure_activism_campaign_tables(conn)
    ph = get_placeholder(conn)
    where = "WHERE af.filed_date >= " + ph if since else ""
    rows = conn.execute(
        "SELECT af.filing_id, af.manager_id, af.filing_type, af.subject_company, af.subject_cusip, "
        "af.ownership_pct, af.filed_date, af.url FROM activism_filings af "
        f"{where} ORDER BY af.manager_id, af.subject_cusip, af.subject_company, af.filed_date, af.filing_id",
        (since,) if since else (),
    ).fetchall()
    events_by_filing: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
    if table_exists(conn, "activism_events"):
        for event in conn.execute(
            "SELECT event_id, filing_id, event_type, detected_at FROM activism_events ORDER BY detected_at, event_id"
        ).fetchall():
            events_by_filing[int(event[1])].append(event)

    grouped: dict[tuple[int, str], list[tuple[Any, ...]]] = defaultdict(list)
    skipped: dict[str, int] = defaultdict(int)
    for row in rows:
        identifier, flags = _target_identifier(row[4], row[3])
        if identifier is None:
            skipped[flags[0]] += 1
            continue
        grouped[(int(row[1]), identifier)].append(row)

    timeline_count = 0
    for (manager_id, identifier), filings in grouped.items():
        first, latest = filings[0], filings[-1]
        ownerships = [float(row[5]) for row in filings if row[5] is not None]
        events = [event for row in filings for event in events_by_filing[int(row[0])]]
        campaign_id = _upsert_campaign(
            conn,
            manager_id=manager_id,
            target_identifier=identifier,
            target_company=str(latest[3]),
            first_filed=str(first[6]),
            last_filed=str(latest[6]),
            status=_status(str(latest[2]), float(latest[5]) if latest[5] is not None else None),
            peak_ownership=max(ownerships) if ownerships else None,
            latest_ownership=float(latest[5]) if latest[5] is not None else None,
            filing_count=len(filings),
            event_count=len(events),
            latest_event_type=str(events[-1][2]) if events else None,
            forms=sorted({str(row[2]) for row in filings}),
            flags=_target_identifier(latest[4], latest[3])[1],
        )
        for filing in filings:
            filing_id = int(filing[0])
            event_rows = events_by_filing.get(filing_id) or [
                (None, filing_id, "initial_filing", filing[6])
            ]
            for event_id, _event_filing_id, event_type, detected_at in event_rows:
                event_date = (
                    str(detected_at).split("T", 1)[0] if event_id is not None else str(filing[6])
                )
                if is_sqlite(conn):
                    # SQLite UNIQUE constraints consider NULL values distinct, so a
                    # filing-only timeline row (event_id NULL) needs an explicit
                    # replacement to keep reruns append-safe.
                    conn.execute(
                        "DELETE FROM activism_campaign_timeline WHERE campaign_id = ? AND filing_id = ? "
                        "AND event_id IS ?",
                        (campaign_id, filing_id, event_id),
                    )
                    conn.execute(
                        "INSERT INTO activism_campaign_timeline(campaign_id, filing_id, event_id, event_date, "
                        "event_type, form_type, ownership_pct, summary, source_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        "",
                        (
                            campaign_id,
                            filing_id,
                            event_id,
                            event_date,
                            event_type,
                            filing[2],
                            filing[5],
                            _timeline_summary(
                                str(filing[2]),
                                str(filing[3]),
                                float(filing[5]) if filing[5] is not None else None,
                            ),
                            filing[7],
                        ),
                    )
                else:
                    conn.execute(
                        "DELETE FROM activism_campaign_timeline WHERE campaign_id = %s AND filing_id = %s "
                        "AND event_id IS NOT DISTINCT FROM %s",
                        (campaign_id, filing_id, event_id),
                    )
                    conn.execute(
                        "INSERT INTO activism_campaign_timeline(campaign_id, filing_id, event_id, event_date, "
                        "event_type, form_type, ownership_pct, summary, source_url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            campaign_id,
                            filing_id,
                            event_id,
                            event_date,
                            event_type,
                            filing[2],
                            filing[5],
                            _timeline_summary(
                                str(filing[2]),
                                str(filing[3]),
                                float(filing[5]) if filing[5] is not None else None,
                            ),
                            filing[7],
                        ),
                    )
                timeline_count += 1
    conn.commit()
    return CampaignRunSummary(
        len(rows), len(grouped), timeline_count, sum(skipped.values()), dict(skipped)
    )
