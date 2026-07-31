"""Materialize deterministic activism campaigns from filing-level records."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

from adapters.base import get_placeholder, is_postgres, is_sqlite, table_exists
from adapters.prices import PriceAdapter


@dataclass(frozen=True)
class CampaignRunSummary:
    filings_scanned: int = 0
    campaigns_written: int = 0
    timeline_rows_written: int = 0
    skipped_filings: int = 0
    skip_reasons: dict[str, int] | None = None


class PriceLookup(Protocol):
    """Minimal price dependency required to compute a campaign return."""

    def close_on_or_before(self, ticker: str | None, on: date) -> float | None: ...


def _add_sqlite_column_if_missing(conn: Any, table: str, column: str) -> None:
    names = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column.split()[0] not in names:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column}")


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
                target_ticker TEXT,
                window_return REAL,
                holding_period_days INTEGER,
                return_computed_at TEXT,
                source_forms TEXT NOT NULL DEFAULT '[]',
                data_quality_flags TEXT NOT NULL DEFAULT '[]',
                computed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(manager_id, target_identifier),
                CHECK (status IN ('active', 'monitoring', 'closed', 'unknown'))
            )""")
        for column in (
            "target_ticker TEXT",
            "window_return REAL",
            "holding_period_days INTEGER",
            "return_computed_at TEXT",
        ):
            _add_sqlite_column_if_missing(conn, "activism_campaigns", column)
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
                target_ticker TEXT,
                window_return NUMERIC(18,8),
                holding_period_days INTEGER,
                return_computed_at TIMESTAMPTZ,
                source_forms TEXT NOT NULL DEFAULT '[]',
                data_quality_flags TEXT NOT NULL DEFAULT '[]',
                computed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(manager_id, target_identifier),
                CHECK (status IN ('active', 'monitoring', 'closed', 'unknown'))
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
                UNIQUE(campaign_id, filing_id, event_id)
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
    # Both SQLite and PostgreSQL treat NULLs as distinct in a UNIQUE constraint, so the
    # composite key above cannot stop duplicate filing-only rows. A partial index can.
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_activism_campaign_timeline_filing_only "
        "ON activism_campaign_timeline(campaign_id, filing_id) WHERE event_id IS NULL"
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
    target_ticker: str | None,
    window_return: float | None,
    holding_period_days: int | None,
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
        target_ticker,
        window_return,
        holding_period_days,
        _json(forms),
        _json(flags),
    )
    conn.execute(
        "INSERT INTO activism_campaigns(manager_id, target_identifier, target_company, first_filed, "
        "last_filed, status, peak_ownership_pct, latest_ownership_pct, filing_count, event_count, "
        "latest_event_type, target_ticker, window_return, holding_period_days, source_forms, "
        "data_quality_flags, computed_at, return_computed_at) "
        f"VALUES ({', '.join([ph] * 16)}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) "
        "ON CONFLICT(manager_id, target_identifier) DO UPDATE SET "
        "target_company=excluded.target_company, first_filed=excluded.first_filed, "
        "last_filed=excluded.last_filed, status=excluded.status, peak_ownership_pct=excluded.peak_ownership_pct, "
        "latest_ownership_pct=excluded.latest_ownership_pct, filing_count=excluded.filing_count, "
        "event_count=excluded.event_count, latest_event_type=excluded.latest_event_type, "
        "target_ticker=excluded.target_ticker, window_return=excluded.window_return, "
        "holding_period_days=excluded.holding_period_days, return_computed_at=CURRENT_TIMESTAMP, "
        "source_forms=excluded.source_forms, data_quality_flags=excluded.data_quality_flags, "
        "computed_at=CURRENT_TIMESTAMP",
        values,
    )
    row = conn.execute(
        f"SELECT campaign_id FROM activism_campaigns WHERE manager_id = {ph} AND target_identifier = {ph}",
        (manager_id, target_identifier),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            "activism campaign upsert did not yield a campaign_id for "
            f"manager_id={manager_id} target_identifier={target_identifier!r}"
        )
    return int(row[0])


def _ticker_for_cusip(conn: Any, cusip: Any) -> str | None:
    """Return a cached OpenFIGI ticker without falling back to unsafe name matching."""
    if not cusip or not table_exists(conn, "identifier_resolution_cache"):
        return None
    ph = get_placeholder(conn)
    row = conn.execute(
        f"SELECT ticker FROM identifier_resolution_cache WHERE upper(cusip) = upper({ph}) LIMIT 1",
        (str(cusip),),
    ).fetchone()
    ticker = str(row[0] or "").strip().upper() if row else ""
    return ticker or None


def _campaign_window_dates(first_filed: Any, last_filed: Any) -> tuple[date, date] | None:
    """Parse the filing window once so malformed source dates stay non-fatal."""
    try:
        return date.fromisoformat(str(first_filed)[:10]), date.fromisoformat(str(last_filed)[:10])
    except ValueError:
        return None


def _campaign_return(
    adapter: PriceLookup | None, ticker: str | None, filing_dates: tuple[date, date] | None
) -> float | None:
    if adapter is None or not ticker:
        return None
    if filing_dates is None:
        return None
    entry_date, exit_date = filing_dates
    entry = adapter.close_on_or_before(ticker, entry_date)
    exit_price = adapter.close_on_or_before(ticker, exit_date)
    if entry is None or exit_price is None or entry <= 0:
        return None
    value = (exit_price - entry) / entry
    return value if math.isfinite(value) else None


def materialize_activism_campaigns(
    conn: Any, since: date | None = None, *, price_adapter: PriceLookup | None = None
) -> CampaignRunSummary:
    """Build campaign summaries and deterministic filing timelines from source rows."""
    if not table_exists(conn, "activism_filings"):
        return CampaignRunSummary(skip_reasons={"missing_activism_filings_table": 1})
    ensure_activism_campaign_tables(conn)
    # The price adapter is injected in tests and may be omitted by deployments that
    # intentionally have no market-data access. Construct the cached free adapter
    # only when the identifier cache is present, avoiding speculative name matches.
    if price_adapter is None and table_exists(conn, "identifier_resolution_cache"):
        price_adapter = PriceAdapter(conn)
    ph = get_placeholder(conn)
    rows = conn.execute(
        "SELECT af.filing_id, af.manager_id, af.filing_type, af.subject_company, af.subject_cusip, "
        "af.ownership_pct, af.filed_date, af.url FROM activism_filings af "
        "ORDER BY af.manager_id, af.filed_date, af.filing_id"
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

    # `since` selects which campaigns to refresh; each selected campaign is still
    # recomputed from its full filing history so incremental runs cannot shrink
    # aggregates such as filing_count or first_filed.
    if since is not None:
        cutoff = str(since)
        grouped = defaultdict(
            list,
            {
                key: filings
                for key, filings in grouped.items()
                if any(str(row[6]) >= cutoff for row in filings)
            },
        )

    timeline_count = 0
    for (manager_id, identifier), filings in grouped.items():
        # Group members are collected across differing raw cusip/company spellings,
        # so order inside the group by filing date rather than trusting the SQL sort.
        filings.sort(key=lambda row: (str(row[6]), int(row[0])))
        first, latest = filings[0], filings[-1]
        ownerships = [float(row[5]) for row in filings if row[5] is not None]
        events = sorted(
            (event for row in filings for event in events_by_filing[int(row[0])]),
            key=lambda event: (str(event[3]), int(event[0])),
        )
        ticker = _ticker_for_cusip(conn, latest[4])
        filing_dates = _campaign_window_dates(first[6], latest[6])
        holding_days = (
            max(0, (filing_dates[1] - filing_dates[0]).days) if filing_dates is not None else None
        )
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
            target_ticker=ticker,
            window_return=_campaign_return(price_adapter, ticker, filing_dates),
            holding_period_days=holding_days,
            forms=sorted({str(row[2]) for row in filings}),
            flags=_target_identifier(latest[4], latest[3])[1],
        )
        insert_sql = (
            "INSERT INTO activism_campaign_timeline(campaign_id, filing_id, event_id, event_date, "
            "event_type, form_type, ownership_pct, summary, source_url) "
            f"VALUES ({', '.join([ph] * 9)})"
        )
        for filing in filings:
            filing_id = int(filing[0])
            event_rows = events_by_filing.get(filing_id) or [
                (None, filing_id, "initial_filing", filing[6])
            ]
            # Clear the whole filing rather than the rows about to be rewritten: a filing
            # first materialized without events leaves an event_id IS NULL row that no
            # later per-event delete would match.
            conn.execute(
                f"DELETE FROM activism_campaign_timeline WHERE campaign_id = {ph} AND filing_id = {ph}",
                (campaign_id, filing_id),
            )
            for event_id, _event_filing_id, event_type, _detected_at in event_rows:
                conn.execute(
                    insert_sql,
                    (
                        campaign_id,
                        filing_id,
                        event_id,
                        # detected_at records ingestion time, so the filing date is the
                        # only stable ordering key for a backfilled timeline.
                        str(filing[6]),
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
