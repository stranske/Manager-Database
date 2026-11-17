"""Materialize deterministic activism campaigns from filing-level records."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

from adapters.base import get_placeholder, get_table_columns, is_postgres, is_sqlite, table_exists
from adapters.prices import PriceAdapter

DEFAULT_STALE_AFTER_DAYS = 365

_SQLITE_CAMPAIGNS_DDL = """CREATE TABLE IF NOT EXISTS activism_campaigns (
    campaign_id INTEGER PRIMARY KEY,
    manager_id INTEGER NOT NULL REFERENCES managers(manager_id),
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
    opening_filing_id INTEGER NOT NULL REFERENCES activism_filings(filing_id),
    CONSTRAINT uq_activism_campaign_episode UNIQUE(manager_id, target_identifier, opening_filing_id),
    CONSTRAINT ck_activism_campaigns_status CHECK (status IN ('active', 'monitoring', 'closed', 'unknown'))
)"""


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


def _unwrap_connection(conn: Any) -> Any:
    if hasattr(conn, "exec_driver_sql") and hasattr(conn, "connection"):
        conn.exec_driver_sql("SELECT 1")
        return conn.connection.driver_connection
    return conn


def _add_sqlite_column_if_missing(conn: Any, table: str, column: str) -> None:
    names = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column.split()[0] not in names:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column}")


def _is_initial_filing(form_type: Any) -> bool:
    normalized = str(form_type or "").upper().strip()
    return bool(normalized) and not normalized.endswith("/A")


def _starts_new_episode(
    previous_row: tuple[Any, ...], current_row: tuple[Any, ...], stale_after_days: int
) -> bool:
    if not _is_initial_filing(current_row[2]):
        return False
    window = _campaign_window_dates(previous_row[6], current_row[6])
    if window is None:
        return False
    prev_date, curr_date = window
    return (curr_date - prev_date).days > stale_after_days


def _partition_episodes(
    filings: list[tuple[Any, ...]], stale_after_days: int = DEFAULT_STALE_AFTER_DAYS
) -> list[list[tuple[Any, ...]]]:
    ordered = sorted(filings, key=lambda row: (str(row[6]), int(row[0])))
    episodes: list[list[tuple[Any, ...]]] = []
    for row in ordered:
        if episodes and _starts_new_episode(episodes[-1][-1], row, stale_after_days):
            episodes.append([row])
        elif episodes:
            episodes[-1].append(row)
        else:
            episodes.append([row])
    return episodes


def _sqlite_campaign_table_needs_episode_upgrade(conn: Any) -> bool:
    columns = {
        row[1]: bool(row[3])
        for row in conn.execute("PRAGMA table_info(activism_campaigns)").fetchall()
    }
    if not columns.get("opening_filing_id"):
        return True
    has_episode_unique = False
    for idx in conn.execute("PRAGMA index_list(activism_campaigns)").fetchall():
        if not idx[2]:
            continue
        idx_cols = [info[2] for info in conn.execute(f"PRAGMA index_info({idx[1]})").fetchall()]
        if idx_cols == ["manager_id", "target_identifier"]:
            return True
        if idx_cols == ["manager_id", "target_identifier", "opening_filing_id"]:
            has_episode_unique = True
    return not has_episode_unique


def _linked_filings_for_campaign(
    conn: Any, campaign_id: int, manager_id: int, target_identifier: str, first_filed: Any
) -> list[tuple[Any, ...]]:
    if not table_exists(conn, "activism_filings"):
        return []
    ph = get_placeholder(conn)
    linked_ids: set[int] = set()
    for child_table in ("activism_campaign_timeline", "activism_documents"):
        if table_exists(conn, child_table):
            for (filing_id,) in conn.execute(
                f"SELECT DISTINCT filing_id FROM {child_table} WHERE campaign_id = {ph}",
                (campaign_id,),
            ).fetchall():
                linked_ids.add(int(filing_id))
    if linked_ids:
        placeholders = ", ".join([ph] * len(linked_ids))
        return list(
            conn.execute(
                "SELECT filing_id, manager_id, filing_type, subject_company, subject_cusip, "
                "ownership_pct, filed_date, url FROM activism_filings "
                f"WHERE filing_id IN ({placeholders}) ORDER BY filed_date, filing_id",
                tuple(sorted(linked_ids)),
            ).fetchall()
        )
    manager_rows = list(
        conn.execute(
            "SELECT filing_id, manager_id, filing_type, subject_company, subject_cusip, "
            "ownership_pct, filed_date, url FROM activism_filings "
            f"WHERE manager_id = {ph} ORDER BY filed_date, filing_id",
            (manager_id,),
        ).fetchall()
    )
    matched = [
        row for row in manager_rows if _target_identifier(row[4], row[3])[0] == target_identifier
    ]
    return (
        matched or [row for row in manager_rows if str(row[6]) == str(first_filed)] or manager_rows
    )


def _episode_summary_fields(
    conn: Any,
    episode: list[tuple[Any, ...]],
    events_by_filing: dict[int, list[tuple[Any, ...]]],
    price_adapter: PriceLookup | None,
    fallback_ticker: str | None,
    fallback_flags: str,
    target_identifier: str,
) -> tuple[Any, ...]:
    first, latest = episode[0], episode[-1]
    ownerships = [float(row[5]) for row in episode if row[5] is not None]
    events = sorted(
        (event for row in episode for event in events_by_filing[int(row[0])]),
        key=lambda event: (str(event[3]), int(event[0])),
    )
    ticker = _ticker_for_cusip(conn, latest[4]) or fallback_ticker
    filing_dates = _campaign_window_dates(first[6], latest[6])
    holding_days = (
        max(0, (filing_dates[1] - filing_dates[0]).days) if filing_dates is not None else None
    )
    window_return = _campaign_return(price_adapter, ticker, filing_dates)
    forms = _json(sorted({str(row[2]) for row in episode}))
    raw_ident, raw_flags = _target_identifier(latest[4], latest[3])
    flags = (
        _json(raw_flags)
        if raw_ident == target_identifier
        else (_json(["missing_cusip"]) if target_identifier.startswith("name:") else fallback_flags)
    )
    return (
        str(latest[3]),
        str(first[6]),
        str(latest[6]),
        _status(str(latest[2]), float(latest[5]) if latest[5] is not None else None),
        max(ownerships) if ownerships else None,
        float(latest[5]) if latest[5] is not None else None,
        len(episode),
        len(events),
        str(events[-1][2]) if events else None,
        ticker,
        window_return,
        holding_days,
        forms,
        flags,
        int(first[0]),
    )


def upgrade_activism_campaign_episodes(
    conn: Any, stale_after_days: int = DEFAULT_STALE_AFTER_DAYS
) -> None:
    """Upgrade legacy pair-unique campaigns to episode-anchored uniqueness and split episodes."""
    raw_conn = _unwrap_connection(conn)
    if not table_exists(raw_conn, "activism_campaigns"):
        return
    raw_conn.execute("SAVEPOINT activism_campaign_episode_upgrade")
    try:
        _upgrade_activism_campaign_episodes_inner(raw_conn, stale_after_days=stale_after_days)
    except BaseException:
        raw_conn.execute("ROLLBACK TO SAVEPOINT activism_campaign_episode_upgrade")
        raw_conn.execute("RELEASE SAVEPOINT activism_campaign_episode_upgrade")
        raise
    raw_conn.execute("RELEASE SAVEPOINT activism_campaign_episode_upgrade")


def _upgrade_activism_campaign_episodes_inner(
    conn: Any, *, stale_after_days: int = DEFAULT_STALE_AFTER_DAYS
) -> None:
    ph = get_placeholder(conn)
    events_by_filing: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
    if table_exists(conn, "activism_events"):
        for event in conn.execute(
            "SELECT event_id, filing_id, event_type, detected_at FROM activism_events "
            "ORDER BY detected_at, event_id"
        ).fetchall():
            events_by_filing[int(event[1])].append(event)
    price_adapter = (
        PriceAdapter(conn) if table_exists(conn, "identifier_resolution_cache") else None
    )

    if is_sqlite(conn):
        for column in (
            "target_ticker TEXT",
            "window_return REAL",
            "holding_period_days INTEGER",
            "return_computed_at TEXT",
        ):
            _add_sqlite_column_if_missing(conn, "activism_campaigns", column)
        needs_rebuild = _sqlite_campaign_table_needs_episode_upgrade(conn)
    elif is_postgres(conn):
        if "opening_filing_id" not in get_table_columns(conn, "activism_campaigns"):
            conn.execute(
                "ALTER TABLE activism_campaigns "
                "ADD COLUMN opening_filing_id BIGINT REFERENCES activism_filings(filing_id)"
            )
        for (conname,) in conn.execute(
            "SELECT con.conname FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = current_schema() AND c.relname = 'activism_campaigns' "
            "AND con.contype = 'u' "
            "AND pg_get_constraintdef(con.oid, true) = 'UNIQUE (manager_id, target_identifier)'"
        ).fetchall():
            conn.execute(f'ALTER TABLE activism_campaigns DROP CONSTRAINT "{conname}"')
        needs_rebuild = True
    else:
        raise TypeError(f"Unsupported database connection type: {type(conn)!r}")

    campaign_cols = get_table_columns(conn, "activism_campaigns")
    opening_expr = "opening_filing_id" if "opening_filing_id" in campaign_cols else "NULL"
    existing = conn.execute(
        "SELECT campaign_id, manager_id, target_identifier, target_company, first_filed, "
        "last_filed, status, peak_ownership_pct, latest_ownership_pct, filing_count, "
        "event_count, latest_event_type, target_ticker, window_return, holding_period_days, "
        f"return_computed_at, source_forms, data_quality_flags, computed_at, {opening_expr} "
        "FROM activism_campaigns ORDER BY campaign_id"
    ).fetchall()
    if is_sqlite(conn) and not needs_rebuild and all(row[19] is not None for row in existing):
        return

    max_id = max((int(row[0]) for row in existing), default=0)
    rebuilt_rows: list[tuple[Any, ...]] = []
    moved_filings: list[tuple[int, int, int]] = []
    for row in existing:
        cid, manager_id, target_identifier = int(row[0]), int(row[1]), str(row[2])
        linked = _linked_filings_for_campaign(conn, cid, manager_id, target_identifier, row[4])
        episodes = _partition_episodes(linked, stale_after_days=stale_after_days)
        if len(episodes) <= 1:
            opening_id = (
                int(episodes[0][0][0]) if episodes else (int(row[19]) if row[19] is not None else 1)
            )
            rebuilt_rows.append((*row[:19], opening_id))
            if is_postgres(conn) and row[19] is None:
                conn.execute(
                    f"UPDATE activism_campaigns SET opening_filing_id = {ph} WHERE campaign_id = {ph}",
                    (opening_id, cid),
                )
            continue
        for idx, episode in enumerate(episodes):
            summary = _episode_summary_fields(
                conn,
                episode,
                events_by_filing,
                price_adapter,
                str(row[12]) if row[12] is not None else None,
                str(row[17] or "[]"),
                target_identifier,
            )
            if idx == 0:
                episode_cid = cid
                if is_postgres(conn):
                    conn.execute(
                        "UPDATE activism_campaigns SET target_company = %s, first_filed = %s, "
                        "last_filed = %s, status = %s, peak_ownership_pct = %s, "
                        "latest_ownership_pct = %s, filing_count = %s, event_count = %s, "
                        "latest_event_type = %s, target_ticker = %s, window_return = %s, "
                        "holding_period_days = %s, source_forms = %s, data_quality_flags = %s, "
                        "opening_filing_id = %s WHERE campaign_id = %s",
                        (*summary, cid),
                    )
            else:
                if is_postgres(conn):
                    episode_cid = int(
                        conn.execute(
                            "INSERT INTO activism_campaigns(manager_id, target_identifier, "
                            "target_company, first_filed, last_filed, status, peak_ownership_pct, "
                            "latest_ownership_pct, filing_count, event_count, latest_event_type, "
                            "target_ticker, window_return, holding_period_days, source_forms, "
                            "data_quality_flags, opening_filing_id, return_computed_at, computed_at) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                            "RETURNING campaign_id",
                            (manager_id, target_identifier, *summary, row[15], row[18]),
                        ).fetchone()[0]
                    )
                else:
                    max_id += 1
                    episode_cid = max_id
                for filing in episode:
                    moved_filings.append((cid, int(filing[0]), episode_cid))
            rebuilt_rows.append(
                (
                    episode_cid,
                    manager_id,
                    target_identifier,
                    *summary[:12],
                    row[15],
                    summary[12],
                    summary[13],
                    row[18],
                    summary[14],
                )
            )

    has_timeline = table_exists(conn, "activism_campaign_timeline")
    has_documents = table_exists(conn, "activism_documents")
    if is_sqlite(conn):
        timeline_rows = (
            conn.execute(
                "SELECT timeline_id, campaign_id, filing_id, event_id, event_date, event_type, "
                "form_type, ownership_pct, summary, source_url "
                "FROM activism_campaign_timeline ORDER BY timeline_id"
            ).fetchall()
            if has_timeline
            else []
        )
        document_rows = (
            conn.execute(
                "SELECT document_id, campaign_id, filing_id, doc_type, source_url, raw_key, "
                "filed_date FROM activism_documents ORDER BY document_id"
            ).fetchall()
            if has_documents
            else []
        )
        if has_timeline and timeline_rows:
            conn.execute("DELETE FROM activism_campaign_timeline")
        if has_documents and document_rows:
            conn.execute("DELETE FROM activism_documents")
        conn.execute("DROP TABLE activism_campaigns")
        conn.execute(_SQLITE_CAMPAIGNS_DDL)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activism_campaigns_manager ON activism_campaigns(manager_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activism_campaigns_status ON activism_campaigns(status)"
        )
        if rebuilt_rows:
            conn.executemany(
                "INSERT INTO activism_campaigns(campaign_id, manager_id, target_identifier, "
                "target_company, first_filed, last_filed, status, peak_ownership_pct, "
                "latest_ownership_pct, filing_count, event_count, latest_event_type, "
                "target_ticker, window_return, holding_period_days, return_computed_at, "
                "source_forms, data_quality_flags, computed_at, opening_filing_id) "
                f"VALUES ({', '.join(['?'] * 20)})",
                rebuilt_rows,
            )
        if has_timeline and timeline_rows:
            conn.executemany(
                "INSERT INTO activism_campaign_timeline(timeline_id, campaign_id, filing_id, "
                "event_id, event_date, event_type, form_type, ownership_pct, summary, source_url) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                timeline_rows,
            )
        if has_documents and document_rows:
            conn.executemany(
                "INSERT INTO activism_documents(document_id, campaign_id, filing_id, doc_type, "
                "source_url, raw_key, filed_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
                document_rows,
            )

    for old_cid, filing_id, new_cid in moved_filings:
        if has_timeline:
            conn.execute(
                f"UPDATE activism_campaign_timeline SET campaign_id = {ph} "
                f"WHERE campaign_id = {ph} AND filing_id = {ph}",
                (new_cid, old_cid, filing_id),
            )
        if has_documents:
            conn.execute(
                f"UPDATE activism_documents SET campaign_id = {ph} "
                f"WHERE campaign_id = {ph} AND filing_id = {ph}",
                (new_cid, old_cid, filing_id),
            )

    if is_postgres(conn):
        conn.execute("ALTER TABLE activism_campaigns ALTER COLUMN opening_filing_id SET NOT NULL")
        has_episode_uq = conn.execute(
            "SELECT 1 FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = current_schema() AND c.relname = 'activism_campaigns' "
            "AND con.contype = 'u' "
            "AND pg_get_constraintdef(con.oid, true) = "
            "'UNIQUE (manager_id, target_identifier, opening_filing_id)'"
        ).fetchone()
        if not has_episode_uq:
            conn.execute(
                "ALTER TABLE activism_campaigns "
                "ADD CONSTRAINT uq_activism_campaign_episode "
                "UNIQUE (manager_id, target_identifier, opening_filing_id)"
            )


def ensure_activism_campaign_tables(conn: Any) -> None:
    """Create runtime tables for SQLite development databases.

    Production schemas are owned by Alembic; these definitions keep the
    ingestion/materialization path usable in a fresh local SQLite database.
    """
    if is_sqlite(conn):
        conn.execute(_SQLITE_CAMPAIGNS_DDL)
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
        conn.execute("""CREATE TABLE IF NOT EXISTS activism_documents (
                document_id INTEGER PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES activism_campaigns(campaign_id),
                filing_id INTEGER NOT NULL REFERENCES activism_filings(filing_id),
                doc_type TEXT NOT NULL,
                source_url TEXT,
                raw_key TEXT,
                filed_date TEXT NOT NULL,
                UNIQUE(campaign_id, filing_id, doc_type, source_url)
            )""")
        if _sqlite_campaign_table_needs_episode_upgrade(conn):
            upgrade_activism_campaign_episodes(conn)
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
                opening_filing_id BIGINT NOT NULL REFERENCES activism_filings(filing_id),
                CONSTRAINT uq_activism_campaign_episode UNIQUE(manager_id, target_identifier, opening_filing_id),
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
        conn.execute("""CREATE TABLE IF NOT EXISTS activism_documents (
                document_id BIGSERIAL PRIMARY KEY,
                campaign_id BIGINT NOT NULL REFERENCES activism_campaigns(campaign_id),
                filing_id BIGINT NOT NULL REFERENCES activism_filings(filing_id),
                doc_type TEXT NOT NULL,
                source_url TEXT,
                raw_key TEXT,
                filed_date DATE NOT NULL,
                UNIQUE(campaign_id, filing_id, doc_type, source_url)
            )""")
        if "opening_filing_id" not in get_table_columns(conn, "activism_campaigns"):
            upgrade_activism_campaign_episodes(conn)
    else:
        raise TypeError(f"Unsupported database connection type: {type(conn)!r}")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_activism_campaigns_manager ON activism_campaigns(manager_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_activism_documents_campaign "
        "ON activism_documents(campaign_id, filed_date)"
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


def _episodes_separated(
    first_window: tuple[date, date] | None,
    second_window: tuple[date, date] | None,
    stale_after_days: int,
) -> bool:
    if first_window is None or second_window is None:
        return False
    return (first_window[0] - second_window[1]).days > stale_after_days or (
        second_window[0] - first_window[1]
    ).days > stale_after_days


def _upsert_campaign(
    conn: Any,
    *,
    manager_id: int,
    target_identifier: str,
    opening_filing_id: int,
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
    campaign_id: int | None = None,
    stale_after_days: int = DEFAULT_STALE_AFTER_DAYS,
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
        opening_filing_id,
    )
    if campaign_id is not None:
        # Filing membership is durable identity. A corrected identifier is metadata,
        # and cannot silently move the episode into another independently owned ID.
        this_window = _campaign_window_dates(first_filed, last_filed)
        for other_id, other_opening, other_first, other_last in conn.execute(
            f"SELECT campaign_id, opening_filing_id, first_filed, last_filed "
            f"FROM activism_campaigns WHERE manager_id = {ph} AND target_identifier = {ph}",
            (manager_id, target_identifier),
        ).fetchall():
            if int(other_id) == campaign_id:
                continue
            other_window = _campaign_window_dates(other_first, other_last)
            if (
                (other_opening is not None and int(other_opening) == opening_filing_id)
                or str(other_first) == str(first_filed)
                or not _episodes_separated(this_window, other_window, stale_after_days)
            ):
                raise ValueError(
                    "Corrected identifier belongs to another campaign; explicit reconciliation required"
                )
        columns = (
            "manager_id",
            "target_identifier",
            "target_company",
            "first_filed",
            "last_filed",
            "status",
            "peak_ownership_pct",
            "latest_ownership_pct",
            "filing_count",
            "event_count",
            "latest_event_type",
            "target_ticker",
            "window_return",
            "holding_period_days",
            "source_forms",
            "data_quality_flags",
            "opening_filing_id",
        )
        conn.execute(
            "UPDATE activism_campaigns SET "
            + ", ".join(f"{column} = {ph}" for column in columns)
            + f", computed_at = CURRENT_TIMESTAMP, return_computed_at = CURRENT_TIMESTAMP WHERE campaign_id = {ph} AND manager_id = {ph}",
            (*values, campaign_id, manager_id),
        )
    else:
        conn.execute(
            "INSERT INTO activism_campaigns(manager_id, target_identifier, target_company, first_filed, "
            "last_filed, status, peak_ownership_pct, latest_ownership_pct, filing_count, event_count, "
            "latest_event_type, target_ticker, window_return, holding_period_days, source_forms, "
            "data_quality_flags, opening_filing_id, computed_at, return_computed_at) "
            f"VALUES ({', '.join([ph] * 17)}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) "
            "ON CONFLICT(manager_id, target_identifier, opening_filing_id) DO UPDATE SET "
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
        f"SELECT campaign_id FROM activism_campaigns "
        f"WHERE manager_id = {ph} AND target_identifier = {ph} AND opening_filing_id = {ph}",
        (manager_id, target_identifier, opening_filing_id),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            "activism campaign upsert did not yield a campaign_id for "
            f"manager_id={manager_id} target_identifier={target_identifier!r} "
            f"opening_filing_id={opening_filing_id}"
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


def _document_references(row: tuple[Any, ...]) -> list[tuple[str, str | None, str | None]]:
    """Return safe document-reference metadata without ingesting document contents."""
    url = str(row[7]) if row[7] else None
    raw_key = str(row[8]) if len(row) > 8 and row[8] else None
    references: list[tuple[str, str | None, str | None]] = [("filing", url, raw_key)]
    if len(row) <= 9 or not row[9]:
        return references
    try:
        metadata = json.loads(str(row[9]))
    except (TypeError, ValueError):
        return references
    if not isinstance(metadata, list):
        return references
    for item in metadata:
        if not isinstance(item, dict):
            continue
        reference_url = item.get("url") or item.get("href")
        reference_key = item.get("raw_key") or item.get("key")
        if not reference_url and not reference_key:
            continue
        doc_type = str(item.get("doc_type") or item.get("type") or "exhibit").strip() or "exhibit"
        references.append(
            (
                doc_type,
                str(reference_url) if reference_url else None,
                str(reference_key) if reference_key else None,
            )
        )
    return references


def _materialize_activism_campaigns(
    conn: Any,
    since: date | None = None,
    stale_after_days: int = DEFAULT_STALE_AFTER_DAYS,
    *,
    price_adapter: PriceLookup | None = None,
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
    filing_columns = get_table_columns(conn, "activism_filings")
    raw_key_sql = "af.raw_key" if "raw_key" in filing_columns else "NULL"
    document_metadata_sql = (
        "af.document_references" if "document_references" in filing_columns else "NULL"
    )
    rows = conn.execute(
        "SELECT af.filing_id, af.manager_id, af.filing_type, af.subject_company, af.subject_cusip, "
        f"af.ownership_pct, af.filed_date, af.url, {raw_key_sql}, {document_metadata_sql} FROM activism_filings af "
        "ORDER BY af.manager_id, af.filed_date, af.filing_id"
    ).fetchall()
    events_by_filing: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
    if table_exists(conn, "activism_events"):
        for event in conn.execute(
            "SELECT event_id, filing_id, event_type, detected_at FROM activism_events ORDER BY detected_at, event_id"
        ).fetchall():
            events_by_filing[int(event[1])].append(event)

    membership: dict[tuple[int, int], int] = {}
    for manager, filing, campaign in conn.execute(
        "SELECT c.manager_id, t.filing_id, c.campaign_id FROM activism_campaigns c "
        "JOIN activism_campaign_timeline t ON t.campaign_id=c.campaign_id "
        "UNION SELECT c.manager_id, d.filing_id, c.campaign_id FROM activism_campaigns c "
        "JOIN activism_documents d ON d.campaign_id=c.campaign_id"
    ).fetchall():
        member_key = (int(manager), int(filing))
        if member_key in membership and membership[member_key] != int(campaign):
            raise ValueError(
                "Filing belongs to multiple campaigns; explicit reconciliation required"
            )
        membership[member_key] = int(campaign)
    identities = {
        (int(manager), str(identifier), int(opening_filing)): int(campaign)
        for manager, identifier, opening_filing, campaign in conn.execute(
            "SELECT manager_id, target_identifier, opening_filing_id, campaign_id "
            "FROM activism_campaigns WHERE opening_filing_id IS NOT NULL"
        ).fetchall()
    }
    rows_by_campaign: dict[tuple[int, int], list[tuple[Any, ...]]] = defaultdict(list)
    for row in rows:
        identifier = _target_identifier(row[4], row[3])[0]
        owner = membership.get((int(row[1]), int(row[0])))
        if identifier is None or owner is None:
            continue
        rows_by_campaign[(int(row[1]), owner)].append(row)

    effective_membership: dict[tuple[int, int], int] = {}
    effective_identifiers: dict[tuple[int, int], str] = {}
    detached_filings_by_campaign: dict[int, set[int]] = defaultdict(set)
    for (manager_id, owned_campaign_id), campaign_rows in rows_by_campaign.items():
        campaign_episodes = _partition_episodes(campaign_rows, stale_after_days=stale_after_days)
        for index, episode_rows in enumerate(campaign_episodes):
            latest_identifier = _target_identifier(episode_rows[-1][4], episode_rows[-1][3])[0]
            assert latest_identifier is not None
            for row in episode_rows:
                filing_id = int(row[0])
                effective_identifiers[(manager_id, filing_id)] = latest_identifier
                if index == 0:
                    effective_membership[(manager_id, filing_id)] = owned_campaign_id
                else:
                    detached_filings_by_campaign[owned_campaign_id].add(filing_id)

    histories: dict[tuple[int, str], list[tuple[Any, ...]]] = defaultdict(list)
    skipped: dict[str, int] = defaultdict(int)
    for row in rows:
        raw_identifier, flags = _target_identifier(row[4], row[3])
        if raw_identifier is None:
            skipped[flags[0]] += 1
            continue
        manager_id = int(row[1])
        filing_id = int(row[0])
        identifier = effective_identifiers.get((manager_id, filing_id), raw_identifier)
        histories[(manager_id, identifier)].append(row)

    resolved_owners: dict[tuple[int, str, int], int] = {}
    grouped: dict[tuple[int, str, int], list[tuple[Any, ...]]] = defaultdict(list)
    existing_ids: dict[tuple[int, str, int], int] = {}
    for (manager_id, identifier), target_rows in histories.items():
        for episode in _partition_episodes(target_rows, stale_after_days=stale_after_days):
            opening_filing_id = int(episode[0][0])
            episode_key = (manager_id, identifier, opening_filing_id)
            for row in episode:
                owner = effective_membership.get((manager_id, int(row[0])))
                if owner is None:
                    continue
                if episode_key in resolved_owners and resolved_owners[episode_key] != owner:
                    raise ValueError("Resolved target has conflicting campaign ownership")
                resolved_owners[episode_key] = owner
            matched_campaign_id = resolved_owners.get(episode_key)
            if matched_campaign_id is None:
                for row in episode:
                    matched_campaign_id = identities.get((manager_id, identifier, int(row[0])))
                    if matched_campaign_id is not None:
                        break
            grouped[episode_key] = episode
            if matched_campaign_id is not None:
                existing_ids[episode_key] = matched_campaign_id

    # `since` selects which episodes to refresh; full target histories are partitioned
    # first so each selected episode is recomputed from its full episode filing history.
    if since is not None:
        cutoff = str(since)
        grouped = defaultdict(
            list,
            {
                ep_key: filings
                for ep_key, filings in grouped.items()
                if any(str(row[6]) >= cutoff for row in filings)
            },
        )

    timeline_count = 0
    for ep_key, filings in grouped.items():
        manager_id = ep_key[0]
        # Group members are collected across differing raw cusip/company spellings,
        # so order inside the group by filing date rather than trusting the SQL sort.
        filings.sort(key=lambda row: (str(row[6]), int(row[0])))
        first, latest = filings[0], filings[-1]
        opening_filing_id = int(first[0])
        identifier = _target_identifier(latest[4], latest[3])[0]
        assert identifier is not None
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
            opening_filing_id=opening_filing_id,
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
            campaign_id=existing_ids.get(ep_key),
            stale_after_days=stale_after_days,
        )
        episode_filing_ids = {int(f[0]) for f in filings}
        for detached_id in (
            detached_filings_by_campaign.get(campaign_id, set()) - episode_filing_ids
        ):
            conn.execute(
                f"DELETE FROM activism_campaign_timeline WHERE campaign_id = {ph} AND filing_id = {ph}",
                (campaign_id, detached_id),
            )
            conn.execute(
                f"DELETE FROM activism_documents WHERE campaign_id = {ph} AND filing_id = {ph}",
                (campaign_id, detached_id),
            )
        insert_sql = (
            "INSERT INTO activism_campaign_timeline(campaign_id, filing_id, event_id, event_date, "
            "event_type, form_type, ownership_pct, summary, source_url) "
            f"VALUES ({', '.join([ph] * 9)})"
        )
        for filing in filings:
            filing_id = int(filing[0])
            prior_owner = membership.get((manager_id, filing_id))
            if prior_owner is not None and prior_owner != campaign_id:
                conn.execute(
                    f"DELETE FROM activism_campaign_timeline WHERE campaign_id = {ph} AND filing_id = {ph}",
                    (prior_owner, filing_id),
                )
                conn.execute(
                    f"DELETE FROM activism_documents WHERE campaign_id = {ph} AND filing_id = {ph}",
                    (prior_owner, filing_id),
                )
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
            conn.execute(
                f"DELETE FROM activism_documents WHERE campaign_id = {ph} AND filing_id = {ph}",
                (campaign_id, filing_id),
            )
            for doc_type, source_url, raw_key in _document_references(filing):
                conn.execute(
                    "INSERT INTO activism_documents(campaign_id, filing_id, doc_type, source_url, raw_key, filed_date) "
                    f"VALUES ({', '.join([ph] * 6)})",
                    (campaign_id, filing_id, doc_type, source_url, raw_key, str(filing[6])),
                )
    return CampaignRunSummary(
        len(rows), len(grouped), timeline_count, sum(skipped.values()), dict(skipped)
    )


def materialize_activism_campaigns(
    conn: Any,
    since: date | None = None,
    stale_after_days: int = DEFAULT_STALE_AFTER_DAYS,
    *,
    price_adapter: PriceLookup | None = None,
) -> CampaignRunSummary:
    """Refresh durable campaign identities and their derived rows atomically."""
    if not table_exists(conn, "activism_filings"):
        return CampaignRunSummary(skip_reasons={"missing_activism_filings_table": 1})
    conn.execute("SAVEPOINT activism_campaign_materialization")
    try:
        result = _materialize_activism_campaigns(
            conn, since, stale_after_days=stale_after_days, price_adapter=price_adapter
        )
    except BaseException:
        conn.execute("ROLLBACK TO SAVEPOINT activism_campaign_materialization")
        conn.execute("RELEASE SAVEPOINT activism_campaign_materialization")
        raise
    conn.execute("RELEASE SAVEPOINT activism_campaign_materialization")
    conn.commit()
    return result
