from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from typing import Any

import pytest

from api import activism
from etl import activism_flow
from etl.activism_detection import (
    ActivismEvent,
    ensure_activism_events_table,
    insert_activism_events,
)


class _Cursor:
    def __init__(self, row: tuple[Any, ...] | None = None, *, rowcount: int = 1) -> None:
        self._row = row
        self.rowcount = rowcount
        self.lastrowid = None

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class _StrictPostgresConn:
    forbidden_tokens = ("AUTOINCREMENT", "INSERT OR IGNORE", "PRAGMA")

    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []

    @property
    def statements(self) -> list[str]:
        return [sql for sql, _params in self.executed]

    @property
    def params(self) -> list[Any]:
        return [params for _sql, params in self.executed]

    def execute(self, sql: str, params: Any = None) -> _Cursor:
        normalized = " ".join(sql.split())
        for token in self.forbidden_tokens:
            if token in normalized.upper():
                raise AssertionError(f"SQLite-only SQL used for Postgres: {token}")
        if "?" in normalized:
            raise AssertionError("SQLite placeholder used for Postgres")
        self.executed.append((normalized, params))
        if normalized.startswith("SELECT filing_id FROM activism_filings"):
            return _Cursor(None, rowcount=0)
        if normalized.startswith("INSERT INTO activism_filings"):
            return _Cursor((42,))
        return _Cursor()


def test_activism_table_setup_uses_postgres_ddl_without_sqlite_tokens() -> None:
    conn = _StrictPostgresConn()

    activism_flow._ensure_activism_filings_table(conn)
    ensure_activism_events_table(conn)

    statements = "\n".join(conn.statements)
    assert "BIGSERIAL PRIMARY KEY" in statements
    assert "MANAGER_ID BIGINT NOT NULL REFERENCES MANAGERS(MANAGER_ID)" in statements.upper()
    assert "FILING_ID BIGINT NOT NULL REFERENCES ACTIVISM_FILINGS(FILING_ID)" in statements.upper()
    assert "FILING_TYPE TEXT NOT NULL CHECK" in statements.upper()
    assert "FILED_DATE DATE NOT NULL" in statements.upper()
    assert "NUMERIC(8,4)" in statements.upper()
    assert "TEXT[]" in statements
    assert "DOUBLE PRECISION" not in statements.upper()
    assert "MANAGER_ID INTEGER" not in statements.upper()
    assert "FILING_ID INTEGER" not in statements.upper()
    assert "FILED_DATE TEXT" not in statements.upper()
    assert "AUTOINCREMENT" not in statements
    assert "INSERT OR IGNORE" not in statements
    assert "PRAGMA" not in statements


def test_activism_event_insert_uses_backend_placeholder_and_conflict_clause() -> None:
    conn = _StrictPostgresConn()
    event = ActivismEvent(
        manager_id=1,
        filing_id=2,
        event_type="initial_stake",
        subject_company="Example Co",
        subject_cusip="123456789",
        ownership_pct=5.1,
        previous_pct=None,
        delta_pct=None,
    )

    inserted = insert_activism_events(conn, [event])

    assert inserted == [event]
    insert_sql = next(
        sql for sql in conn.statements if sql.startswith("INSERT INTO activism_events")
    )
    assert "%s" in insert_sql
    assert "ON CONFLICT DO NOTHING" in insert_sql
    assert "INSERT OR IGNORE" not in insert_sql
    assert "?" not in insert_sql


def test_activism_filing_upsert_uses_postgres_placeholders_and_array_members() -> None:
    conn = _StrictPostgresConn()
    filing_id, is_new = activism_flow._upsert_activism_filing(
        conn,
        manager_id=7,
        filing={"form": "SC 13D", "filed": "2026-05-09", "url": "https://sec.example/filing"},
        parsed={
            "subject_company": "Example Co",
            "cusip": "123456789",
            "ownership_pct": 6.2,
            "shares": 1000,
            "group_members": ["Fund A", "Fund B"],
            "purpose_snippet": "Engagement",
        },
        raw_key="raw/activism/example.txt",
    )

    assert (filing_id, is_new) == (42, True)
    insert_sql = next(
        sql for sql in conn.statements if sql.startswith("INSERT INTO activism_filings")
    )
    assert "%s" in insert_sql
    assert "?" not in insert_sql
    insert_params = conn.params[-1]
    assert insert_params[6] == ["Fund A", "Fund B"]


@pytest.fixture(params=["id", "manager_id"])
def activism_sqlite(request: pytest.FixtureRequest) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(":memory:")
    conn.execute(f"CREATE TABLE managers ({request.param} INTEGER PRIMARY KEY, name TEXT)")
    conn.executescript("""
        INSERT INTO managers VALUES (7, 'Example Manager');
        CREATE TABLE activism_filings (
            filing_id INTEGER PRIMARY KEY, manager_id INTEGER, filing_type TEXT,
            subject_company TEXT, subject_cusip TEXT, ownership_pct REAL,
            shares INTEGER, filed_date TEXT, url TEXT
        );
        INSERT INTO activism_filings VALUES
            (10, 7, 'SC 13D', 'Example Co', '123456789', 6.2, 1000, '2026-05-09', NULL);
        CREATE TABLE activism_events (
            event_id INTEGER PRIMARY KEY, manager_id INTEGER, event_type TEXT,
            subject_company TEXT, subject_cusip TEXT, ownership_pct REAL,
            previous_pct REAL, delta_pct REAL, threshold_crossed REAL, detected_at TEXT
        );
        INSERT INTO activism_events VALUES
            (20, 7, 'initial_stake', 'Example Co', '123456789', 6.2,
             NULL, NULL, NULL, '2026-05-09T12:00:00');
        CREATE TABLE activism_campaigns (
            campaign_id INTEGER PRIMARY KEY, manager_id INTEGER, target_identifier TEXT,
            target_company TEXT, first_filed TEXT, last_filed TEXT, status TEXT,
            peak_ownership_pct REAL, latest_ownership_pct REAL, filing_count INTEGER,
            event_count INTEGER, latest_event_type TEXT, data_quality_flags TEXT,
            window_return REAL
        );
        INSERT INTO activism_campaigns VALUES
            (30, 7, '123456789', 'Example Co', '2026-05-09', '2026-05-09', 'active',
             6.2, 6.2, 1, 1, 'initial_stake', '[]', 0.12);
    """)
    try:
        yield conn
    finally:
        conn.close()


@pytest.mark.parametrize("missing_manager", [False, True])
@pytest.mark.parametrize(
    "query,kwargs,record_key,expected",
    [
        (activism.query_activism_filings, {"manager_id": 7}, "filing_id", 10),
        (activism.query_activism_events, {"manager_id": 7}, "event_id", 20),
        (activism.query_active_campaigns, {}, "event_count", 1),
        (activism.query_activism_campaigns, {"manager_id": 7}, "campaign_id", 30),
        (activism.query_manager_activism_profile, {"manager_id": 7}, "campaign_count", 1),
    ],
)
def test_activism_queries_resolve_manager_join(
    activism_sqlite, missing_manager, query, kwargs, record_key, expected
):
    if missing_manager:
        activism_sqlite.execute("DELETE FROM managers")

    result = query(activism_sqlite, **kwargs)
    if isinstance(result, list):
        assert len(result) == 1
        result = result[0]
    assert result is not None
    assert getattr(result, record_key) == expected
    assert result.manager_name == (None if missing_manager else "Example Manager")


@pytest.mark.parametrize(
    "query,filter_name",
    [
        (activism.query_activism_filings, "cusip"),
        (activism.query_activism_events, "cusip"),
        (activism.query_activism_campaigns, "target_identifier"),
    ],
)
def test_activism_join_keeps_filters_parameterized(activism_sqlite, query, filter_name):
    assert len(query(activism_sqlite, **{filter_name: "123456789"})) == 1
    assert query(activism_sqlite, **{filter_name: "123456789' OR 1=1 --"}) == []


def test_activism_timeline_needs_no_manager_join(activism_sqlite):
    entries = activism.query_activism_timeline(activism_sqlite, 7)
    assert [entry.type for entry in entries] == ["event", "filing"]
    assert all(entry.ownership_pct == 6.2 for entry in entries)
