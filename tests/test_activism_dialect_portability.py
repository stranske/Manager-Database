from __future__ import annotations

from typing import Any

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
    assert "TEXT[]" in statements
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
