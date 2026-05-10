from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from alerts.db import ensure_alert_tables
from api import chat as chat_api
from api.managers import _ensure_universe_schema
from api.search import _get_columns as search_columns
from api.signals import _manager_id_column
from scripts.check_dialect_portability import scan


class _FakePostgresCursor:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self._rows = rows or []

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None


class _FakePostgresConnection:
    def __init__(self, columns: dict[str, set[str]] | None = None) -> None:
        self.columns = columns or {}
        self.statements: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> _FakePostgresCursor:
        self.statements.append((sql, params))
        normalized = " ".join(sql.split()).lower()
        if "information_schema.columns" in normalized:
            table = str(params[0])
            return _FakePostgresCursor(
                [(column,) for column in sorted(self.columns.get(table, set()))]
            )
        if "to_regclass" in normalized:
            table = str(params[0]).split(".")[-1]
            return _FakePostgresCursor([(table if table in self.columns else None,)])
        return _FakePostgresCursor([(1,)])


def test_api_alert_surfaces_pass_dialect_gate_without_allowlist() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    findings = scan(
        [
            repo_root / "alerts" / "db.py",
            repo_root / "api" / "chat.py",
            repo_root / "api" / "managers.py",
            repo_root / "api" / "search.py",
            repo_root / "api" / "signals.py",
        ],
        repo_root=repo_root,
        allowlist={},
        scan_all=True,
    )

    assert findings == []


def test_alert_and_feedback_sqlite_schema_uses_portable_integer_keys(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "alerts.db")
    try:
        ensure_alert_tables(conn)
        chat_api._ensure_chat_feedback_table(conn)

        alert_rule_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'alert_rules'"
        ).fetchone()[0]
        alert_history_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'alert_history'"
        ).fetchone()[0]
        feedback_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'chat_feedback'"
        ).fetchone()[0]

        assert "AUTOINCREMENT" not in alert_rule_sql.upper()
        assert "AUTOINCREMENT" not in alert_history_sql.upper()
        assert "AUTOINCREMENT" not in feedback_sql.upper()
        assert "INTEGER PRIMARY KEY" in alert_rule_sql.upper()
        assert "INTEGER PRIMARY KEY" in alert_history_sql.upper()
        assert "INTEGER PRIMARY KEY" in feedback_sql.upper()
    finally:
        conn.close()


def test_manager_sqlite_column_detection_handles_legacy_id(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "managers.db")
    try:
        conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT)")

        assert chat_api._manager_id_column(conn) == "id"
        assert _manager_id_column(conn) == "id"
        assert search_columns(conn, "managers") == {"id", "name"}
    finally:
        conn.close()


def test_postgres_paths_use_schema_metadata_without_sqlite_bootstrap() -> None:
    conn = _FakePostgresConnection(
        {"managers": {"manager_id", "name", "cik"}, "chat_feedback": {"feedback_id"}}
    )

    _ensure_universe_schema(conn)
    chat_api._ensure_chat_feedback_table(conn)

    executed_sql = "\n".join(sql for sql, _params in conn.statements)
    assert "ALTER TABLE managers ADD COLUMN IF NOT EXISTS cik text" in executed_sql
    assert "CREATE TABLE IF NOT EXISTS managers" not in executed_sql
    assert chat_api._manager_id_column(conn) == "manager_id"
    assert search_columns(conn, "managers") == {"manager_id", "name", "cik"}
