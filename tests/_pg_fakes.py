"""Shared Postgres test doubles for adapter test suites."""

from __future__ import annotations

from typing import Any


class StrictPostgresConn:
    """Fake DB connection that rejects SQLite-only SQL and ``?`` placeholders.

    ``executed`` records ``(normalized_sql, params)`` for every call. The
    ``statements`` and ``params`` properties expose the same data in the shapes
    that pre-existing tests rely on.
    """

    forbidden_tokens = ("AUTOINCREMENT", "INSERT OR IGNORE", "PRAGMA")

    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.committed = False
        self.closed = False

    @property
    def statements(self) -> list[str]:
        return [sql for sql, _params in self.executed]

    @property
    def params(self) -> list[Any]:
        return [p for _sql, p in self.executed if p is not None]

    def execute(self, sql: str, params: Any = None) -> None:
        normalized = " ".join(sql.split())
        for token in self.forbidden_tokens:
            if token in normalized.upper():
                raise AssertionError(f"SQLite-only SQL used for Postgres: {token}")
        if "?" in normalized:
            raise AssertionError("SQLite placeholder used for Postgres")
        self.executed.append((normalized, params))

    def commit(self) -> None:
        self.committed = True

    def close(self) -> None:
        self.closed = True
