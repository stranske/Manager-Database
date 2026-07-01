from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path


def _load_seed_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "seed_managers.py"
    spec = importlib.util.spec_from_file_location("seed_managers_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakePostgresResult:
    def __init__(self, rows_by_cik: dict[str, dict[str, object]]) -> None:
        self._rows_by_cik = rows_by_cik
        self._last_row: tuple[bool] | None = None

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        assert "ON CONFLICT (cik) WHERE cik IS NOT NULL DO UPDATE" in query
        assert "VALUES (%s, %s, %s, %s, %s)" in query
        cik = str(params[1])
        inserted = cik not in self._rows_by_cik
        self._rows_by_cik[cik] = {
            "name": params[0],
            "aliases": params[2],
            "jurisdictions": params[3],
            "tags": params[4],
        }
        self._last_row = (inserted,)

    def fetchone(self) -> tuple[bool] | None:
        return self._last_row


class _FakePostgresConnection:
    def __init__(self, rows_by_cik: dict[str, dict[str, object]], commits: list[int]) -> None:
        self._rows_by_cik = rows_by_cik
        self._commits = commits
        self.closed = False

    def execute(self, query: str, params: tuple[object, ...]) -> _FakePostgresResult:
        result = _FakePostgresResult(self._rows_by_cik)
        result.execute(query, params)
        return result

    def commit(self) -> None:
        self._commits.append(1)

    def close(self) -> None:
        self.closed = True


def test_seed_managers_uses_sqlite_when_db_url_unset(tmp_path, monkeypatch) -> None:
    sm = _load_seed_module()
    db_path = tmp_path / "local.db"
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))

    first_inserted = sm.seed_managers()
    second_inserted = sm.seed_managers()

    assert first_inserted == len(sm.SEED_MANAGERS)
    assert second_inserted == 0
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT name, cik, aliases, jurisdictions, tags FROM managers ORDER BY cik"
        ).fetchall()
    finally:
        conn.close()

    assert [row[0] for row in rows] == [
        "SIR Capital Management L.P.",
        "Elliott Investment Management L.P.",
    ]
    assert json.loads(rows[0][2]) == ["Standard Investment Research"]
    assert json.loads(rows[1][3]) == ["us"]


def test_seed_managers_keeps_postgres_upsert_contract(monkeypatch) -> None:
    sm = _load_seed_module()
    rows_by_cik: dict[str, dict[str, object]] = {}
    commits: list[int] = []
    connections: list[_FakePostgresConnection] = []

    def _fake_connect_db() -> _FakePostgresConnection:
        conn = _FakePostgresConnection(rows_by_cik, commits)
        connections.append(conn)
        return conn

    monkeypatch.setattr(sm, "connect_db", _fake_connect_db)
    monkeypatch.setenv("DB_URL", "postgresql://example:example@localhost:5432/postgres")

    first_inserted = sm.seed_managers()
    second_inserted = sm.seed_managers()

    assert first_inserted == len(sm.SEED_MANAGERS)
    assert second_inserted == 0
    assert len(rows_by_cik) == len(sm.SEED_MANAGERS)
    assert len(commits) == 2
    assert all(conn.closed for conn in connections)
