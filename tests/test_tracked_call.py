import sqlite3
from pathlib import Path

import pytest

from adapters import base
from adapters.base import tracked_call


class DummyResp:
    def __init__(self, status_code=200, content=b"ok"):
        self.status_code = status_code
        self.content = content


class StrictPostgresConn:
    forbidden_tokens = ("AUTOINCREMENT", "INSERT OR IGNORE", "PRAGMA")

    def __init__(self):
        self.executed = []
        self.committed = False
        self.closed = False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        for token in self.forbidden_tokens:
            if token in normalized.upper():
                raise AssertionError(f"SQLite-only SQL used for Postgres: {token}")
        if "?" in normalized:
            raise AssertionError("SQLite placeholder used for Postgres")
        self.executed.append((normalized, params))

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_tracked_call_writes(tmp_path: Path):
    db_path = tmp_path / "dev.db"
    async with tracked_call("test", "http://x", db_path=str(db_path)) as log:
        log(DummyResp())
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT source, endpoint, status FROM api_usage").fetchone()
    view_row = conn.execute("SELECT month, source, calls FROM monthly_usage").fetchone()
    conn.close()
    assert row == ("test", "http://x", 200)
    assert view_row[1:] == ("test", 1)


@pytest.mark.asyncio
async def test_tracked_call_defaults_when_no_response(tmp_path: Path):
    db_path = tmp_path / "dev.db"
    async with tracked_call("empty", "http://none", db_path=str(db_path)):
        pass
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT source, endpoint, status, bytes FROM api_usage").fetchone()
    conn.close()
    assert row == ("empty", "http://none", 0, 0)


@pytest.mark.asyncio
async def test_tracked_call_postgres_uses_strict_backend_safe_sql(monkeypatch):
    dummy = StrictPostgresConn()
    monkeypatch.setattr(base, "connect_db", lambda _db_path=None: dummy)

    async with tracked_call("pg", "http://pg") as log:
        log(DummyResp(status_code=201, content=b"abc"))

    insert_calls = [call for call in dummy.executed if call[0].startswith("INSERT INTO")]
    assert len(insert_calls) == 1
    sql, params = insert_calls[0]
    assert "VALUES (%s,%s,%s,%s,%s,%s)" in sql
    assert params[:3] == ("pg", "http://pg", 201)
    assert params[3] == 3
    assert params[5] == 0.0
    assert any(call[0] == "SELECT to_regclass('api_usage')" for call in dummy.executed)
    assert any("CREATE MATERIALIZED VIEW monthly_usage AS" in call[0] for call in dummy.executed)
    assert dummy.committed is True
    assert dummy.closed is True
