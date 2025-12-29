import types

import pytest

from adapters import base


def test_connect_db_uses_psycopg_with_timeout(monkeypatch):
    calls = {}

    def fake_connect(url, **kwargs):
        calls["url"] = url
        calls["kwargs"] = kwargs
        return object()

    monkeypatch.setenv("DB_URL", "postgres://example/db")
    monkeypatch.setattr(base, "psycopg", types.SimpleNamespace(connect=fake_connect))

    conn = base.connect_db(connect_timeout=5)

    assert conn is not None
    assert calls["url"] == "postgres://example/db"
    assert calls["kwargs"]["autocommit"] is True
    assert calls["kwargs"]["connect_timeout"] == 5


@pytest.mark.asyncio
async def test_tracked_call_postgres_handles_view_errors(monkeypatch):
    class Connection:
        def __init__(self):
            self.info = object()
            self.executed = []
            self.committed = False
            self.closed = False

        def execute(self, sql, params=None):
            self.executed.append((sql, params))
            if "CREATE MATERIALIZED VIEW" in sql:
                raise RuntimeError("boom")
            return self

        def commit(self):
            self.committed = True

        def close(self):
            self.closed = True

    conn = Connection()
    monkeypatch.setattr(base, "connect_db", lambda _path=None: conn)

    resp = type("Resp", (), {"status_code": 201, "content": b"ok"})()
    async with base.tracked_call("edgar", "https://example.test") as log:
        # Explicitly send a response to populate status/size fields.
        log(resp)

    insert_calls = [
        params
        for sql, params in conn.executed
        if sql.startswith("INSERT INTO api_usage")
    ]
    assert insert_calls
    assert conn.committed is True
    assert conn.closed is True
