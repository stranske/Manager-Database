"""A committed bulk import must still report failed connection cleanup honestly."""

import asyncio
import logging

import httpx
import pytest
from fastapi import FastAPI

from api import managers


def test_bulk_import_reports_autocommit_restore_failure_after_commit(monkeypatch, caplog):
    psycopg = pytest.importorskip("psycopg")
    events = []
    cleanup_error = psycopg.OperationalError("restore failed after commit")

    class RestoreFailureConnection:
        autocommit = True

        def __setattr__(self, name, value):
            if name == "autocommit":
                events.append((name, value))
                if value is True:
                    raise cleanup_error
            super().__setattr__(name, value)

        def commit(self):
            events.append("commit")

        def rollback(self):
            events.append("rollback")

        def close(self):
            events.append("close")

    conn = RestoreFailureConnection()

    def insert_manager(db_conn, payload, *, commit):
        assert db_conn is conn
        assert db_conn.autocommit is False
        events.append(("insert", payload.name, commit))
        return 17

    monkeypatch.setattr(managers, "connect_db", lambda: conn)
    monkeypatch.setattr(managers, "_ensure_universe_schema", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(managers, "_insert_manager", insert_manager)
    monkeypatch.setattr(
        managers, "invalidate_cache_prefix", lambda prefix: events.append(("invalidate", prefix))
    )
    caplog.set_level(logging.WARNING, logger=managers.logger.name)
    app = FastAPI()
    app.include_router(managers.router)

    async def request():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            return await client.post("/api/managers/bulk", json=[{"name": "Manager A"}])

    response = asyncio.run(request())

    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
    assert events == [
        ("autocommit", False),
        ("insert", "Manager A", False),
        "commit",
        ("invalidate", "managers"),
        ("autocommit", True),
        "close",
    ]
    records = [record for record in caplog.records if record.name == managers.logger.name]
    assert [(record.levelno, record.getMessage()) for record in records] == [
        (
            logging.WARNING,
            "Bulk import autocommit restoration failed: restore failed after commit",
        ),
        (logging.ERROR, "Database error in managers API."),
    ]
    assert records[0].args == (cleanup_error,)
    assert records[1].exc_info[0] is psycopg.OperationalError
    assert records[1].exc_info[1] is cleanup_error
    assert records[1].exc_info[2] is not None
