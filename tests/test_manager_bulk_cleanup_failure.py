"""A committed bulk import must still report failed connection cleanup honestly."""

import asyncio

import httpx
import pytest
from fastapi import FastAPI

from api import managers


def test_bulk_import_reports_autocommit_restore_failure_after_commit(monkeypatch, caplog):
    psycopg = pytest.importorskip("psycopg")
    events = []

    class RestoreFailureConnection:
        autocommit = True

        def __setattr__(self, name, value):
            if name == "autocommit":
                events.append((name, value))
                if value is True:
                    raise psycopg.OperationalError("restore failed after commit")
            super().__setattr__(name, value)

        def commit(self):
            events.append("commit")

        def close(self):
            events.append("close")

    conn = RestoreFailureConnection()
    monkeypatch.setattr(managers, "connect_db", lambda: conn)
    monkeypatch.setattr(managers, "_ensure_universe_schema", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(managers, "_insert_manager", lambda *_args, **_kwargs: 17)
    monkeypatch.setattr(managers, "invalidate_cache_prefix", lambda _: events.append("invalidate"))
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
    assert events == [("autocommit", False), "commit", "invalidate", ("autocommit", True), "close"]
    assert "Bulk import autocommit restoration failed: restore failed after commit" in caplog.text
    errors = [
        record for record in caplog.records if record.message == "Database error in managers API."
    ]
    assert len(errors) == 1
    assert str(errors[0].exc_info[1]) == "restore failed after commit"
