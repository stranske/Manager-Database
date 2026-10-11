"""Protect create/import outage responses and committed create readback fallback."""

import asyncio
import sqlite3
from contextlib import closing
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from api import managers


@pytest.fixture
def observed_database(tmp_path, monkeypatch):
    database = tmp_path / "managers.db"
    with closing(sqlite3.connect(database)) as seed:
        managers._ensure_universe_schema(seed)
        managers._insert_manager(seed, managers.ManagerCreate(name="Original"))
    connections = []
    close_events = []

    class ObservedConnection(sqlite3.Connection):
        def close(self):
            close_events.append("close")
            super().close()

    def connect():
        connection = sqlite3.connect(database, factory=ObservedConnection)
        connections.append(connection)
        return connection

    invalidate = Mock()
    monkeypatch.setenv("DB_PATH", str(database))
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setattr(managers, "connect_db", connect)
    monkeypatch.setattr(managers, "invalidate_cache_prefix", invalidate)
    yield database, invalidate
    assert len(connections) == 1
    assert close_events == ["close"]
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")


def assert_safe_outage(caught, database, invalidate):
    assert caught.value.status_code == 503
    assert caught.value.detail == "Database unavailable"
    assert isinstance(caught.value.__cause__, sqlite3.DatabaseError)
    invalidate.assert_not_called()
    with closing(sqlite3.connect(database)) as observer:
        assert observer.execute("SELECT name FROM managers ORDER BY id").fetchall() == [
            ("Original",)
        ]


def install_failing_insert(database):
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute(
            "CREATE TRIGGER reject_new_manager BEFORE INSERT ON managers "
            "WHEN NEW.name = 'New' BEGIN "
            "SELECT RAISE(ABORT, 'private storage detail /secret/managers.db'); END"
        )


def test_create_unrelated_schema_integrity_error_returns_503(observed_database, monkeypatch):
    database, invalidate = observed_database
    insert = Mock(side_effect=AssertionError("schema failure must prevent insertion"))

    def fail_schema(connection):
        # A real integrity error whose constraint is unrelated to CIK uniqueness.
        connection.execute("INSERT INTO managers (id, name) VALUES (1, 'New')")

    monkeypatch.setattr(managers, "_ensure_universe_schema", fail_schema)
    monkeypatch.setattr(managers, "_insert_manager", insert)
    with pytest.raises(HTTPException) as caught:
        asyncio.run(managers.create_manager(managers.ManagerCreate(name="New")))
    assert_safe_outage(caught, database, invalidate)
    insert.assert_not_called()


def test_create_non_cik_insert_failure_returns_503(observed_database):
    database, invalidate = observed_database
    install_failing_insert(database)
    with pytest.raises(HTTPException) as caught:
        asyncio.run(managers.create_manager(managers.ManagerCreate(name="New")))
    assert_safe_outage(caught, database, invalidate)
    assert "private storage detail" in str(caught.value.__cause__)


def test_universe_import_write_failure_returns_503(observed_database):
    database, invalidate = observed_database
    install_failing_insert(database)
    with pytest.raises(HTTPException) as caught:
        asyncio.run(
            managers.import_manager_universe([{"name": "New", "cik": "123", "jurisdiction": "us"}])
        )
    assert_safe_outage(caught, database, invalidate)
    assert "private storage detail" in str(caught.value.__cause__)


def test_create_missing_readback_returns_committed_payload(observed_database, monkeypatch):
    database, invalidate = observed_database
    readback = Mock(return_value=None)
    monkeypatch.setattr(managers, "_fetch_manager", readback)
    payload = managers.ManagerCreate(name="New", aliases=["Alias"], tags=["new"])
    response = asyncio.run(managers.create_manager(payload))
    assert response.manager_id == 2
    assert response.name == "New"
    assert response.aliases == ["Alias"]
    assert response.tags == ["new"]
    invalidate.assert_called_once_with("managers")
    readback.assert_called_once()
    assert readback.call_args.args[1:] == (str(database), 2)
    with closing(sqlite3.connect(database)) as observer:
        assert observer.execute(
            "SELECT name, aliases, tags FROM managers WHERE id = 2"
        ).fetchone() == (
            "New",
            '["Alias"]',
            '["new"]',
        )
