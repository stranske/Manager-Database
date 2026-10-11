"""Protect empty updates and records removed between write and readback."""

import asyncio
import json
import sqlite3

import pytest
from fastapi import HTTPException

from api import managers


def test_empty_update_does_not_execute_sql():
    conn = sqlite3.connect(":memory:")
    statements = []
    conn.set_trace_callback(statements.append)
    try:
        assert managers._update_manager(conn, 1, managers.ManagerUpdate()) is False
        assert statements == []
    finally:
        conn.close()


def test_blank_patch_name_returns_400_before_connecting(monkeypatch):
    def unexpected_connection():
        pytest.fail("blank names must be rejected before database access")

    monkeypatch.setattr(managers, "connect_db", unexpected_connection)
    response = asyncio.run(managers.patch_manager(managers.ManagerUpdate(name=" \t "), id=1))
    assert response.status_code == 400
    errors = [{"field": "name", "message": "Name is required."}]
    assert json.loads(response.body) == {"errors": errors, "error": errors}


@pytest.fixture
def disappearing_manager(tmp_path, monkeypatch):
    db = tmp_path / "manager.db"
    with sqlite3.connect(db) as seed:
        managers._ensure_manager_table(seed)
        seed.execute("INSERT INTO managers (id, name, tags) VALUES (1, 'Original', '[]')")
    connections = []
    events = []

    class ObservedConnection(sqlite3.Connection):
        def close(self):
            events.append("close")
            super().close()

    def connect():
        conn = sqlite3.connect(db, factory=ObservedConnection)
        connections.append(conn)
        return conn

    fetch = managers._fetch_manager.__wrapped__

    def readback(conn, identity, manager_id):
        # The tags endpoint has an initial read before its committed update.
        name, tags = conn.execute("SELECT name, tags FROM managers WHERE id = 1").fetchone()
        if name == "Updated" or tags != "[]":
            events.append("removed-after-write")
            conn.execute("DELETE FROM managers WHERE id = 1")
            conn.commit()
        return fetch(conn, identity, manager_id)

    monkeypatch.setenv("DB_PATH", str(db))
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setattr(managers, "connect_db", connect)
    monkeypatch.setattr(managers, "_fetch_manager", readback)
    yield events
    assert len(connections) == 1
    assert events == ["removed-after-write", "close"]
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")
    with sqlite3.connect(db) as verify:
        assert verify.execute("SELECT COUNT(*) FROM managers").fetchone()[0] == 0


def test_patch_removed_record_returns_404_after_committed_write(disappearing_manager):
    with pytest.raises(HTTPException) as caught:
        asyncio.run(managers.patch_manager(managers.ManagerUpdate(name="Updated"), id=1))
    assert caught.value.status_code == 404
    assert caught.value.detail == "Manager not found"


def test_tag_patch_removed_record_returns_404_after_committed_write(disappearing_manager):
    with pytest.raises(HTTPException) as caught:
        asyncio.run(managers.patch_manager_tags(managers.ManagerTagsPatch(add=["new"]), id=1))
    assert caught.value.status_code == 404
    assert caught.value.detail == "Manager not found"
