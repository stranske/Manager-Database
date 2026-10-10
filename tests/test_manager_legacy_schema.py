"""Exercise real SQLite upgrades from the earliest manager table shapes."""

import json
import sqlite3

import pytest

from api import managers


@pytest.fixture
def legacy_connection():
    connection = sqlite3.connect(":memory:")
    try:
        yield connection
    finally:
        connection.close()


def test_universe_upgrade_adds_missing_identifiers_and_native_json_defaults(legacy_connection):
    conn = legacy_connection
    conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    conn.execute("INSERT INTO managers (id, name) VALUES (7, 'Legacy manager')")
    managers._ensure_universe_schema(conn)
    row = conn.execute(
        "SELECT id, name, cik, jurisdiction, jurisdictions, quality_flags FROM managers"
    ).fetchone()
    assert row == (7, "Legacy manager", None, None, "[]", "[]")
    assert json.loads(row[4]) == []
    indexes = {row[1] for row in conn.execute("PRAGMA index_list(managers)")}
    assert {"idx_managers_cik_unique", "idx_managers_trimmed_cik"} <= indexes


def test_universe_upgrade_backfills_created_at_without_rewriting_updated_at(legacy_connection):
    conn = legacy_connection
    conn.execute(
        "CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, updated_at TEXT)"
    )
    conn.execute("INSERT INTO managers VALUES (7, 'Legacy manager', '2001-02-03 04:05:06')")
    managers._ensure_universe_schema(conn)
    created, updated = conn.execute("SELECT created_at, updated_at FROM managers").fetchone()
    assert created is not None
    assert conn.execute("SELECT datetime(?)", (created,)).fetchone()[0] == created
    assert updated == "2001-02-03 04:05:06"


def test_universe_upgrade_backfills_updated_at_without_rewriting_created_at(legacy_connection):
    conn = legacy_connection
    conn.execute(
        "CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at TEXT)"
    )
    conn.execute("INSERT INTO managers VALUES (7, 'Legacy manager', '2001-02-03 04:05:06')")
    managers._ensure_universe_schema(conn)
    created, updated = conn.execute("SELECT created_at, updated_at FROM managers").fetchone()
    assert updated is not None
    assert conn.execute("SELECT datetime(?)", (updated,)).fetchone()[0] == updated
    assert created == "2001-02-03 04:05:06"


def test_universe_upgrade_is_idempotent_and_enforces_cik_uniqueness(legacy_connection):
    conn = legacy_connection
    conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    conn.execute("INSERT INTO managers VALUES (7, 'Legacy manager')")
    managers._ensure_universe_schema(conn)
    conn.execute(
        "UPDATE managers SET cik = '0000000007', created_at = '2001-02-03 04:05:06', "
        "updated_at = '2002-03-04 05:06:07' WHERE id = 7"
    )
    conn.commit()
    before = conn.execute("SELECT * FROM managers").fetchall()
    schema = conn.execute("SELECT type, name, sql FROM sqlite_master ORDER BY name").fetchall()
    managers._ensure_universe_schema(conn, check_legacy_ciks=True)
    assert conn.execute("SELECT * FROM managers").fetchall() == before
    assert (
        conn.execute("SELECT type, name, sql FROM sqlite_master ORDER BY name").fetchall() == schema
    )
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"):
        conn.execute("INSERT INTO managers (name, cik) VALUES ('Duplicate', '0000000007')")
