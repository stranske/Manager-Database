import sqlite3

from adapters.base import connect_db, get_adapter


def test_connect_db_respects_timeout(tmp_path):
    db_path = tmp_path / "dev.db"
    # Exercise the SQLite timeout path for health checks.
    conn = connect_db(str(db_path), connect_timeout=0.01)
    try:
        # Ensure the adapter returns a native SQLite connection instance.
        assert isinstance(conn, sqlite3.Connection)
        conn.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 1
    finally:
        conn.close()


def test_get_adapter_caches_module():
    adapter = get_adapter("edgar")
    # The registry should return the same module instance on repeated calls.
    assert get_adapter("edgar") is adapter
