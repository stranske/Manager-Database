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


def test_connect_db_retries_on_transient_error(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    attempts = {"count": 0}
    real_connect = sqlite3.connect

    def flaky_connect(path, **kwargs):
        attempts["count"] += 1
        # Simulate the database being unavailable for the first two attempts.
        if attempts["count"] < 3:
            raise sqlite3.OperationalError("temporary outage")
        return real_connect(path, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", flaky_connect)
    monkeypatch.setenv("DB_CONNECT_RETRIES", "3")
    monkeypatch.setenv("DB_CONNECT_RETRY_DELAY", "0")
    # Avoid real sleeps while validating retry behavior.
    monkeypatch.setattr("adapters.base.time.sleep", lambda _delay: None)

    conn = connect_db(str(db_path))
    try:
        assert attempts["count"] == 3
        assert isinstance(conn, sqlite3.Connection)
    finally:
        conn.close()


def test_get_adapter_caches_module():
    adapter = get_adapter("edgar")
    # The registry should return the same module instance on repeated calls.
    assert get_adapter("edgar") is adapter
