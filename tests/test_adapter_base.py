import sqlite3

import pytest

from adapters import base
from adapters.base import _db_retry_config, connect_db, get_adapter


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


def test_db_retry_config_uses_env_defaults(monkeypatch):
    monkeypatch.setenv("DB_CONNECT_RETRIES", "-2")
    monkeypatch.setenv("DB_CONNECT_RETRY_DELAY", "-1.5")
    retries, delay = _db_retry_config(None, None)
    assert retries == 0
    assert delay == 0.0


def test_db_retry_config_uses_explicit_values():
    retries, delay = _db_retry_config(2, 1.25)
    assert retries == 2
    assert delay == 1.25


def test_connect_db_raises_when_retries_exhausted(monkeypatch):
    def failing_connect(*_args, **_kwargs):
        raise sqlite3.OperationalError("down")

    monkeypatch.setattr(sqlite3, "connect", failing_connect)
    monkeypatch.setenv("DB_CONNECT_RETRIES", "0")
    monkeypatch.setenv("DB_CONNECT_RETRY_DELAY", "0")

    with pytest.raises(sqlite3.OperationalError):
        connect_db("missing.db")


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


def test_connect_db_uses_env_db_path(tmp_path, monkeypatch):
    env_path = tmp_path / "env.db"
    monkeypatch.setenv("DB_PATH", str(env_path))
    conn = connect_db()
    try:
        assert isinstance(conn, sqlite3.Connection)
    finally:
        conn.close()
    assert env_path.exists()


def test_connect_db_falls_back_when_postgres_unavailable(tmp_path, monkeypatch):
    env_path = tmp_path / "fallback.db"
    monkeypatch.setenv("DB_PATH", str(env_path))
    monkeypatch.setenv("DB_URL", "postgres://user@localhost/db")
    monkeypatch.setattr(base, "psycopg", None)
    conn = connect_db()
    try:
        assert isinstance(conn, sqlite3.Connection)
    finally:
        conn.close()


def test_connect_db_postgres_retries_and_timeout(monkeypatch):
    class DummyConn:
        def close(self):
            pass

    class DummyPsycopg:
        class Error(Exception):
            pass

        def __init__(self, failures):
            self.failures = failures
            self.calls = []

        def connect(self, url, **kwargs):
            self.calls.append((url, kwargs))
            if self.failures > 0:
                self.failures -= 1
                raise self.Error("temporary")
            return DummyConn()

    sleep_calls = []
    dummy_psycopg = DummyPsycopg(failures=2)
    monkeypatch.setattr(base, "psycopg", dummy_psycopg)
    monkeypatch.setenv("DB_URL", "postgres://user@localhost/db")
    monkeypatch.setattr(base.time, "sleep", lambda delay: sleep_calls.append(delay))

    conn = connect_db(connect_timeout=5, retries=2, retry_delay=0.1)
    assert isinstance(conn, DummyConn)
    assert sleep_calls == [0.1, 0.2]
    assert dummy_psycopg.calls[0][1]["autocommit"] is True
    assert dummy_psycopg.calls[0][1]["connect_timeout"] == 5


def test_connect_db_postgres_raises_after_retries(monkeypatch):
    class DummyPsycopg:
        class Error(Exception):
            pass

        def connect(self, *_args, **_kwargs):
            raise self.Error("still down")

    dummy_psycopg = DummyPsycopg()
    monkeypatch.setattr(base, "psycopg", dummy_psycopg)
    monkeypatch.setenv("DB_URL", "postgres://user@localhost/db")
    monkeypatch.setattr(base.time, "sleep", lambda _delay: None)

    with pytest.raises(DummyPsycopg.Error):
        connect_db(retries=1, retry_delay=0)


def test_get_adapter_caches_module():
    adapter = get_adapter("edgar")
    # The registry should return the same module instance on repeated calls.
    assert get_adapter("edgar") is adapter


def test_get_adapter_unknown_module_raises():
    with pytest.raises(ModuleNotFoundError):
        get_adapter("not_a_real_adapter")
