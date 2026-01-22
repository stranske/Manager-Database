"""Utility helpers for adapters."""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from importlib import import_module
from types import ModuleType
from typing import Any, Protocol

logger = logging.getLogger(__name__)

try:
    import psycopg as _psycopg
except ImportError:  # pragma: no cover - optional dependency
    psycopg: ModuleType | None = None
else:  # pragma: no cover - imported above when available
    psycopg = _psycopg


class AdapterProtocol(Protocol):
    async def list_new_filings(self, *args, **kwargs): ...

    async def download(self, *args, **kwargs): ...

    async def parse(self, *args, **kwargs): ...


def connect_db(db_path: str | None = None, *, connect_timeout: float | None = None):
    """Return a database connection to SQLite or Postgres with automatic retry.
    
    Parameters
    ----------
    db_path : str | None
        Path to SQLite database file. Ignored if DB_URL is set to Postgres.
    connect_timeout : float | None
        Connection timeout in seconds for health checks.
    
    Returns
    -------
    sqlite3.Connection | psycopg.Connection
        An active database connection.
    
    Raises
    ------
    sqlite3.OperationalError
        If SQLite connection fails after retries.
    psycopg.OperationalError
        If Postgres connection fails after retries.
    """
    url = os.getenv("DB_URL")
    if url and psycopg and url.startswith("postgres"):
        return _connect_postgres_with_retry(url, connect_timeout)
    return _connect_sqlite(db_path, connect_timeout)


def _connect_postgres_with_retry(
    url: str, connect_timeout: float | None = None, max_retries: int = 3
):
    """Connect to Postgres with exponential backoff retry logic.
    
    Parameters
    ----------
    url : str
        Postgres connection string.
    connect_timeout : float | None
        Connection timeout in seconds.
    max_retries : int
        Maximum number of retry attempts.
    
    Returns
    -------
    psycopg.Connection
        An active Postgres connection.
    
    Raises
    ------
    psycopg.OperationalError
        If connection fails after all retry attempts.
    """
    if psycopg is None:
        raise RuntimeError("psycopg is not available")
    
    connect_kwargs: dict[str, Any] = {"autocommit": True}
    if connect_timeout is not None:
        connect_kwargs["connect_timeout"] = connect_timeout
    
    last_error = None
    for attempt in range(max_retries):
        try:
            return psycopg.connect(url, **connect_kwargs)
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                # Exponential backoff: 0.1s, 0.2s, 0.4s, etc.
                backoff = 0.1 * (2 ** attempt)
                logger.debug(
                    "Postgres connection attempt %d failed, retrying in %.2fs: %s",
                    attempt + 1,
                    backoff,
                    str(e),
                )
                time.sleep(backoff)
            else:
                logger.error(
                    "Postgres connection failed after %d attempts: %s",
                    max_retries,
                    str(e),
                )
    
    # Re-raise the last error if all retries exhausted
    if last_error:
        raise last_error
    raise RuntimeError("Failed to connect to Postgres")


def _connect_sqlite(db_path: str | None = None, connect_timeout: float | None = None):
    """Connect to SQLite with timeout handling.
    
    Parameters
    ----------
    db_path : str | None
        Path to SQLite database file. Defaults to DB_PATH env var or 'dev.db'.
    connect_timeout : float | None
        Connection timeout in seconds.
    
    Returns
    -------
    sqlite3.Connection
        An active SQLite connection.
    
    Raises
    ------
    sqlite3.OperationalError
        If connection fails.
    """
    path = db_path or os.getenv("DB_PATH", "dev.db")
    sqlite_kwargs: dict[str, Any] = {}
    if connect_timeout is not None:
        sqlite_kwargs["timeout"] = connect_timeout
    
    try:
        return sqlite3.connect(str(path), **sqlite_kwargs)
    except sqlite3.OperationalError as e:
        logger.error("SQLite connection failed: %s", str(e))
        raise


@asynccontextmanager
async def tracked_call(source: str, endpoint: str, *, db_path: str | None = None):
    """Record API usage metrics in the ``api_usage`` table.

    Parameters
    ----------
    source:
        Identifier for the calling adapter, e.g. ``"edgar"``.
    endpoint:
        Endpoint or URL being hit.
    db_path:
        Optional path to a database. If ``DB_URL`` is set and points to
        a Postgres instance, that URL is used instead; otherwise defaults
        to ``DB_PATH`` or ``dev.db``.

    Usage::

        async with tracked_call("edgar", url) as log:
            resp = await client.get(url)
            log(resp)
    """

    start = time.perf_counter()
    container: dict[str, Any] = {}

    def _store(resp: Any) -> None:
        container["resp"] = resp

    try:
        yield _store
    finally:
        resp = container.get("resp")
        latency = int((time.perf_counter() - start) * 1000)
        status = getattr(resp, "status_code", 0)
        size = len(getattr(resp, "content", b""))
        conn = connect_db(db_path)
        conn.execute("""CREATE TABLE IF NOT EXISTS api_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT,
                endpoint TEXT,
                status INT,
                bytes INT,
                latency_ms INT,
                cost_usd REAL
            )""")
        if isinstance(conn, sqlite3.Connection):
            conn.execute(
                "CREATE VIEW IF NOT EXISTS monthly_usage AS "
                "SELECT substr(ts, 1, 7) || '-01' AS month, source, "
                "COUNT(*) AS calls, SUM(bytes) AS mb, SUM(cost_usd) AS cost "
                "FROM api_usage GROUP BY 1,2"
            )
        else:  # Postgres
            try:
                conn.execute(
                    "CREATE MATERIALIZED VIEW monthly_usage AS "
                    "SELECT date_trunc('month', ts) AS month, source, "
                    "COUNT(*) AS calls, SUM(bytes) AS mb, SUM(cost_usd) AS cost "
                    "FROM api_usage GROUP BY 1,2"
                )
            except Exception:
                pass
        placeholder = "%s" if not isinstance(conn, sqlite3.Connection) else "?"
        values_clause = ",".join([placeholder] * 6)
        sql = (
            "INSERT INTO api_usage(source, endpoint, status, bytes, latency_ms, cost_usd)"
            f" VALUES ({values_clause})"
        )
        conn.execute(sql, (source, endpoint, status, size, latency, 0.0))
        conn.commit()
        conn.close()


ADAPTERS: dict[str, AdapterProtocol] = {}


def get_adapter(jurisdiction: str) -> AdapterProtocol:
    """Return an adapter module for the given jurisdiction."""
    if jurisdiction not in ADAPTERS:
        module = import_module(f"adapters.{jurisdiction}")
        ADAPTERS[jurisdiction] = module  # type: ignore[assignment]
    return ADAPTERS[jurisdiction]
