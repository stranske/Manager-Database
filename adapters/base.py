"""Utility helpers for adapters."""

from __future__ import annotations

import os
import sqlite3
import time
from contextlib import asynccontextmanager
from importlib import import_module
from types import ModuleType
from typing import Any, Protocol

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


def _db_retry_config(
    retries: int | None,
    retry_delay: float | None,
) -> tuple[int, float]:
    if retries is None:
        retries = int(os.getenv("DB_CONNECT_RETRIES", "3"))
    if retry_delay is None:
        retry_delay = float(os.getenv("DB_CONNECT_RETRY_DELAY", "0.5"))
    return max(0, retries), max(0.0, retry_delay)


def connect_db(
    db_path: str | None = None,
    *,
    connect_timeout: float | None = None,
    retries: int | None = None,
    retry_delay: float | None = None,
):
    """Return a database connection to SQLite or Postgres."""
    url = os.getenv("DB_URL")
    retries, retry_delay = _db_retry_config(retries, retry_delay)
    attempt = 0
    if url and psycopg and url.startswith("postgres"):
        # psycopg connections require autocommit for DDL during tests.
        # Allow health checks to cap connection time.
        connect_kwargs: dict[str, Any] = {"autocommit": True}
        if connect_timeout is not None:
            connect_kwargs["connect_timeout"] = connect_timeout
        while True:
            try:
                return psycopg.connect(url, **connect_kwargs)
            except psycopg.Error:
                # Retry a few times to let the database recover before failing.
                if attempt >= retries:
                    raise
                time.sleep(retry_delay * (2**attempt))
                attempt += 1
    path = db_path or os.getenv("DB_PATH", "dev.db")
    # SQLite timeout prevents long waits on locked files during health checks.
    sqlite_kwargs: dict[str, Any] = {}
    if connect_timeout is not None:
        sqlite_kwargs["timeout"] = connect_timeout
    while True:
        try:
            return sqlite3.connect(str(path), **sqlite_kwargs)
        except sqlite3.Error:
            # Retry a few times to allow transient filesystem/db startup issues.
            if attempt >= retries:
                raise
            time.sleep(retry_delay * (2**attempt))
            attempt += 1


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
        conn.execute(
            """CREATE TABLE IF NOT EXISTS api_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT,
                endpoint TEXT,
                status INT,
                bytes INT,
                latency_ms INT,
                cost_usd REAL
            )"""
        )
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
